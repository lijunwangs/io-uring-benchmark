use io_uring::{cqueue, opcode, squeue, types, IoUring};
use std::net::UdpSocket;
use std::os::unix::io::AsRawFd;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::{thread, time::Duration};
use structopt::StructOpt;

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "io-uring_benchmark")]
struct Opt {
    /// Multi recev (recvmsgs), otherwise use recv2
    #[structopt(long)]
    multi_recv: bool,
}

const BUFFER_SIZE: usize = 4096;
const LOG_INTERVAL_SECS: u64 = 5;
const SQPOLL_IDLE_MS: u32 = 5000; // Kernel polling time before sleep

fn bench_mark_recv(socket: UdpSocket, mut ring: IoUring) -> std::io::Result<()> {
    let fd = socket.as_raw_fd();

    let (submitter, mut sq, mut cq) = ring.split();

    // Atomic counter for received packets
    let packet_count = Arc::new(AtomicUsize::new(0));
    let packet_counter_clone = Arc::clone(&packet_count);

    // Spawn a logging thread
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(LOG_INTERVAL_SECS));
        let count = packet_counter_clone.swap(0, Ordering::Relaxed);
        println!(
            "[LOG] Packets received in last {} seconds: {}",
            LOG_INTERVAL_SECS, count
        );
    });

    println!(
        "Listening for UDP packets on {socket:?} fd: {fd} (Using IORING_SETUP_SQPOLL, idle={}ms)",
        SQPOLL_IDLE_MS
    );

    loop {
        let mut buffer = [0 as u8; BUFFER_SIZE];

        // Prepare a read operation using io_uring
        let entry = opcode::Recv::new(
            types::Fd(fd),
            buffer.as_mut_ptr() as *mut _,
            buffer.len() as _,
        )
        .build()
        .user_data(42);

        // Submit request
        unsafe {
            let _ = sq.push(&entry); // .expect("Failed to submit request");
        }
        sq.sync();

        // println!("Sunmitted recv request");
        if let Err(e) = submitter.submit_and_wait(1) {
            eprintln!("Submitter failed to wake up SQPOLL: {:?}", e);
        }

        cq.sync();

        for cqe in &mut cq {
            // println!("Reading from socket");
            // Get completion queue event
            if cqe.result() < 0 {
                eprintln!("Error receiving UDP packet: {}", cqe.result());
            } else {
                // let bytes_received = cqe.result() as usize;
                packet_count.fetch_add(1, Ordering::Relaxed);
                // println!("Received {} bytes", bytes_received);
            }
        }
    }
}

fn bench_mark_multi_recv(socket: UdpSocket, mut ring: IoUring) -> std::io::Result<()> {
    // Provide 2 buffers in buffer group `33`, at index 0 and 1.
    // Each one is 512 bytes large.
    const BUF_GROUP: u16 = 33;
    const SIZE: usize = 1400;
    let mut buffers = [[0u8; SIZE]; 1024];
    for (index, buf) in buffers.iter_mut().enumerate() {
        let provide_bufs_e = io_uring::opcode::ProvideBuffers::new(
            buf.as_mut_ptr(),
            SIZE as i32,
            1,
            BUF_GROUP,
            index as u16,
        )
        .build()
        .user_data(11)
        .into();
        unsafe {
            ring.submission()
                .push(&provide_bufs_e)
                .expect("submit should succeed");
        }

        ring.submitter().submit_and_wait(1)?;
        let cqes: Vec<io_uring::cqueue::Entry> = ring.completion().map(Into::into).collect();
        assert_eq!(cqes.len(), 1);
        assert_eq!(cqes[0].user_data(), 11);
        assert_eq!(cqes[0].result(), 0);
        assert_eq!(cqes[0].flags(), 0);
    }

    // This structure is actually only used for input arguments to the kernel
    // (and only name length and control length are actually relevant).
    let mut msghdr: libc::msghdr = unsafe { std::mem::zeroed() };
    msghdr.msg_namelen = 32;
    msghdr.msg_controllen = 0;

    loop {
        let recvmsg_e = opcode::RecvMsgMulti::new(
            types::Fd(socket.as_raw_fd()),
            &msghdr as *const _,
            BUF_GROUP,
        )
        .build()
        .user_data(77)
        .into();
        unsafe {
            ring.submission()
                .push(&recvmsg_e)
                .expect("submit should suceed");
        };
        ring.submitter().submit().unwrap();

        let cqes: Vec<io_uring::cqueue::Entry> = ring.completion().map(Into::into).collect();
        for cqe in cqes {
            assert!(cqe.result() > 0);
            assert!(is_more);
            let buf_id = io_uring::cqueue::buffer_select(cqe.flags()).unwrap();
            let tmp_buf = &buffers[buf_id as usize];
            let msg = types::RecvMsgOut::parse(tmp_buf, &msghdr).unwrap();
            assert!([25, 15].contains(&msg.payload_data().len()));
            assert!(!msg.is_payload_truncated());
            assert!(!msg.is_control_data_truncated());
            assert_eq!(msg.control_data(), &[]);
            assert!(!msg.is_name_data_truncated());
            let addr = unsafe {
                let storage = msg
                    .name_data()
                    .as_ptr()
                    .cast::<libc::sockaddr_storage>()
                    .read_unaligned();
                let len = msg.name_data().len().try_into().unwrap();
                socket2::SockAddr::new(storage, len)
            };
            let addr = addr.as_socket_ipv4().unwrap();
            assert_eq!(addr.ip(), client_addr.ip());
            assert_eq!(addr.port(), client_addr.port());
        }
    }

    Ok(())
}

fn main() -> std::io::Result<()> {
    let opt = Opt::from_args();
    // Create and bind UDP socket
    let socket = UdpSocket::bind("0.0.0.0:11228")?;
    socket.set_nonblocking(true)?;
    // Enable IORING_SETUP_SQPOLL with idle timeout
    let ring = IoUring::<squeue::Entry, cqueue::Entry>::builder()
        .setup_sqpoll(SQPOLL_IDLE_MS) // Kernel polls for 5 seconds before sleeping
        .build(128)?;

    if !opt.multi_recv {
        bench_mark_recv(socket, ring)
    } else {
        bench_mark_multi_recv(socket, ring)
    }
}
