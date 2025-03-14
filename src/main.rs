use io_uring::{cqueue, opcode, squeue, types, IoUring, Probe};
use std::mem::MaybeUninit;
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
    /// Traditional recev (recvmsgs)
    #[structopt(long)]
    recvmsg: bool,

    /// use provided buffer
    #[structopt(long)]
    use_buffer: bool,    
}

const BUFFER_SIZE: usize = 4096;
const LOG_INTERVAL_SECS: u64 = 5;
const SQPOLL_IDLE_MS: u32 = 5000; // Kernel polling time before sleep

fn bench_mark_recv(
    socket: UdpSocket,
    mut ring: IoUring,
    packet_count: Arc<AtomicUsize>,
) -> std::io::Result<()> {
    let fd = socket.as_raw_fd();

    let (submitter, sq, cq) = ring.split();

    run_recv(packet_count, fd, submitter, sq, cq)
}

fn run_recv(
    packet_count: Arc<AtomicUsize>,
    fd: i32,
    submitter: io_uring::Submitter<'_>,
    mut sq: squeue::SubmissionQueue<'_>,
    mut cq: cqueue::CompletionQueue<'_>,
) -> ! {
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
            if !sq.is_full() {
                let result = sq.push(&entry); // .expect("Failed to submit request");
                if result.is_err() {
                    println!("Could not submit request: {result:?}");
                }
            }
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

// Multi-shot recv is not supported on Ubuntu 22.04 -- need kernel 6.0
fn bench_mark_multi_recv(
    socket: UdpSocket,
    mut ring: IoUring,
    _packet_count: Arc<AtomicUsize>,
) -> std::io::Result<()> {
    // Provide 2 buffers in buffer group `33`, at index 0 and 1.
    // Each one is 512 bytes large.
    let probe = Probe::new();

    assert!(probe.is_supported(opcode::ProvideBuffers::CODE));
    assert!(probe.is_supported(opcode::SendMsgZc::CODE));
    assert!(probe.is_supported(opcode::SendMsg::CODE));
    assert!(probe.is_supported(opcode::RecvMsgMulti::CODE));

    const BUF_GROUP: u16 = 33;
    const SIZE: usize = 1400;
    let mut buffers = [[0u8; SIZE]; 1024];
    for (index, buf) in buffers.iter_mut().enumerate() {
        println!("Providing buffer: {index}");
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
        println!("Results: {}", cqes[0].result());
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
            println!("Got message from addr: {addr:?}");
        }
    }
}

// Use recvmsg
fn bench_mark_recvmsg(
    socket: UdpSocket,
    mut ring: IoUring,
    packet_count: Arc<AtomicUsize>,
) -> std::io::Result<()> {
    let fd = types::Fd(socket.as_raw_fd());
    let sockaddr = socket.local_addr().unwrap();
    let sockaddr = socket2::SockAddr::from(sockaddr);

    // let probe = Probe::new();
    // assert!(probe.is_supported(opcode::RecvMsg::CODE));
    const SIZE: usize = 1400;

    let mut buf2 = vec![0; SIZE];
    let mut bufs2 = [std::io::IoSliceMut::new(&mut buf2)];

    // build recvmsg
    let mut msg = MaybeUninit::<libc::msghdr>::zeroed();

    unsafe {
        let p = msg.as_mut_ptr();
        (*p).msg_name = sockaddr.as_ptr() as *const _ as *mut _;
        (*p).msg_namelen = sockaddr.len();
        (*p).msg_iov = bufs2.as_mut_ptr() as *mut _;
        (*p).msg_iovlen = 1;
    }

    loop {
        let recvmsg_e = opcode::RecvMsg::new(fd, msg.as_mut_ptr());

        // submit
        unsafe {
            let mut queue = ring.submission();
            queue
                .push(&recvmsg_e.build().user_data(0x02).into())
                .expect("queue is full");
        }

        ring.submit_and_wait(1)?;

        let cqes: Vec<cqueue::Entry> = ring.completion().map(Into::into).collect();
        for entry in cqes {
            packet_count.fetch_add(1, Ordering::Relaxed);
        }
    }
}


// Use recvmsg
fn bench_mark_recvmsg_with_provided_buf(
    socket: UdpSocket,
    mut ring: IoUring,
    packet_count: Arc<AtomicUsize>,
) -> std::io::Result<()> {
    let fd = types::Fd(socket.as_raw_fd());
    let sockaddr = socket.local_addr().unwrap();
    let sockaddr = socket2::SockAddr::from(sockaddr);

    const BGID: u16 = 0xdeaf;
    const INPUT_BID: u16 = 100;

    // provide two buffers for recvmsg
    let mut buf = [0u8; 2 * 1024];
    let provide_bufs_e = opcode::ProvideBuffers::new(buf.as_mut_ptr(), 1024, 2, BGID, INPUT_BID);

    unsafe {
        ring.submission()
            .push(&provide_bufs_e.build().user_data(0x26).into())
            .expect("queue is full");
    }

    ring.submit_and_wait(1)?;

    let cqe: cqueue::Entry = ring.completion().next().expect("cqueue is empty").into();
    assert_eq!(cqe.user_data(), 0x26);


    // recvmsg
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    let mut iovecs: [libc::iovec; 1] = unsafe { std::mem::zeroed() };
    iovecs[0].iov_len = 1024; // This can be used to reduce the length of the read.
    msg.msg_iov = &mut iovecs as *mut _;
    msg.msg_iovlen = 1; // 2 results in EINVAL, Invalid argument, being returned in result.

    // N.B. This op will only support a BUFFER_SELECT when the msg.msg_iovlen is 1;
    // the kernel will return EINVAL for anything else. There would be no way of knowing
    // which other buffer IDs had been chosen.
    let op = opcode::RecvMsg::new(fd, &mut msg as *mut _)
        .buf_group(BGID) // else result is -105, ENOBUFS, no buffer space available
        .build()
        .flags(squeue::Flags::BUFFER_SELECT) // else result is -14, EFAULT, bad address
        .user_data(0x27);

    // Safety: the msghdr and the iovecs remain valid for length of the operation.
    unsafe {
        ring.submission().push(&op.into()).expect("queue is full");
    }

    ring.submit_and_wait(1)?;

    let cqe: cqueue::Entry = ring.completion().next().expect("cqueue is empty").into();
    assert_eq!(cqe.user_data(), 0x27);
    assert_eq!(cqe.result(), 1024); // -14 would mean EFAULT, bad address.

    let bid = cqueue::buffer_select(cqe.flags()).expect("no buffer id");
    if bid == INPUT_BID {
        // The 6.1 case.
        // Test buffer slice associated with the given bid.
        assert_eq!(&(buf[..1024]), &([0x56u8; 1024][..]));
    } else if bid == (INPUT_BID + 1) {
        // The 5.15 case.
        // Test buffer slice associated with the given bid.
        assert_eq!(&(buf[1024..]), &([0x56u8; 1024][..]));
    } else {
        panic!(
            "cqe bid {}, was neither {} nor {}",
            bid,
            INPUT_BID,
            INPUT_BID + 1
        );
    }

    Ok(())

    // loop {
    //     let recvmsg_e = opcode::RecvMsg::new(fd, msg.as_mut_ptr());

    //     // submit
    //     unsafe {
    //         let mut queue = ring.submission();
    //         queue
    //             .push(&recvmsg_e.build().user_data(0x02).into())
    //             .expect("queue is full");
    //     }

    //     ring.submit_and_wait(1)?;

    //     let cqes: Vec<cqueue::Entry> = ring.completion().map(Into::into).collect();
    //     for entry in cqes {
    //         packet_count.fetch_add(1, Ordering::Relaxed);
    //     }
    // }
}


fn main() -> std::io::Result<()> {
    let opt = Opt::from_args();
    // Create and bind UDP socket
    let socket = UdpSocket::bind("0.0.0.0:11228")?;
    socket.set_nonblocking(true)?;
    // Enable IORING_SETUP_SQPOLL with idle timeout
    let ring = IoUring::<squeue::Entry, cqueue::Entry>::builder()
        .setup_sqpoll(SQPOLL_IDLE_MS) // Kernel polls for 5 seconds before sleeping
        .build(1024)?;

    // Atomic counter for received packets
    let packet_count = Arc::new(AtomicUsize::new(0));
    let packet_counter_clone = packet_count.clone();

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
        "Listening for UDP packets on {socket:?} fd: {} (Using IORING_SETUP_SQPOLL, idle={}ms)",
        socket.as_raw_fd(),
        SQPOLL_IDLE_MS
    );

    if opt.multi_recv {
        bench_mark_multi_recv(socket, ring, packet_count)
    } else if opt.recvmsg {
        bench_mark_recvmsg(socket, ring, packet_count)
    }  else if opt.use_buffer {
        bench_mark_recvmsg_with_provided_buf(socket, ring, packet_count)
    } else {
        bench_mark_recv(socket, ring, packet_count)
    }
}
