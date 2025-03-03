use io_uring::{IoUring, IoUringBuilder, opcode, types};
use std::net::UdpSocket;
use std::os::unix::io::AsRawFd;
use std::sync::{
    Arc, atomic::{AtomicUsize, Ordering}
};
use std::{mem::MaybeUninit, thread, time::Duration};

const BUFFER_SIZE: usize = 4096;
const LOG_INTERVAL_SECS: u64 = 5;
const SQPOLL_IDLE_MS: u32 = 5000; // Kernel polling time before sleep

fn main() -> std::io::Result<()> {
    // Create and bind UDP socket
    let socket = UdpSocket::bind("0.0.0.0:8080")?;
    socket.set_nonblocking(true)?;
    let fd = socket.as_raw_fd();

    // Enable IORING_SETUP_SQPOLL with idle timeout
    let mut ring = IoUringBuilder::new()
        .setup_sqpoll(SQPOLL_IDLE_MS) // Kernel polls for 5 seconds before sleeping
        .build(32)?;

    let submitter = ring.submitter(); // Handle for waking up SQPOLL if needed

    // Atomic counter for received packets
    let packet_count = Arc::new(AtomicUsize::new(0));
    let packet_counter_clone = Arc::clone(&packet_count);

    // Spawn a logging thread
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(LOG_INTERVAL_SECS));
            let count = packet_counter_clone.swap(0, Ordering::Relaxed);
            println!("[LOG] Packets received in last {} seconds: {}", LOG_INTERVAL_SECS, count);
        }
    });

    println!("Listening for UDP packets on 0.0.0.0:8080 (Using IORING_SETUP_SQPOLL, idle={}ms)", SQPOLL_IDLE_MS);

    loop {
        let mut buffer = [MaybeUninit::uninit(); BUFFER_SIZE];

        // Prepare a read operation using io_uring
        let entry = opcode::RecvMsg::new(
            types::Fd(fd),
            buffer.as_mut_ptr() as *mut _,
            BUFFER_SIZE as _,
        )
        .build()
        .user_data(42);

        // Submit request
        unsafe {
            ring.submission().push(&entry).expect("Failed to submit request");
        }

        // **Use `need_wakeup()` to check if SQPOLL is asleep**
        if submitter.need_wakeup() {
            if let Err(e) = submitter.submit() {
                eprintln!("Submitter failed to wake up SQPOLL: {:?}", e);
            }
        }

        // Get completion queue event
        let cqe = ring.completion().next().expect("Failed to get completion");
        if cqe.result() < 0 {
            eprintln!("Error receiving UDP packet: {}", cqe.result());
        } else {
            let bytes_received = cqe.result() as usize;
            packet_count.fetch_add(1, Ordering::Relaxed);
            println!("Received {} bytes", bytes_received);
        }
    }
}
