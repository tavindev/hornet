#[brosqueue_macros::worker(concurrency = 2, retry = 3)]
fn worker() {
    // Your function code here
    println!("Inside my_function");
}

fn main() {
    worker();
}
