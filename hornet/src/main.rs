
use serde::{Deserialize, Serialize};

use worker::Worker;

mod scripts;
mod worker;

#[derive(Debug, Serialize, Deserialize)]
struct ProcessorData {
    name: String,
    age: u8,
}

fn test_processor(data: ProcessorData) -> String {
    println!("Processing: {:?}", data);

    "Done".to_string()
}

#[tokio::main]
async fn main() {
    let mut worker = Worker::new(
        "new-queue".to_string(),
        "redis://localhost:6379".to_string(),
        1,
        test_processor,
    );

    worker.run().await;
}
