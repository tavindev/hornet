# Hornet


**WIP**: A fast and efficient redis backed queue for rust [**BullMq compatible**.]

Currently in development, not ready for production use. 

Currently implemented features:
- Worker
  - Process jobs (no delay)
  - Retry failed jobs (without backoff/delay)
  - Concurrency
  
	 
Basic usage:

```rust
use anyhow::Result;
use hornet::{job::Job, worker::Worker};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct ProcessorData {
    name: String,
    age: u8,
}

fn test_processor(job: &Job<ProcessorData>) -> Result<()> {
    println!("Processing: {:?}", job);

    Ok(())
}

#[tokio::main]
async fn main() {
    let mut worker = Worker::new(
        "queue-name".to_string(),
        "redis://localhost:6379".to_string(),
        1,
        test_processor,
    );

    worker.run().await;
}

```