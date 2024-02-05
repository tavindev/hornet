use crate::scripts::{
    move_to_active::{MoveToActive, MoveToActiveJobArgs, MoveToActiveJobReturn},
    Script,
};
use lazy_static::lazy_static;
use redis::{Client, Commands, Connection, FromRedisValue};
use serde::{de::DeserializeOwned, Deserialize};
use std::{
    collections::HashMap,
    sync::{atomic::AtomicU32, Arc, Mutex},
    vec,
};
use tokio::{sync::Notify, task::JoinHandle};

lazy_static! {
    static ref MOVE_TO_ACTIVE: MoveToActive = MoveToActive::new();
}

enum TaskRunnerEvent {
    Freed,
}

struct TaskRunner {
    prefix: String,
    client: Client,
    sender: tokio::sync::mpsc::Sender<TaskRunnerEvent>,
}

impl TaskRunner {
    fn new(
        prefix: String,
        client: Client,
        sender: tokio::sync::mpsc::Sender<TaskRunnerEvent>,
    ) -> Self {
        TaskRunner {
            prefix,
            client,
            sender,
        }
    }

    fn run<Data: DeserializeOwned + 'static>(mut self, process_fn: fn(Data) -> String) {
        let _ = tokio::spawn(async move {
            // Move to active script
            while let Ok(job) = MOVE_TO_ACTIVE.run(
                &self.prefix,
                &mut self.client,
                MoveToActiveJobArgs {
                    token: "0".to_string(),
                    lock_duration: 10_000,
                },
            ) {
                match job {
                    MoveToActiveJobReturn::None => {
                        // No job to process
                        break;
                    }
                    MoveToActiveJobReturn::Job(job) => match serde_json::from_str(&job.data) {
                        Ok(data) => {
                            let result = process_fn(data);
                        }
                        Err(err) => {
                            println!("Error deserializing job data: {:?}", err);
                        }
                    },
                }
            }

            // Emits a signal to the worker that it's done processing jobs
            let _ = self.sender.send(TaskRunnerEvent::Freed).await;
        });
    }
}

pub struct Worker<Data: DeserializeOwned + 'static> {
    queue_name: String,
    concurrency: usize,
    active_tasks: usize,
    client: Client,
    receiver: tokio::sync::mpsc::Receiver<TaskRunnerEvent>,
    sender: tokio::sync::mpsc::Sender<TaskRunnerEvent>,
    process_fn: fn(Data) -> String,
}

impl<Data> Worker<Data>
where
    Data: DeserializeOwned + 'static,
{
    pub fn new(
        queue_name: String,
        redis_url: String,
        concurrency: usize,
        process_fn: fn(Data) -> String,
    ) -> Self {
        let client = Client::open(redis_url).unwrap();
        let (sender, receiver) = tokio::sync::mpsc::channel(concurrency);

        Worker {
            queue_name,
            concurrency,
            active_tasks: 0,
            client,
            receiver,
            sender,
            process_fn,
        }
    }

    pub async fn run(&mut self) {
        let mut connection = self.client.get_connection().unwrap();

        loop {
            // Does not clear all the buffer
            // What if a message is dropped?
            while self.active_tasks >= self.concurrency {
                if let Some(TaskRunnerEvent::Freed) = self.receiver.recv().await {
                    self.active_tasks -= 1;
                }
            }

            // Marker is used to notify worker of new jobs
            if let Ok(_) = connection
                .bzpopmin::<String, (String, String, f64)>(self.get_prefixed_key("marker"), 10000.)
            {
                let task_runner = TaskRunner::new(
                    self.get_prefixed_key(""),
                    self.client.clone(),
                    self.sender.clone(),
                );
                self.active_tasks += 1;
                task_runner.run(self.process_fn);
            }
        }
    }

    fn get_prefixed_key(&self, key: &str) -> String {
        format!("bull:{}:{}", self.queue_name, key)
    }
}

#[cfg(test)]
mod tests {}
