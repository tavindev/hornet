use crate::{
    job::Job,
    scripts::{
        move_to_active::{MoveToActive, MoveToActiveArgs, MoveToActiveReturn},
        move_to_finished::{
            KeepJobs, MoveToFinished, MoveToFinishedArgs, MoveToFinishedReturn,
            MoveToFinishedTarget,
        },
    },
};
use anyhow::Result;
use lazy_static::lazy_static;
use redis::{Client, Commands};
use serde::de::DeserializeOwned;
use uuid::Uuid;

lazy_static! {
    static ref MOVE_TO_ACTIVE: MoveToActive = MoveToActive::new();
    static ref MOVE_TO_FINISHED: MoveToFinished = MoveToFinished::new();
}

struct WorkerToken {
    token: String,
    postfix: u64,
}

impl WorkerToken {
    fn new() -> Self {
        WorkerToken {
            token: Uuid::new_v4().to_string(),
            postfix: 0,
        }
    }

    fn next(&mut self) -> String {
        self.postfix += 1;
        format!("{}:{}", self.token, self.postfix)
    }
}

pub struct Worker<Data: DeserializeOwned + 'static> {
    queue_name: String,
    concurrency: usize,
    active_tasks: usize,
    client: Client,
    receiver: tokio::sync::mpsc::Receiver<TaskRunnerEvent>,
    sender: tokio::sync::mpsc::Sender<TaskRunnerEvent>,
    process_fn: fn(Job<Data>) -> Result<String>,
    token: WorkerToken,
}

impl<Data> Worker<Data>
where
    Data: DeserializeOwned + 'static,
{
    pub fn new(
        queue_name: String,
        redis_url: String,
        concurrency: usize,
        process_fn: fn(Job<Data>) -> Result<String>,
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
            token: WorkerToken::new(),
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
                    self.token.next(),
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

enum TaskRunnerEvent {
    Freed,
}

struct TaskRunner {
    prefix: String,
    token: String,
    client: Client,
    sender: tokio::sync::mpsc::Sender<TaskRunnerEvent>,
}

impl TaskRunner {
    fn new(
        prefix: String,
        token: String,
        client: Client,
        sender: tokio::sync::mpsc::Sender<TaskRunnerEvent>,
    ) -> Self {
        TaskRunner {
            prefix,
            token,
            client,
            sender,
        }
    }

    fn run<Data: DeserializeOwned + 'static>(
        mut self,
        process_fn: fn(Job<Data>) -> Result<String>,
    ) {
        let _ = tokio::spawn(async move {
            // Move to active script
            while let Ok(job) = MOVE_TO_ACTIVE.run::<Data>(
                &self.prefix,
                &mut self.client,
                MoveToActiveArgs {
                    token: self.token.clone(),
                    lock_duration: 10_000,
                },
            ) {
                match job {
                    MoveToActiveReturn::Job(job) => {
                        let job_id = job.id.clone();

                        match process_fn(job) {
                            Ok(_) => {
                                // Move job to completed
                            }
                            Err(err) => {
                                // Move job to failed
                                match MOVE_TO_FINISHED
                                    .run(
                                        &self.prefix,
                                        &mut self.client,
                                        &job_id,
                                        err.to_string().as_str(),
                                        MoveToFinishedTarget::Failed,
                                        MoveToFinishedArgs {
                                            token: self.token.clone(),
                                            keep_jobs: KeepJobs { count: -1 },
                                            lock_duration: 10_000,
                                            max_attempts: 1,
                                            max_metrics_size: 100,
                                            fail_parent_on_fail: false,
                                            remove_dependency_on_fail: false,
                                        },
                                    )
                                    .unwrap()
                                {
                                    MoveToFinishedReturn::Ok => {}
                                    res => {
                                        println!("Error moving job to failed: {:?}", res);
                                    }
                                }
                            }
                        }
                    }
                    MoveToActiveReturn::None => {
                        // No job to process
                        break;
                    }
                }
            }

            // Emits a signal to the worker that it's done processing jobs
            let _ = self.sender.send(TaskRunnerEvent::Freed).await;
        });
    }
}

#[cfg(test)]
mod tests {}
