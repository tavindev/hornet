use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct JobOptions {
    pub attempts: u32,
}

#[derive(Debug)]
pub struct Job<Data> {
    pub id: String,
    pub name: String,
    pub data: Data,
    pub opts: JobOptions,
    pub timestamp: u128,
    pub delay: u128,
    pub priority: u32,
    pub processed_on: u128,
    pub attempts_started: u32,
    pub attempts_made: Option<u32>,
}

pub struct JobBuilder<Data> {
    id: Option<String>,
    name: Option<String>,
    data: Option<Data>,
    opts: Option<JobOptions>,
    timestamp: Option<u128>,
    delay: Option<u128>,
    priority: Option<u32>,
    processed_on: Option<u128>,
    attempts_started: Option<u32>,
    attempts_made: Option<u32>,
}

impl<Data> JobBuilder<Data> {
    pub fn new() -> Self {
        JobBuilder {
            id: None,
            name: None,
            data: None,
            opts: None,
            timestamp: None,
            delay: None,
            priority: None,
            processed_on: None,
            attempts_started: None,
            attempts_made: None,
        }
    }

    pub fn id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn data(mut self, data: Data) -> Self {
        self.data = Some(data);
        self
    }

    pub fn opts(mut self, opts: String) -> Self {
        self.opts =
            Some(serde_json::from_str(&opts).expect("Failed to parse job options from string"));
        self
    }

    pub fn timestamp(mut self, timestamp: u128) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn delay(mut self, delay: u128) -> Self {
        self.delay = Some(delay);
        self
    }

    pub fn priority(mut self, priority: u32) -> Self {
        self.priority = Some(priority);
        self
    }

    pub fn processed_on(mut self, processed_on: u128) -> Self {
        self.processed_on = Some(processed_on);
        self
    }

    pub fn attempts_started(mut self, attempts_started: u32) -> Self {
        self.attempts_started = Some(attempts_started);
        self
    }

    pub fn attempts_made(mut self, attempts_made: u32) -> Self {
        self.attempts_made = Some(attempts_made);
        self
    }

    pub fn build(self) -> Job<Data> {
        Job {
            id: self.id.unwrap(),
            name: self.name.unwrap(),
            data: self.data.unwrap(),
            opts: self.opts.unwrap(),
            timestamp: self.timestamp.unwrap(),
            delay: self.delay.unwrap(),
            priority: self.priority.unwrap(),
            processed_on: self.processed_on.unwrap(),
            attempts_started: self.attempts_started.unwrap(),
            attempts_made: self.attempts_made,
        }
    }
}
