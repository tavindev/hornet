use std::time::SystemTime;

use super::{
    loader::{load_redis_script},
    Script,
};
use anyhow::Result;
use redis::{FromRedisValue, ToRedisArgs};

use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct MoveToActive(pub redis::Script);

impl MoveToActive {
    pub fn new() -> Self {
        let script = load_redis_script("./src/scripts/commands/moveToActive-11.lua");

        match script {
            Ok(script) => MoveToActive(script),
            Err(e) => panic!("Error: {:?}", e),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MoveToActiveJobArgs {
    pub token: String,
    #[serde(rename = "lockDuration")]
    pub lock_duration: u32,
}

impl ToRedisArgs for MoveToActiveJobArgs {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        rmp_serde::encode::to_vec_named(self)
            .unwrap()
            .write_redis_args(out)
    }
}

#[derive(Debug)]
pub(crate) struct Job {
    pub id: String,
    pub name: String,
    pub data: String,
    pub opts: String,
    pub timestamp: u128,
    pub delay: u128,
    pub priority: u32,
    pub processed_on: u128,
    pub ats: u32,
}

#[derive(Debug)]
pub enum MoveToActiveJobReturn {
    Job(Job),
    None,
}

impl FromRedisValue for MoveToActiveJobReturn {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        use redis::Value;

        match *v {
            Value::Bulk(ref items) => match items.as_slice() {
                [Value::Int(0), Value::Int(0), Value::Int(0), Value::Int(0)] => {
                    return Ok(MoveToActiveJobReturn::None)
                }
                [Value::Bulk(raw_job), Value::Data(job_id), Value::Int(_), Value::Int(_)] => {
                    let raw_job = match raw_job.as_slice() {
                        [_, Value::Data(name), _, Value::Data(data), _, Value::Data(opts), _, Value::Data(timestamp), _, Value::Data(delay), _, Value::Data(priority), _, Value::Data(processed_on), _, Value::Data(ats)] => {
                            Ok(Job {
                                id: String::from_utf8(job_id.to_vec()).unwrap(),
                                name: String::from_utf8(name.to_vec()).unwrap(),
                                data: String::from_utf8(data.to_vec()).unwrap(),
                                opts: String::from_utf8(opts.to_vec()).unwrap(),
                                timestamp: String::from_utf8(timestamp.to_vec())
                                    .unwrap()
                                    .parse::<u128>()
                                    .unwrap(),
                                delay: String::from_utf8(delay.to_vec())
                                    .unwrap()
                                    .parse::<u128>()
                                    .unwrap(),
                                priority: String::from_utf8(priority.to_vec())
                                    .unwrap()
                                    .parse::<u32>()
                                    .unwrap(),
                                processed_on: String::from_utf8(processed_on.to_vec())
                                    .unwrap()
                                    .parse::<u128>()
                                    .unwrap(),
                                ats: String::from_utf8(ats.to_vec())
                                    .unwrap()
                                    .parse::<u32>()
                                    .unwrap(),
                            })
                        }
                        _ => Err(redis::RedisError::from((
                            redis::ErrorKind::TypeError,
                            "Invalid response type",
                        ))),
                    }?;

                    Ok(MoveToActiveJobReturn::Job(raw_job))
                }
                _ => {
                    return Err(redis::RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Invalid response type",
                    )));
                }
            },
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Invalid response type",
            ))),
        }
    }
}

impl Script<MoveToActiveJobArgs, MoveToActiveJobReturn> for MoveToActive {
    fn run(
        &self,
        prefix: &str,
        redis: &mut redis::Client,
        args: MoveToActiveJobArgs,
    ) -> Result<MoveToActiveJobReturn> {
        let keys = vec![
            "wait",
            "active",
            "prioritized",
            "events",
            "stalled",
            "limiter",
            "delayed",
            "paused",
            "meta",
            "pc",
            "marker",
        ];
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let mut script = &mut self.0.prepare_invoke();

        for key in keys {
            let key = format!("{}{}", prefix, key);
            script = script.key(key);
        }

        let res = script
            .arg(prefix)
            .arg(timestamp.to_string())
            .arg(args)
            .invoke::<MoveToActiveJobReturn>(redis);

        Ok(res?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads() {
        let script = MoveToActive::new();

        let mut redis = redis::Client::open("redis://localhost:6379").unwrap();

        let res = script.run(
            "my_queue",
            &mut redis,
            MoveToActiveJobArgs {
                token: "test".to_string(),
                lock_duration: 10_000,
            },
        );

        dbg!(&res);

        assert!(res.is_ok());

        let res = res.unwrap();

        dbg!(res);
    }
}
