use std::time::SystemTime;

use crate::{generate_script_struct, job::Job, queue_keys::QueueKeys};

use anyhow::Result;
use redis::{FromRedisValue, ToRedisArgs};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

generate_script_struct!(MoveToActive, "./src/scripts/commands/moveToActive-11.lua");

impl MoveToActive {
    pub fn run<JobData: DeserializeOwned>(
        &self,
        prefix: &str,
        mut client: &mut redis::Client,
        opts: MoveToActiveArgs,
    ) -> Result<MoveToActiveReturn<JobData>> {
        let mut script = &mut self.0.prepare_invoke();

        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();

        let keys: Vec<String> = [
            QueueKeys::Wait,
            QueueKeys::Active,
            QueueKeys::Prioritized,
            QueueKeys::Events,
            QueueKeys::Stalled,
            QueueKeys::Limiter,
            QueueKeys::Delayed,
            QueueKeys::Paused,
            QueueKeys::Meta,
            QueueKeys::Pc,
            QueueKeys::Marker,
        ]
        .iter()
        .map(|s| s.with_prefix(prefix))
        .collect();

        for key in keys {
            script = script.key(key)
        }

        let res = script
            .arg(prefix)
            .arg(timestamp)
            .arg(opts)
            .invoke::<MoveToActiveReturn<JobData>>(&mut client)?;

        Ok(res)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MoveToActiveArgs {
    pub token: String,
    #[serde(rename = "lockDuration")]
    pub lock_duration: u32,
}

impl ToRedisArgs for MoveToActiveArgs {
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
pub enum MoveToActiveReturn<JobData> {
    Job(Job<JobData>),
    None,
}

impl<JobData: DeserializeOwned> FromRedisValue for MoveToActiveReturn<JobData> {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        use redis::Value;

        match *v {
            Value::Bulk(ref items) => match items.as_slice() {
                [Value::Int(0), Value::Int(0), Value::Int(0), Value::Int(0)] => {
                    return Ok(MoveToActiveReturn::None)
                }
                [Value::Bulk(raw_job), Value::Data(job_id), Value::Int(_), Value::Int(_)] => {
                    let raw_job = match raw_job.as_slice() {
                        [_, Value::Data(name), _, Value::Data(data), _, Value::Data(opts), _, Value::Data(timestamp), _, Value::Data(delay), _, Value::Data(priority), _, Value::Data(processed_on), _, Value::Data(ats)] => {
                            Ok(Job {
                                id: String::from_utf8(job_id.to_vec()).unwrap(),
                                name: String::from_utf8(name.to_vec()).unwrap(),
                                data: serde_json::from_slice(data).unwrap(),
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

                    Ok(MoveToActiveReturn::Job(raw_job))
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

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::queue_keys::QueueKeys;

    use super::*;

    #[test]
    fn loads() {
        let script = MoveToActive::new();
        let mut script = &mut script.0.prepare_invoke();
        let mut redis = redis::Client::open("redis://localhost:6379").unwrap();
        let prefix = "bull:my_queue:";

        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();

        let keys: Vec<String> = vec![
            QueueKeys::Wait,
            QueueKeys::Active,
            QueueKeys::Prioritized,
            QueueKeys::Events,
            QueueKeys::Stalled,
            QueueKeys::Limiter,
            QueueKeys::Delayed,
            QueueKeys::Paused,
            QueueKeys::Meta,
            QueueKeys::Pc,
            QueueKeys::Marker,
        ]
        .iter()
        .map(|s| s.with_prefix(prefix))
        .collect();

        for key in keys {
            script = script.key(key)
        }

        let res = script
            .arg(prefix)
            .arg(timestamp)
            .arg(MoveToActiveArgs {
                token: "test".to_string(),
                lock_duration: 10_000,
            })
            .invoke(&mut redis);

        dbg!(&res);

        assert!(res.is_ok());

        let res: MoveToActiveReturn<String> = res.unwrap();

        dbg!(res);
    }
}
