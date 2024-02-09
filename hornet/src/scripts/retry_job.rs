use anyhow::Result;
use redis::{FromRedisValue, ToRedisArgs};
use serde::Serialize;
use std::time::SystemTime;

use crate::{generate_script_struct, queue_keys::QueueKeys};

generate_script_struct!(RetryJob, "./src/scripts/commands/retryJob-10.lua");

#[derive(Debug, Serialize)]
pub struct RetryJobArgs {
    pub token: String,
    #[serde(rename = "jobId")]
    pub job_id: String,
}

impl ToRedisArgs for RetryJobArgs {
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
pub enum RetryJobReturn {
    Ok,
    MissingKey,
    MissingLock,
}

impl FromRedisValue for RetryJobReturn {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        match v {
            redis::Value::Int(0) => Ok(RetryJobReturn::Ok),
            redis::Value::Int(-1) => Ok(RetryJobReturn::MissingKey),
            redis::Value::Int(-2) => Ok(RetryJobReturn::MissingLock),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Unknown return value",
            ))),
        }
    }
}

impl RetryJob {
    pub fn run(
        &self,
        prefix: &str,
        mut client: &mut redis::Client,
        opts: RetryJobArgs,
    ) -> Result<RetryJobReturn> {
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
            .invoke::<RetryJobReturn>(&mut client)?;

        Ok(res)
    }
}
