use crate::generate_script_struct;
use crate::queue_keys::QueueKeys;
use anyhow::Result;
use redis::FromRedisValue;
use serde::Serialize;
use std::convert::Into;
use std::time::SystemTime;

generate_script_struct!(
    MoveToFinished,
    "./src/scripts/commands/moveToFinished-14.lua"
);

pub enum MoveToFinishedTarget {
    Completed,
    Failed,
}

impl MoveToFinishedTarget {
    pub fn msg_prorperty(&self) -> &'static str {
        match self {
            MoveToFinishedTarget::Completed => "returnvalue",
            MoveToFinishedTarget::Failed => "failedReason",
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            MoveToFinishedTarget::Completed => "completed",
            MoveToFinishedTarget::Failed => "failed",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct KeepJobs {
    pub count: i32,
}

#[derive(Debug, Serialize)]
pub struct MoveToFinishedArgs {
    pub token: String,
    #[serde(rename = "keepJobs")]
    pub keep_jobs: KeepJobs,
    #[serde(rename = "lockDuration")]
    pub lock_duration: u64, // in milliseconds
    #[serde(rename = "attempts")]
    pub max_attempts: u64,
    #[serde(rename = "maxMetricsSize")]
    pub max_metrics_size: u64,
    #[serde(rename = "fpof")]
    pub fail_parent_on_fail: bool,
    #[serde(rename = "rdof")]
    pub remove_dependency_on_fail: bool,
}

#[derive(Debug)]
pub enum MoveToFinishedReturn {
    Ok,
    MissingKey,
    MissingLock,
    JobNotActiveInSet,
    JobHasPendingDependencies,
    LockIsNotOwnedByThisClient,
}

impl FromRedisValue for MoveToFinishedReturn {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        match v {
            redis::Value::Int(0) => Ok(MoveToFinishedReturn::Ok),
            redis::Value::Int(-1) => Ok(MoveToFinishedReturn::MissingKey),
            redis::Value::Int(-2) => Ok(MoveToFinishedReturn::MissingLock),
            redis::Value::Int(-3) => Ok(MoveToFinishedReturn::JobNotActiveInSet),
            redis::Value::Int(-4) => Ok(MoveToFinishedReturn::JobHasPendingDependencies),
            redis::Value::Int(-6) => Ok(MoveToFinishedReturn::LockIsNotOwnedByThisClient),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Unknown return value",
            ))),
        }
    }
}

impl MoveToFinished {
    pub fn run(
        &self,
        prefix: &str,
        mut client: &mut redis::Client,
        job_id: &str,
        return_msg: &str,
        target: MoveToFinishedTarget,
        args: MoveToFinishedArgs,
    ) -> Result<MoveToFinishedReturn> {
        let mut script = &mut self.0.prepare_invoke();

        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();

        let keys = vec![
            QueueKeys::Wait.into(),
            QueueKeys::Active.into(),
            QueueKeys::Prioritized.into(),
            QueueKeys::Events.into(),
            QueueKeys::Stalled.into(),
            QueueKeys::Limiter.into(),
            QueueKeys::Delayed.into(),
            QueueKeys::Paused.into(),
            QueueKeys::Meta.into(),
            QueueKeys::Pc.into(),
            target.as_str(),
            job_id,
            QueueKeys::Metrics.into(),
            QueueKeys::Marker.into(),
        ]
        .iter()
        .map(|s| format!("{}{}", prefix, s))
        .collect::<Vec<String>>();

        for key in keys {
            script = script.key(key)
        }

        let _args = vec![
            job_id,
            timestamp.as_str(),
            target.msg_prorperty(),
            return_msg,
            target.as_str(),
            "false",
            prefix,
        ];

        for arg in _args {
            script = script.arg(arg);
        }

        script = script.arg(rmp_serde::to_vec_named(&args).unwrap());

        let res = script.invoke::<MoveToFinishedReturn>(&mut client)?;

        Ok(res)
    }
}
