use anyhow::Result;

pub(crate) mod add_standard_job;
pub(crate) mod loader;
pub(crate) mod move_to_active;

pub trait Script<ScriptArgs, ScriptReturn> {
    fn run(
        &self,
        queue_name: &str,
        redis: &mut redis::Client,
        opts: ScriptArgs,
    ) -> Result<ScriptReturn>;
}
