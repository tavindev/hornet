use anyhow::Result;

pub(crate) mod move_to_active;

pub mod loader;

pub trait Script<ScriptArgs, ScriptReturn> {
    fn run(
        &self,
        queue_name: &str,
        redis: &mut redis::Client,
        opts: ScriptArgs,
    ) -> Result<ScriptReturn>;
}
