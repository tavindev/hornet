use super::loader::load_redis_script;

pub struct AddStandardJob(pub redis::Script);

impl AddStandardJob {
    pub fn new() -> Self {
        let script = load_redis_script("./src/scripts/commands/addStandardJob-7.lua");

        match script {
            Ok(script) => AddStandardJob(script),
            Err(e) => panic!("Error: {:?}", e),
        }
    }
}
