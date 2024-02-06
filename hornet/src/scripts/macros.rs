#[macro_export]
macro_rules! generate_script_struct {
    ($struct_name:ident, $string_path:expr) => {
        use super::loader::load_redis_script;

        pub struct $struct_name(pub redis::Script);

        impl $struct_name {
            pub fn new() -> Self {
                let script = load_redis_script($string_path);

                match script {
                    Ok(script) => $struct_name(script),
                    Err(e) => panic!("Error: {:?}", e),
                }
            }
        }
    };
}
