use anyhow::{Error, Ok, Result};
use lazy_static::lazy_static;
use regex::Regex;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fs,
    path::Path,
};

lazy_static! {
    static ref RE: Regex =
        Regex::new(r#"(?m)^[-]{2,3}[ \t]*@include[ \t]["']+([^; \t\n]*)["'];?[ \t]?"#).unwrap();
}

#[derive(Debug)]
struct ScriptName(String);

impl ScriptName {
    fn new(name: &str) -> Result<Self> {
        if !name.ends_with(".lua") {
            return Err(Error::msg("Script name must end with .lua"));
        }

        Ok(Self(name.to_string()))
    }
}

#[derive(Debug)]
pub struct Command {
    name: ScriptName,
    lua: String,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct ScriptMetadata {
    path: PathBuf,
    token: String,
    content: String,
}

pub struct ScriptLoader(HashMap<ScriptName, String>);

impl ScriptLoader {
    const SCRIPTS_PATH: &'static str = "./src/scripts/commands";
    const INCLUDES_PATH: &'static str = "./src/scripts/commands/includes";
    const INCLUDES_PREFIX: &'static str = "--- @include";

    pub fn new() -> Self {
        Self(HashMap::new())
    }

    fn load_command(&self, path: &str) -> Result<Command> {
        let path = Path::new(path);
        let mut includes: Vec<ScriptMetadata> = Vec::new();

        let mut meta = ScriptMetadata {
            path: path.to_path_buf(),
            token: self.get_path_hash(path),
            content: fs::read_to_string(path)?,
        };

        self.resolve_dependencies(&mut meta, &mut includes)?;

        for include in includes.iter().rev() {
            meta.content = meta
                .content
                .replacen(&include.token, include.content.as_str(), 1);
        }

        Ok(Command {
            name: ScriptName::new(".lua")?,
            lua: meta.content,
        })
    }

    fn get_path_hash(&self, path: &Path) -> String {
        format!(
            "@@{}",
            self.calculate_hash(path.to_str().unwrap().to_string())
        )
    }

    fn calculate_hash(&self, t: String) -> String {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish().to_string()
    }

    fn resolve_dependencies(
        &self,
        script_meta: &mut ScriptMetadata,
        includes: &mut Vec<ScriptMetadata>,
    ) -> Result<()> {
        let script_dir = script_meta.path.parent().unwrap();

        while let Some(cap) = RE.captures(&script_meta.content.clone()) {
            let (line, [include]) = cap.extract();

            let include_path = if include.ends_with(".lua") {
                script_dir.join(include)
            } else {
                script_dir.join(format!("{}.lua", include))
            };
            let mut include_meta = ScriptMetadata {
                token: self.get_path_hash(&include_path),
                content: fs::read_to_string(&include_path)?,
                path: include_path,
            };

            self.resolve_dependencies(&mut include_meta, includes)?;

            script_meta.content = script_meta.content.replace(line, &include_meta.token);

            if !includes.contains(&include_meta) {
                includes.push(include_meta.clone());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_basic_include() {
        let loader = ScriptLoader::new();
        let fixture = "./tests/fixtures/scripts/fixture_simple_include.lua";
        let command = loader.load_command(fixture);

        assert!(command.is_ok());
    }

    #[test]
    fn removes_include_tag() {
        let loader = ScriptLoader::new();
        let fixture = "./tests/fixtures/scripts/fixture_simple_include.lua";
        let script = loader.load_command(fixture).unwrap().lua;

        assert!(!script.contains("@include"));
    }

    fn parse_included_files(script: String) -> Vec<String> {
        let left = "--- file:";
        let mut res = vec![];

        for line in script.lines() {
            if line.starts_with(left) {
                res.push(line.replace(left, "").trim().to_string());
            }
        }

        res
    }

    #[test]
    fn interpolates_script_exactly_once() {
        let loader = ScriptLoader::new();
        let fixture = "./tests/fixtures/scripts/fixture_duplicate_elimination.lua";
        let script = loader.load_command(fixture).unwrap().lua;
        let includes = parse_included_files(script);
        let count = includes.iter().filter(|i| **i == "strings.lua").count();

        assert_eq!(count, 1);
    }

    #[test]
    fn inserts_scripts_in_dependency_order() {
        let loader = ScriptLoader::new();
        let fixture = "./tests/fixtures/scripts/fixture_recursive_parent.lua";
        let script = loader.load_command(fixture).unwrap().lua;
        let includes = parse_included_files(script);

        let expected = vec![
            "strings.lua",
            "fixture_recursive_great_grandchild.lua",
            "fixture_recursive_grandchild.lua",
            "fixture_recursive_child.lua",
            "fixture_recursive_parent.lua",
        ];

        assert_eq!(includes, expected);
    }
}
