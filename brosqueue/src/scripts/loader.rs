use anyhow::{Error, Result};
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

#[derive(Debug, PartialEq)]
enum ScriptLoaderError {
    CircularDependency,
    IoError(String),
}

#[derive(Debug)]
pub struct Command {
    name: ScriptName,
    lua: String,
}

#[derive(Debug, Clone)]
struct ScriptMetadata {
    parent_token: Option<String>,
    path: PathBuf,
    token: String,
    content: String,
}

impl PartialEq for ScriptMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
    }
}

pub struct ScriptLoader;

impl ScriptLoader {
    pub fn new() -> Self {
        Self
    }

    fn load_command(&self, path: &str) -> Result<Command, ScriptLoaderError> {
        let path = Path::new(path);
        let mut includes: Vec<ScriptMetadata> = Vec::new();

        let content = match fs::read_to_string(path) {
            core::result::Result::Ok(content) => content,
            core::result::Result::Err(_) => return Err(ScriptLoaderError::CircularDependency),
        };

        let mut meta = ScriptMetadata {
            parent_token: None,
            path: path.to_path_buf(),
            token: self.get_path_hash(path),
            content,
        };

        self.resolve_dependencies(&mut meta, &mut includes)?;

        for include in includes.iter().rev() {
            meta.content = meta
                .content
                .replacen(&include.token, include.content.as_str(), 1);
        }

        Ok(Command {
            name: ScriptName::new(".lua").unwrap(),
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
    ) -> Result<(), ScriptLoaderError> {
        let script_dir = script_meta.path.parent().unwrap();

        while let Some(cap) = RE.captures(&script_meta.content.clone()) {
            let (line, [include]) = cap.extract();

            let include_path = if include.ends_with(".lua") {
                script_dir.join(include)
            } else {
                script_dir.join(format!("{}.lua", include))
            };

            let token = self.get_path_hash(&include_path);

            if let Some(parent_token) = &script_meta.parent_token {
                if *parent_token == token {
                    return Err(ScriptLoaderError::CircularDependency);
                }
            }

            let mut include_meta: ScriptMetadata = ScriptMetadata {
                parent_token: Some(script_meta.token.clone()),
                token,
                content: match fs::read_to_string(&include_path) {
                    Ok(content) => content,
                    Err(err) => return Err(ScriptLoaderError::IoError(err.to_string())),
                },
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

    #[test]
    fn detect_circular_dependencies() {
        let loader = ScriptLoader::new();
        let fixture = "./tests/fixtures/scripts/fixture_circular_dependency.lua";
        let script = loader.load_command(fixture);

        assert!(script.is_err());
        assert_eq!(script.err().unwrap(), ScriptLoaderError::CircularDependency);
    }
}
