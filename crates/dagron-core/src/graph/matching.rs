use regex::Regex;

use crate::errors::DagronError;
use crate::node::NodeId;

use super::DAG;

impl<P> DAG<P> {
    /// Return nodes whose names match the regex pattern.
    pub fn nodes_matching_regex(&self, pattern: &str) -> Result<Vec<NodeId>, DagronError> {
        let re = Regex::new(pattern)
            .map_err(|e| DagronError::Graph(format!("Invalid regex pattern: {e}")))?;

        Ok(self
            .nodes()
            .into_iter()
            .filter(|node| re.is_match(&node.name))
            .collect())
    }

    /// Return nodes whose names match a glob pattern (* and ? wildcards).
    pub fn nodes_matching_glob(&self, pattern: &str) -> Result<Vec<NodeId>, DagronError> {
        let regex_pattern = glob_to_regex(pattern);
        self.nodes_matching_regex(&regex_pattern)
    }
}

/// Convert a glob pattern to a regex pattern.
/// Escapes regex metacharacters and translates * → .* and ? → .
fn glob_to_regex(glob: &str) -> String {
    let mut regex = String::with_capacity(glob.len() + 4);
    regex.push('^');
    for c in glob.chars() {
        match c {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            '.' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$' | '\\' => {
                regex.push('\\');
                regex.push(c);
            }
            _ => regex.push(c),
        }
    }
    regex.push('$');
    regex
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_to_regex() {
        assert_eq!(glob_to_regex("foo*"), "^foo.*$");
        assert_eq!(glob_to_regex("f?o"), "^f.o$");
        assert_eq!(glob_to_regex("foo.bar"), "^foo\\.bar$");
        assert_eq!(glob_to_regex("*"), "^.*$");
    }
}
