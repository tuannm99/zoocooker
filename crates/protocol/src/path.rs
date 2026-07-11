use crate::error::ZkError;

pub fn normalize(input: &str) -> Result<String, ZkError> {
    if input.is_empty() {
        return Err(ZkError::InvalidPath("path is empty".into()));
    }

    if !input.starts_with('/') {
        return Err(ZkError::InvalidPath("path must start with '/'".into()));
    }

    if input == "/" {
        return Ok("/".to_string());
    }

    if input.ends_with('/') {
        return Err(ZkError::InvalidPath("trailing slash is not allowed".into()));
    }

    let mut parts = Vec::new();
    for part in input.split('/').skip(1) {
        if part.is_empty() {
            return Err(ZkError::InvalidPath("empty path segment".into()));
        }
        parts.push(part);
    }

    Ok(format!("/{}", parts.join("/")))
}

pub fn parent_of(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }

    let idx = path.rfind('/')?;
    if idx == 0 {
        Some("/".to_string())
    } else {
        Some(path[..idx].to_string())
    }
}

pub fn leaf_name(path: &str) -> Option<&str> {
    if path == "/" {
        return None;
    }

    path.rsplit('/').next()
}

#[cfg(test)]
mod tests {
    use super::{leaf_name, normalize, parent_of};

    #[test]
    fn normalizes_root() {
        assert_eq!(normalize("/").unwrap(), "/");
    }

    #[test]
    fn rejects_trailing_slash() {
        assert!(normalize("/a/").is_err());
    }

    #[test]
    fn rejects_empty_segments() {
        assert!(normalize("//a").is_err());
        assert!(normalize("/a//b").is_err());
    }

    #[test]
    fn rejects_relative_paths() {
        assert!(normalize("a/b").is_err());
    }

    #[test]
    fn computes_parent_and_leaf() {
        assert_eq!(parent_of("/a/b").unwrap(), "/a");
        assert_eq!(leaf_name("/a/b").unwrap(), "b");
    }
}
