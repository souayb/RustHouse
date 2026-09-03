//! Authentication: turns request credentials into a `Principal` (API-SQL-002/003).

use crate::catalog::Principal;
use crate::error::{Result, RustHouseError};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthMaterial {
    Bearer(String),
    ClientCert {
        subject_dn: String,
        fingerprint: String,
    },
    None,
}

impl AuthMaterial {
    /// From an `Authorization` header value.
    pub fn from_authorization_header(value: Option<&str>) -> Self {
        match value {
            Some(v) => match v
                .strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
            {
                Some(tok) if !tok.trim().is_empty() => AuthMaterial::Bearer(tok.trim().to_string()),
                _ => AuthMaterial::None,
            },
            None => AuthMaterial::None,
        }
    }
}

pub trait Authenticator: Send + Sync {
    /// Any failure is `RustHouseError::Unauthenticated`; never leak why.
    fn authenticate(&self, material: &AuthMaterial) -> Result<Principal>;
}

#[derive(Debug, Clone, Deserialize)]
struct StaticEntry {
    id: String,
    #[serde(default)]
    roles: Vec<String>,
    #[serde(default)]
    attributes: BTreeMap<String, String>,
    #[serde(default)]
    expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Default, Deserialize)]
struct StaticFile {
    #[serde(default)]
    tokens: HashMap<String, StaticEntry>,
    #[serde(default)]
    certificates: HashMap<String, StaticEntry>,
}

/// Dev/test authenticator backed by a JSON file:
/// `{"tokens": {"<token>": {"id", "roles", "attributes", "expires_at"}}, "certificates": {"<fingerprint>": {...}}}`.
#[derive(Debug, Default)]
pub struct StaticFileAuthenticator {
    file: StaticFile,
}

impl StaticFileAuthenticator {
    pub fn from_json(json: &str) -> Result<Self> {
        let file: StaticFile = serde_json::from_str(json)?;
        Ok(Self { file })
    }

    pub fn load(path: &Path) -> Result<Self> {
        Self::from_json(&std::fs::read_to_string(path)?)
    }

    /// Authenticator that accepts nobody (safe default when no auth file is configured).
    pub fn deny_all() -> Self {
        Self::default()
    }

    fn to_principal(entry: &StaticEntry) -> Result<Principal> {
        if let Some(exp) = entry.expires_at {
            if exp <= Utc::now() {
                return Err(RustHouseError::Unauthenticated);
            }
        }
        Ok(Principal {
            id: entry.id.clone(),
            roles: entry.roles.clone(),
            attributes: entry.attributes.clone(),
        })
    }
}

impl Authenticator for StaticFileAuthenticator {
    fn authenticate(&self, material: &AuthMaterial) -> Result<Principal> {
        let entry = match material {
            AuthMaterial::Bearer(tok) => self.file.tokens.get(tok),
            AuthMaterial::ClientCert { fingerprint, .. } => self.file.certificates.get(fingerprint),
            AuthMaterial::None => None,
        };
        match entry {
            Some(e) if !e.id.is_empty() => Self::to_principal(e),
            _ => Err(RustHouseError::Unauthenticated),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_file_authenticator_parses_and_maps() {
        // REQ: API-SQL-002, API-SQL-003
        let a = StaticFileAuthenticator::from_json(
            r#"{"tokens": {"t1": {"id": "alice", "roles": ["analyst"], "attributes": {"country": "FR"}},
                           "old": {"id": "bob", "expires_at": "2000-01-01T00:00:00Z"}},
                "certificates": {"ab:cd": {"id": "svc", "roles": ["ingest"]}}}"#,
        )
        .unwrap();
        let p = a.authenticate(&AuthMaterial::Bearer("t1".into())).unwrap();
        assert_eq!(p.id, "alice");
        assert!(p.has_role("analyst"));
        assert_eq!(p.attributes["country"], "FR");
        assert!(matches!(
            a.authenticate(&AuthMaterial::Bearer("old".into())),
            Err(RustHouseError::Unauthenticated)
        ));
        assert!(matches!(
            a.authenticate(&AuthMaterial::Bearer("nope".into())),
            Err(RustHouseError::Unauthenticated)
        ));
        assert!(matches!(
            a.authenticate(&AuthMaterial::None),
            Err(RustHouseError::Unauthenticated)
        ));
        let svc = a
            .authenticate(&AuthMaterial::ClientCert {
                subject_dn: "CN=svc".into(),
                fingerprint: "ab:cd".into(),
            })
            .unwrap();
        assert_eq!(svc.id, "svc");
        assert_eq!(
            AuthMaterial::from_authorization_header(Some("Bearer  t1 ")),
            AuthMaterial::Bearer("t1".into())
        );
        assert_eq!(
            AuthMaterial::from_authorization_header(Some("Basic xyz")),
            AuthMaterial::None
        );
    }
}
