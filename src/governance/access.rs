//! Access control model and read/write decisions (spec GOV-ACC).
//!
//! Policies are catalog data (`CatalogEntry::Grant/RowPolicy/MaskingPolicy`), so they are
//! versioned and time-travelable. Decisions here are pure functions over a principal, a table's
//! owner/schema and the policy set; the plan rewrite itself happens in the Governor rule.

use crate::catalog::Principal;
use crate::error::{Result, RustHouseError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub const ROLE_SECURITY_ADMIN: &str = "security_admin";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Privilege {
    Select,
    SelectColumns { columns: BTreeSet<String> },
    Insert,
    Alter,
    Delete,
    Unmask { column: String },
}

impl fmt::Display for Privilege {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Privilege::Select => f.write_str("SELECT"),
            Privilege::SelectColumns { columns } => {
                write!(
                    f,
                    "SELECT({})",
                    columns.iter().cloned().collect::<Vec<_>>().join(", ")
                )
            }
            Privilege::Insert => f.write_str("INSERT"),
            Privilege::Alter => f.write_str("ALTER"),
            Privilege::Delete => f.write_str("DELETE"),
            Privilege::Unmask { column } => write!(f, "UNMASK({column})"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub role: String,
    pub namespace: String,
    pub table: String,
    pub privilege: Privilege,
    pub granted_by: String,
    /// Catalog version at which the grant was made.
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowPolicy {
    pub name: String,
    pub namespace: String,
    pub table: String,
    pub role: String,
    /// SQL predicate template; `current_attribute('key')` is bound per principal.
    pub predicate_sql: String,
    pub created_by: String,
    pub version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaskKind {
    /// Least restrictive first, so `max` picks the most restrictive on conflict.
    PartialLast4,
    Hash,
    Redact,
    Null,
}

impl MaskKind {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().replace(' ', "").as_str() {
            "hash" => Ok(MaskKind::Hash),
            "redact" => Ok(MaskKind::Redact),
            "partial(last4)" => Ok(MaskKind::PartialLast4),
            "null" => Ok(MaskKind::Null),
            other => Err(RustHouseError::SqlParse(format!(
                "unknown masking kind '{other}'; expected hash | redact | partial(last4) | null"
            ))),
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            MaskKind::Hash => "hash",
            MaskKind::Redact => "redact",
            MaskKind::PartialLast4 => "partial(last4)",
            MaskKind::Null => "null",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaskingPolicy {
    pub name: String,
    pub namespace: String,
    pub table: String,
    pub column: String,
    pub role: String,
    pub kind: MaskKind,
    pub created_by: String,
    pub version: u64,
}

/// Every policy object attached to one table at one catalog version.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PolicySet {
    pub grants: Vec<Grant>,
    pub row_policies: Vec<RowPolicy>,
    pub masking_policies: Vec<MaskingPolicy>,
}

/// Outcome of a read decision (GOV-ACC-004/005/006/007).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadDecision {
    /// Columns the principal may see, in schema order.
    pub visible_columns: Vec<String>,
    /// Bound row-filter predicates (attributes substituted), ANDed.
    pub row_filters: Vec<String>,
    /// Column -> mask to apply.
    pub masks: BTreeMap<String, MaskKind>,
    /// Names of the policies that shaped this decision, for audit.
    pub applied: Vec<String>,
}

impl ReadDecision {
    pub fn is_masked(&self) -> bool {
        !self.masks.is_empty()
    }
}

fn has_privilege<F: Fn(&Privilege) -> bool>(p: &Principal, ps: &PolicySet, pred: F) -> bool {
    ps.grants
        .iter()
        .any(|g| p.has_role(&g.role) && pred(&g.privilege))
}

/// GOV-ACC-001/004: deny by default; owner has every privilege on their own table.
pub fn decide_read(
    principal: &Principal,
    owner: &str,
    schema_columns: &[String],
    policies: &PolicySet,
) -> Result<ReadDecision> {
    if principal.id.is_empty() {
        return Err(RustHouseError::Denied("no principal".into()));
    }
    let is_owner = principal.id == owner;
    let visible_columns: Vec<String> =
        if is_owner || has_privilege(principal, policies, |p| matches!(p, Privilege::Select)) {
            schema_columns.to_vec()
        } else {
            let mut allowed: BTreeSet<String> = BTreeSet::new();
            for g in &policies.grants {
                if let Privilege::SelectColumns { columns } = &g.privilege {
                    if principal.has_role(&g.role) {
                        allowed.extend(columns.iter().cloned());
                    }
                }
            }
            if allowed.is_empty() {
                return Err(RustHouseError::Denied(format!(
                    "principal '{}' has no SELECT grant on this table",
                    principal.id
                )));
            }
            schema_columns
                .iter()
                .filter(|c| allowed.contains(*c))
                .cloned()
                .collect()
        };

    let mut applied = Vec::new();
    let mut row_filters = Vec::new();
    for rp in &policies.row_policies {
        if principal.has_role(&rp.role) {
            row_filters.push(bind_attributes(&rp.predicate_sql, principal)?);
            applied.push(rp.name.clone());
        }
    }

    let mut masks: BTreeMap<String, MaskKind> = BTreeMap::new();
    for mp in &policies.masking_policies {
        if !principal.has_role(&mp.role) || !visible_columns.contains(&mp.column) {
            continue;
        }
        let unmasked = has_privilege(
            principal,
            policies,
            |p| matches!(p, Privilege::Unmask { column } if *column == mp.column),
        );
        if unmasked {
            continue;
        }
        // Conflict: most restrictive wins (open question 3 resolved).
        let entry = masks.entry(mp.column.clone()).or_insert(mp.kind);
        *entry = (*entry).max(mp.kind);
        applied.push(mp.name.clone());
    }
    Ok(ReadDecision {
        visible_columns,
        row_filters,
        masks,
        applied,
    })
}

pub fn decide_write(principal: &Principal, owner: &str, policies: &PolicySet) -> Result<()> {
    if principal.id == owner
        || has_privilege(principal, policies, |p| matches!(p, Privilege::Insert))
    {
        Ok(())
    } else {
        Err(RustHouseError::Denied(format!(
            "principal '{}' has no INSERT grant on this table",
            principal.id
        )))
    }
}

pub fn decide_alter(principal: &Principal, owner: &str, policies: &PolicySet) -> Result<()> {
    if principal.id == owner
        || has_privilege(principal, policies, |p| matches!(p, Privilege::Alter))
    {
        Ok(())
    } else {
        Err(RustHouseError::Denied(format!(
            "principal '{}' has no ALTER grant on this table",
            principal.id
        )))
    }
}

/// GOV-ACC-003: only security admins manage grants and policies.
pub fn require_security_admin(principal: &Principal) -> Result<()> {
    if principal.has_role(ROLE_SECURITY_ADMIN) {
        Ok(())
    } else {
        Err(RustHouseError::Denied(format!(
            "managing grants and policies requires role {ROLE_SECURITY_ADMIN}"
        )))
    }
}

/// Replace every `current_attribute('key')` with a quoted literal from the principal's
/// attributes. Values are SQL-escaped (quotes doubled), never interpolated raw (GOV-ACC-006).
pub fn bind_attributes(template: &str, principal: &Principal) -> Result<String> {
    const NEEDLE: &str = "current_attribute(";
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    loop {
        let lower = rest.to_ascii_lowercase();
        let Some(i) = lower.find(NEEDLE) else {
            out.push_str(rest);
            return Ok(out);
        };
        out.push_str(&rest[..i]);
        let after = &rest[i + NEEDLE.len()..];
        let close = after
            .find(')')
            .ok_or_else(|| RustHouseError::SqlParse("unterminated current_attribute(".into()))?;
        let key = after[..close].trim().trim_matches('\'').to_string();
        let value = principal
            .attributes
            .get(&key)
            .ok_or_else(|| RustHouseError::Denied(format!("missing_attribute:{key}")))?;
        out.push('\'');
        out.push_str(&value.replace('\'', "''"));
        out.push('\'');
        rest = &after[close + 1..];
    }
}

/// Reference implementation of the masks, shared by the UDF and by tests (GOV-ACC-007).
pub fn mask_string(kind: MaskKind, value: &str) -> Option<String> {
    match kind {
        MaskKind::Null => None,
        MaskKind::Redact => Some("[REDACTED]".to_string()),
        MaskKind::Hash => {
            use sha2::{Digest, Sha256};
            let digest = Sha256::digest(value.as_bytes());
            Some(digest.iter().map(|b| format!("{b:02x}")).collect())
        }
        MaskKind::PartialLast4 => {
            let chars: Vec<char> = value.chars().collect();
            let keep = chars.len().min(4);
            let stars = "*".repeat(chars.len() - keep);
            Some(format!(
                "{stars}{}",
                chars[chars.len() - keep..].iter().collect::<String>()
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn ps() -> PolicySet {
        PolicySet::default()
    }
    fn grant(role: &str, privilege: Privilege) -> Grant {
        Grant {
            role: role.into(),
            namespace: "gold".into(),
            table: "t".into(),
            privilege,
            granted_by: "sec".into(),
            version: 1,
        }
    }
    fn cols() -> Vec<String> {
        ["id", "country", "email"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn test_deny_by_default_and_owner_bypass() {
        // REQ: GOV-ACC-001
        let p = Principal::new("alice", vec!["analyst".into()]);
        assert!(matches!(
            decide_read(&p, "owner", &cols(), &ps()),
            Err(RustHouseError::Denied(_))
        ));
        assert!(decide_read(&p, "alice", &cols(), &ps()).is_ok());
        assert!(matches!(
            decide_read(&Principal::new("", vec![]), "owner", &cols(), &ps()),
            Err(RustHouseError::Denied(_))
        ));
    }

    #[test]
    fn test_column_grants_union_in_schema_order() {
        // REQ: GOV-ACC-004
        let p = Principal::new("alice", vec!["a".into(), "b".into()]);
        let mut set = ps();
        set.grants.push(grant(
            "a",
            Privilege::SelectColumns {
                columns: ["email".to_string()].into(),
            },
        ));
        set.grants.push(grant(
            "b",
            Privilege::SelectColumns {
                columns: ["id".to_string()].into(),
            },
        ));
        let d = decide_read(&p, "owner", &cols(), &set).unwrap();
        assert_eq!(d.visible_columns, vec!["id", "email"]);
    }

    #[test]
    fn test_multiple_row_policies_are_anded() {
        // REQ: GOV-ACC-005
        let p = Principal::new("bob", vec!["a".into(), "b".into()]);
        let mut set = ps();
        set.grants.push(grant("a", Privilege::Select));
        for (role, pred) in [
            ("a", "country = 'FR'"),
            ("b", "balance < 25"),
            ("c", "id = 0"),
        ] {
            set.row_policies.push(RowPolicy {
                name: format!("p_{role}"),
                namespace: "gold".into(),
                table: "t".into(),
                role: role.into(),
                predicate_sql: pred.into(),
                created_by: "sec".into(),
                version: 1,
            });
        }
        let d = decide_read(&p, "owner", &cols(), &set).unwrap();
        assert_eq!(d.row_filters, vec!["country = 'FR'", "balance < 25"]);
        assert_eq!(d.applied, vec!["p_a", "p_b"]);
    }

    #[test]
    fn test_predicate_template_bind() {
        // REQ: GOV-ACC-006
        let mut p = Principal::new("a", vec![]);
        p.attributes.insert("country".into(), "FR".into());
        assert_eq!(
            bind_attributes("country = current_attribute('country')", &p).unwrap(),
            "country = 'FR'"
        );
        assert!(matches!(
            bind_attributes("x = current_attribute('desk')", &p),
            Err(RustHouseError::Denied(m)) if m == "missing_attribute:desk"
        ));
        p.attributes.insert("desk".into(), "a'b".into());
        assert_eq!(
            bind_attributes("x = CURRENT_ATTRIBUTE( 'desk' )", &p).unwrap(),
            "x = 'a''b'"
        );
        assert_eq!(
            bind_attributes("no attributes here", &p).unwrap(),
            "no attributes here"
        );
    }

    #[test]
    fn test_mask_conflict_most_restrictive_and_unmask() {
        // REQ: GOV-ACC-007
        let p = Principal::new("a", vec!["r1".into(), "r2".into()]);
        let mut set = ps();
        set.grants.push(grant("r1", Privilege::Select));
        for (role, kind) in [("r1", MaskKind::PartialLast4), ("r2", MaskKind::Null)] {
            set.masking_policies.push(MaskingPolicy {
                name: format!("m_{role}"),
                namespace: "gold".into(),
                table: "t".into(),
                column: "email".into(),
                role: role.into(),
                kind,
                created_by: "sec".into(),
                version: 1,
            });
        }
        let d = decide_read(&p, "owner", &cols(), &set).unwrap();
        assert_eq!(d.masks["email"], MaskKind::Null);
        set.grants.push(grant(
            "r2",
            Privilege::Unmask {
                column: "email".into(),
            },
        ));
        let d = decide_read(&p, "owner", &cols(), &set).unwrap();
        assert!(d.masks.is_empty());
    }

    proptest! {
        /// REQ: GOV-ACC-007 — masks are total on strings and never echo the input except hash length.
        #[test]
        fn test_mask_kinds_are_type_stable(s in "\\PC{0,40}") {
            prop_assert_eq!(mask_string(MaskKind::Null, &s), None);
            prop_assert_eq!(mask_string(MaskKind::Redact, &s).unwrap(), "[REDACTED]");
            let h = mask_string(MaskKind::Hash, &s).unwrap();
            prop_assert_eq!(h.len(), 64);
            let p = mask_string(MaskKind::PartialLast4, &s).unwrap();
            prop_assert_eq!(p.chars().count(), s.chars().count());
            let tail: String = s.chars().rev().take(4).collect::<Vec<_>>().into_iter().rev().collect();
            prop_assert!(p.ends_with(&tail));
        }
    }
}
