//! Sensitivity classification of columns and tables (spec GOV-CLS).

use crate::error::{Result, RustHouseError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

/// Ordered sensitivity level: `Public < Internal < Confidential < Restricted` (GOV-CLS-002).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Level {
    Public,
    Internal,
    Confidential,
    Restricted,
}

impl FromStr for Level {
    type Err = RustHouseError;
    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "public" => Ok(Level::Public),
            "internal" => Ok(Level::Internal),
            "confidential" => Ok(Level::Confidential),
            "restricted" => Ok(Level::Restricted),
            other => Err(RustHouseError::InvalidData(format!(
                "unknown classification level '{other}'"
            ))),
        }
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Level::Public => "Public",
            Level::Internal => "Internal",
            Level::Confidential => "Confidential",
            Level::Restricted => "Restricted",
        })
    }
}

/// Sensitivity tag. Serialised as a stable string: `PII`, `PCI`, `MNPI`, `SPI`, `FINANCIAL`,
/// or `custom:<name>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub enum Tag {
    Pii,
    Pci,
    Mnpi,
    Spi,
    Financial,
    Custom(String),
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Tag::Pii => f.write_str("PII"),
            Tag::Pci => f.write_str("PCI"),
            Tag::Mnpi => f.write_str("MNPI"),
            Tag::Spi => f.write_str("SPI"),
            Tag::Financial => f.write_str("FINANCIAL"),
            Tag::Custom(name) => write!(f, "custom:{name}"),
        }
    }
}

impl From<Tag> for String {
    fn from(t: Tag) -> String {
        t.to_string()
    }
}

impl TryFrom<String> for Tag {
    type Error = RustHouseError;
    fn try_from(s: String) -> Result<Self> {
        s.parse()
    }
}

impl FromStr for Tag {
    type Err = RustHouseError;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "PII" => Ok(Tag::Pii),
            "PCI" => Ok(Tag::Pci),
            "MNPI" => Ok(Tag::Mnpi),
            "SPI" => Ok(Tag::Spi),
            "FINANCIAL" => Ok(Tag::Financial),
            other => match other.strip_prefix("custom:") {
                Some(name) if !name.is_empty() => Ok(Tag::Custom(name.to_string())),
                _ => Err(RustHouseError::InvalidData(format!(
                    "unknown classification tag '{other}'"
                ))),
            },
        }
    }
}

/// Level plus tag set carried by every lake column (GOV-CLS-002).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Classification {
    pub level: Level,
    #[serde(default)]
    pub tags: BTreeSet<Tag>,
}

impl Classification {
    pub fn new(level: Level) -> Self {
        Self {
            level,
            tags: BTreeSet::new(),
        }
    }

    pub fn with_tags<I: IntoIterator<Item = Tag>>(mut self, tags: I) -> Self {
        self.tags.extend(tags);
        self
    }

    /// Max level, union of tags (GOV-CLS-003, GOV-CLS-006).
    pub fn merge(&self, other: &Self) -> Self {
        Self {
            level: self.level.max(other.level),
            tags: self.tags.union(&other.tags).cloned().collect(),
        }
    }

    /// True if `self` is less protective than `prev` in level or drops a tag (GOV-CLS-011).
    pub fn is_lowering_of(&self, prev: &Self) -> bool {
        self.level < prev.level || !prev.tags.is_subset(&self.tags)
    }

    /// Whether any tag mandates default masking (GOV-CLS-007).
    pub fn requires_default_masking(&self) -> bool {
        self.tags
            .iter()
            .any(|t| matches!(t, Tag::Pii | Tag::Pci | Tag::Spi))
    }
}

impl fmt::Display for Classification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{{", self.level)?;
        for (i, t) in self.tags.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{t}")?;
        }
        f.write_str("}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_level_is_totally_ordered() {
        // REQ: GOV-CLS-002
        let mut levels = vec![
            Level::Restricted,
            Level::Public,
            Level::Confidential,
            Level::Internal,
        ];
        levels.sort();
        assert_eq!(
            levels,
            vec![
                Level::Public,
                Level::Internal,
                Level::Confidential,
                Level::Restricted
            ]
        );
        for a in &levels {
            for b in &levels {
                assert!(a.partial_cmp(b).is_some());
            }
        }
    }

    #[test]
    fn test_classification_merge_max_and_union() {
        // REQ: GOV-CLS-003
        let a = Classification::new(Level::Internal).with_tags([Tag::Pii]);
        let b = Classification::new(Level::Restricted).with_tags([Tag::Pci]);
        let m = a.merge(&b);
        assert_eq!(m.level, Level::Restricted);
        assert_eq!(m.tags.len(), 2);
        assert_eq!(m, b.merge(&a), "merge is commutative");
        assert_eq!(m.merge(&m), m, "merge is idempotent");
    }

    #[test]
    fn test_lowering_detection() {
        // REQ: GOV-CLS-011
        let prev = Classification::new(Level::Confidential).with_tags([Tag::Pii]);
        assert!(Classification::new(Level::Internal)
            .with_tags([Tag::Pii])
            .is_lowering_of(&prev));
        assert!(Classification::new(Level::Confidential).is_lowering_of(&prev));
        assert!(!Classification::new(Level::Restricted)
            .with_tags([Tag::Pii, Tag::Spi])
            .is_lowering_of(&prev));
    }

    #[test]
    fn test_tag_string_roundtrip() {
        for t in [
            Tag::Pii,
            Tag::Pci,
            Tag::Mnpi,
            Tag::Spi,
            Tag::Financial,
            Tag::Custom("desk".into()),
        ] {
            let s = t.to_string();
            assert_eq!(s.parse::<Tag>().unwrap(), t);
        }
        assert!("custom:".parse::<Tag>().is_err());
        assert!("bogus".parse::<Tag>().is_err());
    }
}
