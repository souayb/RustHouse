//! Lake table schema with stable column ids and mandatory classification
//! (LAKE-SCH, GOV-CLS-002/003/010).
//!
//! `meta::Column` remains the hot-tier physical column; a lake `Field` converts into it.

use crate::error::{Result, RustHouseError};
use crate::governance::classification::{Classification, Level};
use crate::meta::{Column, ColumnType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    /// Stable id, never reused within a table (LAKE-SCH).
    pub id: u32,
    pub name: String,
    pub column_type: ColumnType,
    #[serde(default)]
    pub nullable: bool,
    /// `None` only when read from a log written without classification; every constructor
    /// sets it and readers must call [`Schema::require_classified`] (GOV-CLS-010).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classification: Option<Classification>,
}

impl Field {
    /// Id is assigned by [`Schema::new`] / [`Schema::add_field`].
    pub fn new(
        name: impl Into<String>,
        column_type: ColumnType,
        classification: Classification,
    ) -> Self {
        Self {
            id: 0,
            name: name.into(),
            column_type,
            nullable: false,
            classification: Some(classification),
        }
    }

    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn classification(&self) -> Result<&Classification> {
        self.classification
            .as_ref()
            .ok_or_else(|| RustHouseError::MissingClassification(self.name.clone()))
    }

    pub fn to_column(&self) -> Column {
        Column {
            name: self.name.clone(),
            column_type: self.column_type.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Vec<Field>,
    pub next_column_id: u32,
}

impl Schema {
    /// Assigns column ids 1..n in order.
    pub fn new(fields: Vec<Field>) -> Self {
        let mut s = Self {
            fields: Vec::with_capacity(fields.len()),
            next_column_id: 1,
        };
        for f in fields {
            s.add_field(f);
        }
        s
    }

    pub fn add_field(&mut self, mut field: Field) -> u32 {
        field.id = self.next_column_id;
        self.next_column_id += 1;
        let id = field.id;
        self.fields.push(field);
        id
    }

    pub fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    pub fn names(&self) -> Vec<String> {
        self.fields.iter().map(|f| f.name.clone()).collect()
    }

    pub fn to_columns(&self) -> Vec<Column> {
        self.fields.iter().map(Field::to_column).collect()
    }

    /// Fail closed: every field must carry a classification (GOV-CLS-001/010, LAKE-CAT-011).
    pub fn require_classified(&self) -> Result<()> {
        let missing: Vec<&str> = self
            .fields
            .iter()
            .filter(|f| f.classification.is_none())
            .map(|f| f.name.as_str())
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(RustHouseError::MissingClassification(format!(
                "columns without classification: {}",
                missing.join(", ")
            )))
        }
    }

    /// Table-level classification: max level, union of tags (GOV-CLS-003).
    pub fn classification(&self) -> Result<Classification> {
        self.require_classified()?;
        let mut acc = Classification::new(Level::Public);
        for f in &self.fields {
            acc = acc.merge(f.classification()?);
        }
        Ok(acc)
    }

    /// An explicitly requested table level may not be lower than the computed one (GOV-CLS-003).
    pub fn validate_table_level(&self, requested: Level) -> Result<()> {
        let computed = self.classification()?;
        if requested < computed.level {
            return Err(RustHouseError::ClassificationLowering(format!(
                "table level {requested} is below the computed column maximum {}",
                computed.level
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::classification::Tag;

    #[test]
    fn test_schema_assigns_stable_ids() {
        let mut s = Schema::new(vec![
            Field::new("a", ColumnType::Int64, Classification::new(Level::Public)),
            Field::new("b", ColumnType::String, Classification::new(Level::Public)),
        ]);
        assert_eq!(s.fields[0].id, 1);
        assert_eq!(s.fields[1].id, 2);
        let id = s.add_field(Field::new(
            "c",
            ColumnType::Boolean,
            Classification::new(Level::Public),
        ));
        assert_eq!(id, 3);
        assert_eq!(s.next_column_id, 4);
    }

    #[test]
    fn test_schema_classification_roundtrip_json() {
        let s = Schema::new(vec![Field::new(
            "email",
            ColumnType::String,
            Classification::new(Level::Confidential).with_tags([Tag::Pii]),
        )]);
        let json = serde_json::to_string(&s).unwrap();
        let back: Schema = serde_json::from_str(&json).unwrap();
        assert_eq!(back, s);
        assert_eq!(back.classification().unwrap().level, Level::Confidential);
    }
}
