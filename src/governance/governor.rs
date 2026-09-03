//! The single governance enforcement point (ADR-0005). M1 scope: classification fail-closed
//! checks (GOV-CLS-009/010/011). Grants, row filters and masking arrive with GOV-ACC (M2).

use crate::catalog::Principal;
use crate::error::{Result, RustHouseError};
use crate::governance::classification::Classification;
use crate::lake::schema::Schema;
use crate::sql::LogicalPlan;
use std::collections::BTreeMap;

pub const ROLE_CLASSIFICATION_ADMIN: &str = "classification_admin";
pub const ROLE_METRICS_READER: &str = "metrics_reader";

/// What a governed read is allowed to touch, and how sensitive it is (GOV-CLS-009).
#[derive(Debug, Clone, PartialEq)]
pub struct GovernedRead {
    /// Concrete column names (wildcards expanded), in plan order.
    pub columns: Vec<String>,
    pub classifications: BTreeMap<String, Classification>,
}

pub struct Governor;

impl Governor {
    /// Fail closed: every column must be classified and exist (GOV-CLS-010).
    pub fn authorise_read(
        _principal: &Principal,
        plan: &LogicalPlan,
        schema: &Schema,
    ) -> Result<GovernedRead> {
        schema.require_classified()?;
        let mut columns: Vec<String> = Vec::new();
        for c in plan.referenced_columns() {
            if c == "*" {
                for name in schema.names() {
                    if !columns.contains(&name) {
                        columns.push(name);
                    }
                }
            } else if !columns.contains(&c) {
                columns.push(c);
            }
        }
        let mut classifications = BTreeMap::new();
        for c in &columns {
            let field = schema
                .field(c)
                .ok_or_else(|| RustHouseError::ColumnNotFound(c.clone()))?;
            classifications.insert(c.clone(), field.classification()?.clone());
        }
        Ok(GovernedRead {
            columns,
            classifications,
        })
    }

    pub fn authorise_write(_principal: &Principal, schema: &Schema) -> Result<()> {
        schema.require_classified()
    }

    /// GOV-CLS-011: lowering needs the admin role and a reason.
    pub fn authorise_reclassify(
        principal: &Principal,
        previous: &Classification,
        next: &Classification,
        reason: Option<&str>,
    ) -> Result<()> {
        if next.is_lowering_of(previous) {
            if !principal.has_role(ROLE_CLASSIFICATION_ADMIN) {
                return Err(RustHouseError::Denied(format!(
                    "lowering a classification ({previous} -> {next}) requires role {ROLE_CLASSIFICATION_ADMIN}"
                )));
            }
            if reason.map(str::trim).filter(|r| !r.is_empty()).is_none() {
                return Err(RustHouseError::SqlParse(
                    "REASON '<text>' is required when lowering a classification".into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::classification::{Level, Tag};
    use crate::lake::schema::Field;
    use crate::meta::ColumnType;
    use crate::sql::Planner;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new(
                "id",
                ColumnType::Int64,
                Classification::new(Level::Internal),
            ),
            Field::new(
                "email",
                ColumnType::String,
                Classification::new(Level::Confidential).with_tags([Tag::Pii]),
            ),
        ])
    }

    #[test]
    fn test_authorise_read_expands_wildcard_and_collects_classifications() {
        // REQ: GOV-CLS-009
        let plan = Planner::new()
            .plan_sql("SELECT * FROM t WHERE id > 1")
            .unwrap();
        let g = Governor::authorise_read(&Principal::new("a", vec![]), &plan, &schema()).unwrap();
        assert_eq!(g.columns, vec!["id", "email"]);
        assert_eq!(g.classifications["email"].level, Level::Confidential);
        let plan = Planner::new().plan_sql("SELECT nope FROM t").unwrap();
        assert!(matches!(
            Governor::authorise_read(&Principal::new("a", vec![]), &plan, &schema()),
            Err(RustHouseError::ColumnNotFound(_))
        ));
    }

    #[test]
    fn test_authorise_reclassify_rules() {
        // REQ: GOV-CLS-011
        let prev = Classification::new(Level::Confidential);
        let lower = Classification::new(Level::Internal);
        let higher = Classification::new(Level::Restricted);
        let analyst = Principal::new("a", vec![]);
        let dpo = Principal::new("d", vec![ROLE_CLASSIFICATION_ADMIN.into()]);
        assert!(matches!(
            Governor::authorise_reclassify(&analyst, &prev, &lower, Some("x")),
            Err(RustHouseError::Denied(_))
        ));
        assert!(matches!(
            Governor::authorise_reclassify(&dpo, &prev, &lower, None),
            Err(RustHouseError::SqlParse(_))
        ));
        Governor::authorise_reclassify(&dpo, &prev, &lower, Some("ticket")).unwrap();
        Governor::authorise_reclassify(&analyst, &prev, &higher, None).unwrap();
    }
}
