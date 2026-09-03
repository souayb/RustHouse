# Glossary

| Term | Meaning in RustHouse |
|------|----------------------|
| **Warehouse (as-built)** | The current engine: proprietary LZ4 columnar segments, `metadata.json`, min/max pruning, SQL planner. |
| **Lakehouse** | Open data files (Parquet) on cheap storage plus a table format (transaction log) that gives ACID commits, schema evolution, and time travel. |
| **Hybrid** | Lakehouse tables as the system of record, with the existing segment format used as a rebuildable *hot tier* for low-latency serving. |
| **Table format** | The transaction log and metadata that turn a directory of Parquet files into a versioned table (Delta Lake / Apache Iceberg in the industry; `rhlog` here). |
| **Commit / Version** | One atomic change to a table. Versions are monotonically increasing `u64`. |
| **Snapshot** | The set of data files visible at a given version. |
| **Time travel** | Querying a table `AS OF VERSION n` or `AS OF TIMESTAMP t`. |
| **Medallion layers** | Bronze (raw, immutable ingest), Silver (validated, conformed), Gold (business-ready aggregates). |
| **Hot tier** | Derived segment files built from a Gold/Silver table version, used to accelerate queries. Always rebuildable from the lake. |
| **Principal** | An authenticated identity (user or service) executing an operation. |
| **Classification** | Sensitivity label on a column or table: `Public`, `Internal`, `Confidential`, `Restricted`, with tags such as `PII`, `PCI`, `MNPI`. |
| **Masking policy** | A rule that rewrites a column's values (hash, redact, partial, null) for principals without clearance. |
| **Row filter policy** | A predicate appended to every scan of a table for a given role (row-level security). |
| **Audit log** | Append-only, hash-chained record of every DDL/DML/query/policy decision. |
| **Lineage** | Graph of which tables/columns were derived from which, at which version. |
| **Retention policy** | Rule that defines how long a table version or row set must be kept before vacuum may delete it. |
| **Legal hold** | A flag that suspends retention and erasure for a table until released by an authorised principal. |
| **Erasure request** | A request to make specific subjects' data unreadable (GDPR Art. 17), executed by rewrite or crypto-shredding. |
| **Data contract** | Machine-checkable schema + quality rules a producer promises for a table. |
| **Quarantine** | Table receiving rows that violate a data contract instead of the target table. |
| **BCBS 239** | Basel Committee principles for risk data aggregation: accuracy, completeness, timeliness, adaptability, traceability. |
| **DORA** | EU Digital Operational Resilience Act: ICT risk, incident reporting, resilience testing. |
| **MiFID II** | EU markets directive: record keeping of orders/transactions for 5-7 years. |
| **SOX** | Sarbanes-Oxley: controls over financial reporting, audit trail integrity. |
| **GDPR** | EU data protection: lawful basis, minimisation, erasure, access requests. |
