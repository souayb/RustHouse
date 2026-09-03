# GOV-ENC: Column encryption, key management, and crypto-shredding

| Field | Value |
|-------|-------|
| Status | Draft |
| Owner | |
| Related ADRs | ADR-0001, ADR-0002, ADR-0003, ADR-0005 |
| Depends on | LAKE-TXN, LAKE-PQT, LAKE-CAT, GOV-CLS, GOV-ACC, GOV-AUD |
| Milestone | M3 |

## 1. Problem and motivation
PCI DSS requirement 3 demands that stored cardholder data be rendered unreadable; GDPR Art. 32
names encryption as an appropriate technical measure and Art. 17 requires erasure that a
seven-year MiFID II archive cannot honour by deleting files. DORA asks for evidence that data
integrity and confidentiality survive an ICT incident, including theft of a disk or backup.
In the hybrid architecture (ADR-0001) data lives as open Parquet files that any tool can read,
which is exactly the interoperability we want and exactly the exposure a bank cannot accept
for `Restricted` or `PCI` columns.

This spec adds envelope encryption at the column level: a data encryption key (DEK) per table
or per column, wrapped by a key-encryption key (KEK) held by a pluggable `KeyProvider`.
Columns classified `Restricted` or tagged `PCI` (GOV-CLS) are encrypted at rest without the
producer having to ask. Destroying a DEK (crypto-shredding) makes the data it protected
unrecoverable without touching a single Parquet file, which is how GOV-RET erases subjects
from cold history while the immutable log stays byte-identical.

## 2. Scope
### In scope
* Envelope encryption model, `KeyProvider` trait, local file provider for dev and tests.
* Automatic encryption for `Restricted`/`PCI` columns; explicit `ENCRYPTED` column option.
* Key scopes: table, column, subject (for crypto-shredding by subject).
* Key rotation (re-wrap), crypto-shredding, denied reads without key access.
* Key usage audit, key material hygiene (never logged), `keys/` layout and permissions.
### Out of scope
* HSM/KMS provider implementations (interface only; a `MockKms` for tests).
* Encryption of the audit log and the rhlog itself (integrity is covered by GOV-AUD hash
  chaining; confidentiality of metadata is a later spec).
* Transport encryption (TLS at the HTTP layer, API-SQL).

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| GOV-ENC-001 | MUST | Reading an encrypted column when the principal's role has no `DECRYPT` grant on it, or when the DEK is unavailable or destroyed, MUST return `RustHouseError::Denied` or `KeyUnavailable` respectively and MUST NOT return ciphertext, garbage, or nulls in place of values. |
| GOV-ENC-002 | MUST | Data MUST be encrypted with per-scope DEKs (AES-256-GCM, unique nonce per Parquet page or per column chunk) that are stored only in wrapped form under `<data_dir>/keys/`, wrapped by a KEK obtained from a `KeyProvider`. |
| GOV-ENC-003 | MUST | `KeyProvider` MUST be a trait (`wrap`, `unwrap`, `kek_id`, `rotate_kek`) with a `LocalFileKeyProvider` implementation for development and tests; the provider in use MUST be selected by configuration and MUST be recorded in the catalog. |
| GOV-ENC-004 | MUST | Any column classified `Restricted` or tagged `PCI` in GOV-CLS MUST be encrypted at rest; a commit that would write such a column in plaintext MUST be refused with `Denied("encryption required")`. |
| GOV-ENC-005 | MUST | Column-level encryption MUST use Parquet modular encryption (`parquet` crate, ADR-0003) when the crate exposes column-key encryption with plaintext footer mode; otherwise it MUST fall back to application-level AES-256-GCM applied to the encoded column bytes before Parquet encoding, and the chosen mode MUST be recorded per data file in the `add` action (`encryption.mode`). |
| GOV-ENC-006 | MUST | Key identifiers, wrapped keys, plaintext DEKs, KEKs, and nonces MUST never appear in tracing output, error messages, audit records, or commit files; only opaque key ids (`kid`) and KEK ids MUST be recorded. |
| GOV-ENC-007 | MUST | `ALTER KEY ROTATE KEK` MUST re-wrap every DEK under the new KEK without rewriting any data file, MUST be atomic per key file (tmp + rename), and MUST leave the previous KEK id in the wrapped record until all DEKs are re-wrapped. |
| GOV-ENC-008 | MUST | `DESTROY KEY <kid>` (crypto-shredding) MUST overwrite and remove the wrapped DEK, MUST require the role `key_custodian`, MUST refuse if any table protected by the key is under legal hold (GOV-RET), and MUST be recorded as an audit record and, when invoked through GOV-RET, as an `ERASE` commit with method `CRYPTO_SHRED`. |
| GOV-ENC-009 | MUST | Subject-scoped DEKs MUST be supported for columns declared `ENCRYPTED BY SUBJECT (<subject_col>)`, so that destroying one subject's DEK affects only that subject's values across all snapshots. |
| GOV-ENC-010 | MUST | Every wrap, unwrap, rotate, and destroy operation MUST produce a GOV-AUD record containing kid, KEK id, principal, operation, and outcome. |
| GOV-ENC-011 | MUST | `<data_dir>/keys/` MUST be created with mode 0700 and key files with mode 0600; the server MUST refuse to start if the directory is world- or group-readable (fail closed). |
| GOV-ENC-012 | MUST | The `KeyProvider` MUST be unreachable from any code path other than `src/governance/crypto.rs`; the hot tier (HYB-ACC) MUST receive already-decrypted `ColumnData` only through the Governor and MUST re-encrypt derived segments for encrypted columns. |
| GOV-ENC-013 | SHOULD | Encrypted columns SHOULD keep per-column min/max statistics disabled in `add` actions and Parquet metadata to avoid leaking value ranges; pruning on encrypted columns is therefore not available. |
| GOV-ENC-014 | SHOULD | A `MockKmsProvider` SHOULD simulate unavailability and latency so that `KeyUnavailable` paths and the fail-closed behaviour of GOV-ENC-001 are exercised in tests. |
| GOV-ENC-015 | MAY | Backup tooling MAY export data files and the rhlog without `keys/`; the spec notes that such a backup is unreadable without the KEK, which is the intended property. |

## 4. Design sketch

Module: `src/governance/crypto.rs`.

```rust
pub struct Kid(String);            // opaque key id, safe to log
pub enum KeyScope { Table(TableRef), Column(TableRef, String), Subject(TableRef, String, SubjectId) }

pub trait KeyProvider: Send + Sync {
    fn kek_id(&self) -> String;
    fn wrap(&self, dek: &SecretBytes) -> Result<WrappedKey>;
    fn unwrap(&self, wrapped: &WrappedKey) -> Result<SecretBytes>;
    fn rotate_kek(&mut self) -> Result<String>;
}

pub struct LocalFileKeyProvider { kek_path: PathBuf }
pub struct MockKmsProvider { available: AtomicBool, latency: Duration }

pub struct WrappedKey { pub kid: Kid, pub kek_id: String, pub ciphertext: Vec<u8>, pub created_at: u64 }

pub struct KeyStore { dir: PathBuf, provider: Box<dyn KeyProvider>, audit: Arc<AuditSink> }
impl KeyStore {
    pub fn dek_for(&self, scope: &KeyScope, principal: &Principal) -> Result<SecretBytes>; // audited unwrap
    pub fn create_dek(&self, scope: &KeyScope, principal: &Principal) -> Result<Kid>;
    pub fn rotate_kek(&self, principal: &Principal) -> Result<()>;
    pub fn destroy(&self, kid: &Kid, principal: &Principal) -> Result<()>;
}

pub enum EncryptionMode { ParquetModular, AppLevelAesGcm }
```

* `SecretBytes` zeroises on drop (`zeroize` crate) and has a `Debug` impl that prints `[redacted]`
  (GOV-ENC-006).
* **Decision criteria for GOV-ENC-005**: at implementation time, check that the pinned `parquet`
  version supports `FileEncryptionProperties` with per-column keys and plaintext footer; if yes,
  `ParquetModular` is used, which keeps files readable by other Parquet engines given the keys.
  If not, `AppLevelAesGcm` wraps the output of `ColumnData::to_bytes()` in
  `src/storage/column_writer.rs` with a 12-byte random nonce prefix and stores the result as a
  `Binary` column; the file remains valid Parquet but the column is opaque. The mode is written
  into `add.encryption = { mode, kid }` in the rhlog (LAKE-TXN).
* Hook points: `src/lake/parquet.rs` encoder consults GOV-CLS for the table's classification
  and asks `KeyStore` for a DEK per encrypted column before encoding; the decoder does the
  reverse only when the Governor has already granted `DECRYPT` (ADR-0005). The hot tier
  (`src/hot/`) applies the same encrypt step to `col_N.bin` for encrypted columns
  (GOV-ENC-012).
* `keys/` layout: `keys/<kid>.json` = `WrappedKey` serialised; `keys/_kek` (local provider
  only, 0600); `keys/_destroyed/<kid>` tombstone with timestamp and audit id, no key material.
* Subject-scoped DEKs derive `kid` deterministically from `(table, column, subject_hash)`; the
  subject value itself is not stored in the key file (GOV-ENC-006).
* `src/error.rs` gains `KeyUnavailable(String)`; `Denied` is introduced by GOV-ACC.
* `src/config.rs` gains `key_provider: KeyProviderConfig` and `keys_path()`.
* Metrics: `rusthouse_key_ops_total{op, outcome}`, `rusthouse_encrypted_columns`.

## 5. Acceptance scenarios

### GOV-ENC-001: read without key access is denied, never garbage
```gherkin
Given silver.cards.pan classified PCI and encrypted, principal analyst without DECRYPT on pan
When  analyst executes SELECT pan FROM silver.cards
Then  the result is Denied
Given principal auditor with DECRYPT on pan and the KeyProvider reporting unavailable
When  auditor executes SELECT pan FROM silver.cards
Then  the result is KeyUnavailable and no row is returned
```

### GOV-ENC-002: DEKs are stored wrapped only
```gherkin
Given a table with an encrypted column written once
When  every file under <data_dir>/keys/ is inspected
Then  each is a WrappedKey with a kek_id and ciphertext
And   no file contains the plaintext DEK bytes used during the write
```

### GOV-ENC-003: provider is pluggable and recorded
```gherkin
Given config key_provider = LocalFile
When  the server starts and a DEK is created
Then  the catalog records provider = "LocalFileKeyProvider" and the KEK id
And   swapping config to MockKms and restarting uses the mock for the next wrap
```

### GOV-ENC-004: Restricted/PCI columns cannot be written in plaintext
```gherkin
Given silver.cards.pan tagged PCI and no DEK available for the column
When  a batch containing pan is written with encryption disabled by a test hook
Then  the commit is refused with Denied("encryption required")
And   no data file exists
```

### GOV-ENC-005: encryption mode is recorded per file
```gherkin
Given silver.cards with an encrypted column
When  a batch is written
Then  the add action for the new file has encryption.mode in {ParquetModular, AppLevelAesGcm} and encryption.kid
And   opening the file with a plain Parquet reader does not yield the plaintext values
```

### GOV-ENC-006: no key material in logs, errors, audit, or commits
```gherkin
Given tracing at TRACE level captured to a buffer during create_dek, write, read, rotate, destroy
When  the buffer, all audit records, and all rhlog files are scanned
Then  none contains the DEK, KEK, wrapped ciphertext bytes, or any nonce
And   each contains only kid and kek_id strings
```

### GOV-ENC-007: KEK rotation re-wraps without rewriting data
```gherkin
Given 3 DEKs wrapped under KEK k1 and 2 data files
When  ALTER KEY ROTATE KEK is executed
Then  all 3 key files report kek_id = k2
And   both data files have the same size, mtime, and checksum as before
And   reads of encrypted columns still succeed
```

### GOV-ENC-008: crypto-shred requires custodian, respects legal hold, is audited
```gherkin
Given DEK kid-7 protecting silver.cards.pan, principal analyst without key_custodian
When  analyst executes DESTROY KEY kid-7
Then  the result is Denied and the key file still exists
Given silver.cards under LEGAL HOLD 'case-1' and principal custodian with key_custodian
When  custodian executes DESTROY KEY kid-7
Then  the result is LegalHoldActive
When  the hold is released and custodian executes DESTROY KEY kid-7
Then  keys/kid-7.json no longer exists, keys/_destroyed/kid-7 exists, and an audit record DESTROY_KEY exists
And   SELECT pan FROM silver.cards returns KeyUnavailable
```

### GOV-ENC-009: subject-scoped keys isolate one subject
```gherkin
Given silver.customers.ssn declared ENCRYPTED BY SUBJECT (customer_id) with rows for subjects 1 and 2 across versions 1..3
When  the DEK for subject 1 is destroyed
Then  ssn for subject 2 is readable at every version
And   ssn for subject 1 returns KeyUnavailable at every version
```

### GOV-ENC-010: key operations are audited
```gherkin
When  create_dek, dek_for, rotate_kek, and destroy are each invoked once
Then  the audit log contains exactly four records with kinds WRAP, UNWRAP, ROTATE_KEK, DESTROY_KEY
And   each record has kid, kek_id, principal, and outcome
```

### GOV-ENC-011: insecure keys directory prevents startup
```gherkin
Given <data_dir>/keys/ with mode 0755
When  the server starts
Then  startup fails with an error naming the directory permissions
And   no HTTP listener is bound
```

### GOV-ENC-012: only crypto.rs touches the provider; hot tier re-encrypts
```gherkin
Given a gold table with an encrypted column materialised to the hot tier
When  the hot/<table>/v<n>/seg_0/col_N.bin file for that column is read as raw bytes
Then  it does not contain the plaintext values
And   a compile-time visibility check (pub(crate) on KeyProvider consumers) confines provider use to src/governance/crypto.rs
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| GOV-ENC-001 | `fn gov_enc_001_read_without_key_access_denied_not_garbage()` | `tests/spec_gov_enc.rs` | acceptance |
| GOV-ENC-001 | `fn test_decode_never_returns_ciphertext_on_error()` | `src/governance/crypto.rs` | unit |
| GOV-ENC-002 | `fn gov_enc_002_deks_stored_wrapped_only()` | `tests/spec_gov_enc.rs` | acceptance |
| GOV-ENC-002 | `fn prop_encrypt_decrypt_roundtrip_unique_nonce()` | `src/governance/crypto.rs` | property |
| GOV-ENC-003 | `fn gov_enc_003_provider_pluggable_and_recorded()` | `tests/spec_gov_enc.rs` | acceptance |
| GOV-ENC-004 | `fn gov_enc_004_pci_column_plaintext_write_refused()` | `tests/spec_gov_enc.rs` | acceptance |
| GOV-ENC-005 | `fn gov_enc_005_encryption_mode_recorded_per_file()` | `tests/spec_gov_enc.rs` | acceptance |
| GOV-ENC-006 | `fn gov_enc_006_no_key_material_in_logs_audit_or_commits()` | `tests/spec_gov_enc.rs` | acceptance |
| GOV-ENC-006 | `fn test_secret_bytes_debug_is_redacted()` | `src/governance/crypto.rs` | unit |
| GOV-ENC-007 | `fn gov_enc_007_kek_rotation_rewraps_without_rewriting_data()` | `tests/spec_gov_enc.rs` | acceptance |
| GOV-ENC-008 | `fn gov_enc_008_destroy_key_role_hold_and_audit()` | `tests/spec_gov_enc.rs` | acceptance |
| GOV-ENC-009 | `fn gov_enc_009_subject_scoped_key_isolates_subject()` | `tests/spec_gov_enc.rs` | acceptance |
| GOV-ENC-010 | `fn gov_enc_010_key_operations_audited()` | `tests/spec_gov_enc.rs` | acceptance |
| GOV-ENC-011 | `fn gov_enc_011_insecure_keys_dir_blocks_startup()` | `tests/spec_gov_enc.rs` | acceptance |
| GOV-ENC-012 | `fn gov_enc_012_hot_tier_segments_reencrypted()` | `tests/spec_gov_enc.rs` | acceptance |
| GOV-ENC-013 | `fn gov_enc_013_encrypted_columns_have_no_stats()` | `tests/spec_gov_enc.rs` | acceptance |
| GOV-ENC-014 | `fn gov_enc_014_mock_kms_unavailable_fails_closed()` | `tests/spec_gov_enc.rs` | acceptance |

## 7. Open questions
* Parquet modular encryption support in the pinned `parquet` crate version: verify at M3 start;
  this decides GOV-ENC-005's mode.
* Subject key count: one DEK per subject per column can mean millions of key files; consider a
  key-derivation scheme (HKDF from a per-column master DEK + subject hash) with a revocation
  list, and whether that still counts as crypto-shredding for the DPO.
* Windows/WSL permission semantics for GOV-ENC-011 (mode bits on `/mnt/c`).
* Aggregations over encrypted columns (`SUM(pan)` is meaningless, `COUNT` is not): where does
  the planner refuse?

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
