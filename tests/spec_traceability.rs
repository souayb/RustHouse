//! Requirement traceability gate (ADR-0004).
//!
//! Every `MUST` requirement in a spec whose status is `Accepted` or `Implemented` must be
//! referenced by at least one test. A reference is any of:
//!   * a function whose name contains the ID lowercased with underscores (`gov_cls_001`),
//!   * a `// REQ: <ID>` comment anywhere under `src/` or `tests/`,
//!   * a test listed in the spec's test-plan table for that ID that exists as `fn <name>(`.
//!
//! Additionally, every ID referenced from code must be defined by exactly one spec.
//!
//! `scripts/spec-check.sh` implements the same rules for CI and generates the matrix.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug)]
struct Spec {
    file: String,
    status: String,
    reqs: Vec<(String, String)>,            // (id, level)
    planned: BTreeMap<String, Vec<String>>, // id -> test fn names
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn walk(dir: &Path, ext: &str, out: &mut Vec<PathBuf>) {
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, ext, out);
            } else if p.extension().and_then(|s| s.to_str()) == Some(ext) {
                out.push(p);
            }
        }
    }
}

fn is_req_id(s: &str) -> bool {
    // AREA-TOPIC-nnn with uppercase letters and 3 digits
    let parts: Vec<&str> = s.split('-').collect();
    parts.len() == 3
        && parts[..2]
            .iter()
            .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_uppercase()))
        && parts[2].len() == 3
        && parts[2].chars().all(|c| c.is_ascii_digit())
}

fn parse_spec(path: &Path) -> Spec {
    let text = fs::read_to_string(path).unwrap();
    let mut status = String::new();
    let mut reqs = Vec::new();
    let mut planned: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for line in text.lines() {
        let cells: Vec<&str> = line
            .trim()
            .trim_matches('|')
            .split('|')
            .map(|c| c.trim())
            .collect();
        if cells.len() >= 2 && cells[0] == "Status" {
            status = cells[1].to_string();
        }
        if cells.len() >= 3 && is_req_id(cells[0]) {
            if matches!(cells[1], "MUST" | "SHOULD" | "MAY") {
                reqs.push((cells[0].to_string(), cells[1].to_string()));
            } else if let Some(name) = cells[1]
                .trim_matches('`')
                .strip_prefix("fn ")
                .and_then(|s| s.strip_suffix("()"))
            {
                planned
                    .entry(cells[0].to_string())
                    .or_default()
                    .push(name.to_string());
            }
        }
    }
    Spec {
        file: path.file_name().unwrap().to_string_lossy().into_owned(),
        status,
        reqs,
        planned,
    }
}

struct CodeRefs {
    fn_names: BTreeSet<String>,
    req_comments: BTreeSet<String>,
}

fn scan_code(root: &Path) -> CodeRefs {
    let mut files = Vec::new();
    walk(&root.join("src"), "rs", &mut files);
    walk(&root.join("tests"), "rs", &mut files);
    let mut fn_names = BTreeSet::new();
    let mut req_comments = BTreeSet::new();
    for f in files {
        let text = fs::read_to_string(&f).unwrap();
        for line in text.lines() {
            let t = line.trim();
            if let Some(rest) = t
                .strip_prefix("fn ")
                .or_else(|| t.strip_prefix("pub fn "))
                .or_else(|| t.strip_prefix("async fn "))
                .or_else(|| t.strip_prefix("pub async fn "))
            {
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if !name.is_empty() {
                    fn_names.insert(name);
                }
            }
            if let Some(idx) = t.find("REQ:") {
                for tok in t[idx + 4..].split(|c: char| c.is_whitespace() || c == ',') {
                    let tok = tok.trim();
                    if is_req_id(tok) {
                        req_comments.insert(tok.to_string());
                    }
                }
            }
        }
    }
    CodeRefs {
        fn_names,
        req_comments,
    }
}

fn id_as_fn_prefix(id: &str) -> String {
    id.to_ascii_lowercase().replace('-', "_")
}

#[test]
fn spec_traceability_gate() {
    let root = repo_root();
    let mut spec_files = Vec::new();
    walk(&root.join("specs").join("features"), "md", &mut spec_files);
    assert!(
        !spec_files.is_empty(),
        "no specs found under specs/features"
    );

    let specs: Vec<Spec> = spec_files.iter().map(|p| parse_spec(p)).collect();
    let code = scan_code(&root);

    let mut failures: Vec<String> = Vec::new();

    // 1. IDs are unique across specs and every spec has a known status.
    let mut seen: BTreeMap<String, String> = BTreeMap::new();
    for s in &specs {
        if !matches!(
            s.status.as_str(),
            "Draft" | "Accepted" | "Implemented" | "Withdrawn"
        ) {
            failures.push(format!(
                "{}: unknown or missing status '{}'",
                s.file, s.status
            ));
        }
        if s.reqs.is_empty() {
            failures.push(format!("{}: no requirements table rows found", s.file));
        }
        for (id, _) in &s.reqs {
            if let Some(prev) = seen.insert(id.clone(), s.file.clone()) {
                failures.push(format!(
                    "duplicate requirement id {id} in {} and {}",
                    prev, s.file
                ));
            }
        }
    }

    // 2. Gated specs: every MUST has a reference.
    for s in &specs {
        if !matches!(s.status.as_str(), "Accepted" | "Implemented") {
            continue;
        }
        for (id, level) in &s.reqs {
            if level != "MUST" {
                continue;
            }
            let prefix = id_as_fn_prefix(id);
            let by_name = code.fn_names.iter().any(|f| f.contains(&prefix));
            let by_comment = code.req_comments.contains(id);
            let by_plan = s
                .planned
                .get(id)
                .map(|names| names.iter().any(|n| code.fn_names.contains(n)))
                .unwrap_or(false);
            if !(by_name || by_comment || by_plan) {
                failures.push(format!(
                    "{}: {id} (MUST, status {}) has no test reference",
                    s.file, s.status
                ));
            }
        }
    }

    // 3. Orphans: code references an ID no spec defines.
    let defined: BTreeSet<&String> = seen.keys().collect();
    for id in &code.req_comments {
        if !defined.contains(id) {
            failures.push(format!(
                "code references undefined requirement {id} via REQ: comment"
            ));
        }
    }
    for f in &code.fn_names {
        // pattern: <area>_<topic>_<nnn>_...
        let parts: Vec<&str> = f.split('_').collect();
        if parts.len() >= 3 && parts[2].len() == 3 && parts[2].chars().all(|c| c.is_ascii_digit()) {
            let candidate = format!(
                "{}-{}-{}",
                parts[0].to_ascii_uppercase(),
                parts[1].to_ascii_uppercase(),
                parts[2]
            );
            if is_req_id(&candidate) && !defined.contains(&candidate) {
                failures.push(format!(
                    "fn {f} looks like a requirement test but {candidate} is not defined"
                ));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "spec traceability failures:\n  {}",
        failures.join("\n  ")
    );
}
