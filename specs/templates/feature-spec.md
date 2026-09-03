# <AREA>-<TOPIC>: <Feature title>

| Field | Value |
|-------|-------|
| Status | Draft / Accepted / Implemented / Withdrawn |
| Owner | |
| Related ADRs | ADR-000x |
| Depends on | <other spec IDs> |
| Milestone | M<n> |

## 1. Problem and motivation
Why this exists. Who asked for it. Which regulation or user need it serves.

## 2. Scope
### In scope
### Out of scope

## 3. Requirements
Use RFC 2119 keywords. One requirement per row. IDs are permanent.

| ID | Level | Requirement |
|----|-------|-------------|
| <AREA>-<TOPIC>-001 | MUST | ... |
| <AREA>-<TOPIC>-002 | SHOULD | ... |

## 4. Design sketch
Data structures, module placement (`src/...`), public API, error variants, metrics.

## 5. Acceptance scenarios
One per MUST, Given/When/Then.

### <AREA>-<TOPIC>-001: <scenario title>
```gherkin
Given ...
When  ...
Then  ...
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| <AREA>-<TOPIC>-001 | `fn area_topic_001_...()` | `tests/spec_<area>_<topic>.rs` | acceptance |
| <AREA>-<TOPIC>-001 | `fn test_...()` | `src/<module>.rs` | unit |

## 7. Open questions

## 8. Change log
| Date | Change |
|------|--------|
| YYYY-MM-DD | Created |
