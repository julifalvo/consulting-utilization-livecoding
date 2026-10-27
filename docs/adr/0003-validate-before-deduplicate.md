# ADR 0003: Validate before deduplicating in silver

**Status:** accepted

## Context

An entry can arrive several times: re-sent, corrected, deleted. Some versions are invalid
(e.g. a correction typed as 80 hours). If we keep only the latest version and *then*
validate, an invalid correction hides the previous valid one, and the outcome depends on
whether files are processed week by week or replayed all at once.

## Decision

Validate every version first, quarantine the invalid ones with a reason, then keep the
latest *valid* version per `entry_id`. The MERGE into silver only applies strictly newer
versions.

## Consequences

* Full replay and incremental runs converge to the same silver (tested end to end).
* An invalid correction leaves the last good value in place and shows up in quarantine,
  where someone can chase it. The trade-off: until it is fixed, silver shows the older value.
