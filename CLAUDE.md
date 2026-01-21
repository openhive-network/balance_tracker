# Balance Tracker (btracker)

Tracks Hive blockchain account balances including HIVE, HBD, VESTS, savings, delegations, rewards, and transfer statistics. Built on PostgreSQL/PostgREST with HAF (Hive Application Framework).

## Tech Stack
- **Database**: PostgreSQL 14+, PL/pgSQL
- **API**: PostgREST (REST from SQL)
- **Testing**: Tavern (API), pytest, JMeter
- **CI/CD**: GitLab CI, Docker

## Documentation Routing

| Task | Read |
|------|------|
| Architecture/general | `scripts/claude/main.md` |
| Processing functions | `scripts/claude/processing.md` |
| API endpoints | `scripts/claude/endpoints.md` |
| Tests | `scripts/claude/tests.md` |
| Debugging/CI failures | `scripts/claude/tools.md` |

## External Dependencies

**HAF (Hive Application Framework)**: HAF is installed separately (not a submodule). If you need HAF internals, ASK THE USER where HAF is cloned, then read `<haf_path>/scripts/claude/*.md`.

## Parent Application

This is a **submodule of HAFBE** (HAF Block Explorer). For HAFBE integration details, see the parent repo's `scripts/claude/` documentation.

## Specialized Agents

- **Developer agent** (`balance-tracker-dev`): For implementing features, fixing bugs, refactoring code
- **Reviewer agent** (`balance-tracker-reviewer`): For verifying code quality after changes
- **GitLab Pipeline Engineer** (`gitlab-pipeline-engineer`): For all git operations (commits, MRs, pipeline analysis)

## Expansion Rules

When modifying this project, update the appropriate documentation:
- Processing functions → `scripts/claude/processing.md`
- API endpoints → `scripts/claude/endpoints.md`
- Tests → `scripts/claude/tests.md`
- Utility scripts → `scripts/claude/tools.md`
- Architecture changes → `scripts/claude/main.md`
