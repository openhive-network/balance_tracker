# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Balance Tracker is a HAF (Hive Application Framework) application that tracks HBD and Hive balances for Hive blockchain accounts. It demonstrates how to build HAF apps and provides APIs for querying balance history by block or time.

## Architecture

The application uses a three-tier architecture:
- **Database Layer**: PostgreSQL with HAF extensions. All business logic is in SQL stored procedures.
- **API Layer**: PostgREST exposes SQL functions as REST endpoints. Alternatively, a Python backend can be used.
- **Frontend**: React-based web UI (in `gui/`)

Key database schemas:
- `btracker_app`: Core application tables and processing functions
- `btracker_endpoints`: PostgREST-exposed API functions
- `btracker_backend`: Backend helper functions

### Directory Structure

- `db/` - Core SQL: table definitions (`btracker_app.sql`), block processing functions (`process_*.sql`)
- `backend/` - SQL backend helpers: endpoint implementations, utilities, balance history queries
- `endpoints/` - PostgREST API definitions: endpoint SQL and type definitions
- `scripts/` - Shell scripts for installation and CI
- `tests/tavern/` - API integration tests using Tavern framework
- `tests/performance/` - JMeter performance tests

### Database Roles

- `btracker_owner`: Schema owner, runs migrations and block processing
- `btracker_user`: Read-only user for API queries (PostgREST connects as this role)

## Common Commands

All scripts are located in the `scripts/` directory.

### Installation and Setup
```bash
./scripts/install_app.sh    # Set up database schema
./scripts/uninstall_app.sh  # Remove database schema
```

### Block Processing
```bash
./scripts/process_blocks.sh  # Process blocks from HAF
```

### Testing
```bash
# Run Tavern API tests
cd tests/tavern
pytest
```

### Docker
```bash
cd docker
docker compose up -d                    # Start full stack
docker compose --profile swagger up -d  # Include Swagger UI
docker compose down -v                  # Stop and remove data
```

### Linting
SQL files are linted with sqlfluff (PostgreSQL dialect, max line length 150).

## Specialized Agents

This project uses specialized Claude agents via the Task tool. Agent names follow patterns based on their purpose:

- **Developer agent** (`{project-name}-dev`): For implementing features, fixing bugs, refactoring code, and performing coding tasks. Derive the project name from the application (e.g., Balance Tracker → `balance-tracker-dev`).
- **Reviewer agent** (`{project-name}-reviewer`): For verifying code quality, ensuring existing functionality remains intact, and checking for regressions. Use after code changes.
- **GitLab Pipeline Engineer** (`gitlab-pipeline-engineer`): For all git operations including committing, creating/updating MRs, and pipeline analysis. **Always use this agent instead of running glab/git commands directly.**

**Recommended workflow**:
1. Use the dev agent to write code
2. Use the reviewer agent to identify issues
3. Use the dev agent again to fix any issues found
4. Use `gitlab-pipeline-engineer` to commit and create/update MRs

## Development Notes

- HAF submodule is optional for running the app but required for CI tests
- Database connection defaults: localhost:5432, database `haf_block_log`
- PostgREST configuration is in `postgrest.conf`
- Block processing runs as `btracker_owner`, API queries as `btracker_user`
