# GitHub Actions Service Port Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the three backend certification workflows valid GitHub Actions definitions that connect to their PostgreSQL and Neo4j service containers.

**Architecture:** GitHub-hosted jobs execute directly on the runner host. Service containers therefore publish fixed loopback ports, and every test environment variable addresses those fixed ports. This removes the unsupported `job.services` expression context before GitHub validates the workflow.

**Tech Stack:** GitHub Actions YAML, PostgreSQL 17, Neo4j 5.26, Rust cargo test.

## Global Constraints

- Retain the existing PostgreSQL 17 and Neo4j 5.26 service images and health checks.
- Use only loopback addresses and the fixed ports `5432`, `7474`, and `7687`.
- Do not change the test commands or broaden CI scope.

---

### Task 1: Publish Fixed Service Ports and Remove Invalid Expressions

**Files:**
- Modify: `.github/workflows/postgres-mapping.yml:19-26`
- Modify: `.github/workflows/neo4j-mapping.yml:17-27`
- Modify: `.github/workflows/three-backend-migration.yml:19-41,77-99`
- Test: `.github/workflows/*.yml` static expression scan

**Interfaces:**
- Consumes: GitHub Actions service-container port syntax (`HOST:CONTAINER`).
- Produces: `DTGPROXY_POSTGRES_URL` using `127.0.0.1:5432` and `DTGPROXY_NEO4J_ENDPOINT` using `http://127.0.0.1:7474`.

- [x] **Step 1: Write the failing static regression check**

Run:

```bash
rg -n 'job\\.services' .github/workflows
```

Expected: the command reports every unsupported expression in all three workflow files.

- [x] **Step 2: Run the check to verify the failure**

Expected: non-empty output containing `job.services.postgres.ports[5432]` and `job.services.neo4j.ports[7474]`.

- [x] **Step 3: Write the minimal workflow configuration**

Replace dynamic service-port entries:

```yaml
ports:
  - 5432/tcp
```

with:

```yaml
ports:
  - 5432:5432
```

Replace dynamic URLs:

```yaml
DTGPROXY_POSTGRES_URL: host=127.0.0.1 port=${{ job.services.postgres.ports[5432] }} user=dtgproxy password=dtgproxy-ci-password dbname=dtgproxy sslmode=disable
DTGPROXY_NEO4J_ENDPOINT: http://127.0.0.1:${{ job.services.neo4j.ports[7474] }}
```

with:

```yaml
DTGPROXY_POSTGRES_URL: host=127.0.0.1 port=5432 user=dtgproxy password=dtgproxy-ci-password dbname=dtgproxy sslmode=disable
DTGPROXY_NEO4J_ENDPOINT: http://127.0.0.1:7474
```

Use `7474:7474` and `7687:7687` for Neo4j.

- [x] **Step 4: Run the checks to verify the configuration**

Run:

```bash
! rg -n 'job\\.services' .github/workflows
rg -n '5432:5432|7474:7474|7687:7687|port=5432|127.0.0.1:7474' .github/workflows
git diff --check
```

Expected: no invalid expression is found; each affected workflow contains fixed mappings and URLs; `git diff --check` exits 0.

- [ ] **Step 5: Commit and push**

```bash
git add .github/workflows docs/superpowers/plans/2026-07-23-github-actions-service-port-fix.md
git commit -m "fix: correct GitHub Actions service ports"
git push origin feature/dtgproxy
```

Expected: GitHub starts one run each for PostgreSQL Mapping, Neo4j Mapping, and Three Backend Migration.
