# Deployment

Use the descriptors in `config/examples/clean-break-cluster/` as the topology source of truth.
Production deployments should replace loopback listeners, local directories, endpoints, and
credential references while preserving cluster identity and namespace isolation.

The four executable names are:

```text
dtgproxy-meta
dtgproxy-controller
dtgproxy-data
dtgproxy-gateway
```

Meta and Controller accept `--config PATH`. Data and Gateway read their documented `DTG_DATA_*` and
`DTG_GATEWAY_*` environment variables. The certification script translates the example topology
into isolated runtime directories and verifies that all four roles can run concurrently.

Data placement is per replica binding. There is no node-global backend switch: a Data process may
host independent Fjall, PostgreSQL, Kuzu, and Remote Shards at the same time. Consensus and logical
state use separate namespaces, and every replica identity is fenced by cluster, graph, Shard,
placement epoch, replica, and backend generation.

Use mutual TLS on non-loopback networks. Plaintext listeners are accepted only on loopback by the
process configuration validators.

## Same-host Gateway/Data transport

TCP remains the default and is required between hosts. When Gateway and its assigned Data process
run under the same operating-system user on one Unix host, set `DTG_DATA_GATEWAY_UNIX_SOCKET` to an
absolute socket path and set the matching `DTG_GATEWAY_CLUSTER_ENDPOINT` (or shard endpoint) to
`unix:///absolute/socket/path`. Data continues to serve TCP for Raft, probes, and remote Gateway
routes. The socket is owner-only (`0600`); Data removes only a stale socket at that exact path and
refuses to overwrite any ordinary file.

## Local cluster

For development and integration verification, run:

```bash
scripts/local-cluster.sh start --managed-postgres
scripts/local-cluster.sh status
scripts/local-cluster.sh stop
```

The launcher creates a private directory below `target/`, uses a random PostgreSQL password, and
binds PostgreSQL to `127.0.0.1` only. It starts one Data process per official provider and routes
read fragments to those three Data endpoints by their fenced Shard assignment. It owns only the
processes whose PIDs it records, and the `stop` command refuses to signal a process whose executable
does not match its record. Fjall and Kuzu remain embedded in each Data process; only PostgreSQL is
launched as an external service. Process writes currently require one active catalog Shard, so this
three-Shard launcher is a distributed read and provider-isolation environment rather than a
multi-Shard write certification.
Production deployments must manage PostgreSQL and the DTG processes with the platform supervisor
(for example systemd, launchd, or Kubernetes), rather than this development launcher.
