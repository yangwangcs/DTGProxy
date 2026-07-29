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
host independent Fjall, PostgreSQL, Neo4j, and Remote Shards at the same time. Consensus and logical
state use separate namespaces, and every replica identity is fenced by cluster, graph, Shard,
placement epoch, replica, and backend generation.

Use mutual TLS on non-loopback networks. Plaintext listeners are accepted only on loopback by the
process configuration validators.
