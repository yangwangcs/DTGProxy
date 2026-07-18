# DTGProxy local two-Data-node cluster

These version-1 JSON files describe a loopback-only development cluster with one Meta node, two
Data nodes, one Gateway, and one reconciliation Controller. Replace every data directory before
starting a second copy. Start Meta, both Data nodes, Gateway, then Controller.

The Data service and private Raft transport deliberately use distinct ports. Plaintext is accepted
only on loopback; production deployment must supply the mTLS hardening listed in the boundary audit.

The files are configuration examples, not bootstrap commands. Graph definitions and initial
Replica placement are committed through Meta's versioned `Propose` API.
