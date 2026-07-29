# Clean-break examples

Current executable topology examples live in `config/examples/clean-break-cluster/`. They show the
four process roles, two independent Data nodes, per-node business and consensus roots, and the
approved provider-class set.

Run `scripts/certify-clean-break.sh --local` to materialize an isolated instance of that topology
and produce a digested evidence manifest.
