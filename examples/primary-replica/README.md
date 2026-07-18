# PrimaryReplica example

From the repository root:

```bash
cargo run -p dtgproxy -- init \
  --config examples/primary-replica/node.json \
  --root examples/primary-replica/data \
  --graph-id 1 --graph-name primary-example \
  --mode primary-replica --shards 10:1:10 \
  --backend rocksdb --listen 127.0.0.1:7070

cargo run -p dtgproxy -- serve --config examples/primary-replica/node.json
```

In another terminal:

```bash
cargo run -p dtgproxy -- transaction \
  --config examples/primary-replica/node.json \
  --file examples/primary-replica/transaction.json

cargo run -p dtgproxy -- query \
  --config examples/primary-replica/node.json \
  --text 'VERTEX 1 GRAPH 1 PARTITION 0 FOR VALID TIME 1 CURRENT LIMIT 1'
```
