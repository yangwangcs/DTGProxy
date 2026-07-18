# SharedNothing example

```bash
cargo run -p dtgproxy -- init \
  --config examples/shared-nothing/node.json \
  --root examples/shared-nothing/data \
  --graph-id 2 --graph-name shared-example \
  --mode shared-nothing \
  --shards 10:1:10,20:1:20 \
  --route-seed 99 --virtual-partitions 128 \
  --backend rocksdb --listen 127.0.0.1:7080

cargo run -p dtgproxy -- serve --config examples/shared-nothing/node.json
```

In another terminal, commit two vertices plus a cross-partition edge and then run a global query:

```bash
cargo run -p dtgproxy -- transaction \
  --config examples/shared-nothing/node.json \
  --file examples/shared-nothing/cross-partition-transaction.json

cargo run -p dtgproxy -- query \
  --config examples/shared-nothing/node.json \
  --text 'SCAN VERTICES GRAPH 2 FOR VALID TIME 1 CURRENT LIMIT 100'
```
