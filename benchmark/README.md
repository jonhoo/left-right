To reproduce results, run with:

```shell
cargo build --release
for w in 1 2 4; do
  for d in uniform skewed; do
    for r in 1 2 4 8 16 32; do
      target/release/concurrent-map-bench -r $r -w $w -d $d;
    done;
  done;
done | tee results.log
R -q --no-readline --no-restore --no-save < plot.r
```
