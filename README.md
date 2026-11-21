# Shopping Sessions, native Linux engine

This C++17 engine reads the manifest, both lookup tables and every CSV shard in the three input directories. It writes Parquet user and campaign tables without runtime dependencies. The default entry point reads `/data`, writes `/output` and uses eight worker threads. It accepts `DATA_DIR`, `OUTPUT_DIR` and `NCPU`, or positional input and output paths.

The build uses a statically linked binary from GCC 13 and a scratch runtime. On a machine with the GCC image cached locally, build and run from this directory.

```sh
DOCKER_BUILDKIT=0 docker build --pull=false --network none -t builders-native-shopping-cpp:tuned .
docker run --rm --pull never --network none --read-only --cpus 8 --user 1000:1000 \
  -v "$DATA:/data:ro" -v "$OUT:/output" builders-native-shopping-cpp:tuned
```

`DATA` and `OUT` are absolute paths. Create an empty output directory writable by UID 1000. Each output table includes a valid typed Parquet file when it has zero rows.

The engine canonicalizes linked IDs with union–find and partitions events by complete event ID before earliest-arrival deduplication. It repartitions retained events and clicks by canonical person. Sorted per-person scans create sessions, maintain the seven-day click window and aggregate purchases on each session’s start day. Each user partition writes its Parquet output independently. The final reduction merges campaign totals across partitions.

The engine retains event and click partitions in memory. Peak usage depends on record count and skew, including the largest linked person. Local Docker timings depend on how the input and output volumes are mounted and cached.

This variant parses link edges in parallel before joining their components. It counts repeated event-ID fingerprints in each in-memory partition. An ID with a unique fingerprint has only one row. Every row in a repeated fingerprint bucket goes through exact full-ID and arrival comparison, including collisions between distinct IDs. The method reads each event shard once.
