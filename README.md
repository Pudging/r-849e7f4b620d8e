# Team potato — Shopping Sessions

This entry runs DuckDB 1.5.6 on the contest CSV shards. It names each linked group by its smallest user ID, creates sessions from event-time gaps, and computes last-click credit and distinct campaign assists. Both daily aggregates are written as Parquet.

Build the image from this directory.

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
```

Use the official DuckDB starter's `scripts/docker_run.sh` to run the image with scorer flags. Set `CPUS` and `MEM` to the capacity of the test machine. The container reads the input at `/data` and requires writable `/output` and `/scratch` mounts for UID 1000.

```sh
CPUS=8 MEM=8g "$STARTER/scripts/docker_run.sh" "$DATA" "$OUTPUT"
python "$STARTER/scripts/compare.py" "$OUTPUT" "$EXPECTED"
```

The official starter can also compare all public fixtures directly against the Python entry point. Install the pinned runtime and the starter's comparison dependency first.

```sh
python -m pip install -r requirements.txt polars
python "$STARTER/scripts/run_fixtures.py" "$STARTER/fixtures" -- python pipeline.py
```
