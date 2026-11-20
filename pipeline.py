#!/usr/bin/env python3
"""Normalize the contest CSV dialect and run the DuckDB transformation."""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
from tempfile import TemporaryDirectory
from uuid import uuid4

from identity import build_identity


HERE = Path(__file__).resolve().parent
SCHEMAS = {
    "events": ("event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"),
    "clicks": ("click_id", "user_id", "campaign_id", "ts"),
    "links": ("user_id_a", "user_id_b"),
    "products": ("product_id", "category"),
    "campaigns": ("campaign_id", "channel"),
}
FIELD = rb'(?:[^,"\n]*|"[^,"\n]*")'
ESCAPE = b"\x1d"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def normalize(sources, destination, columns):
    """Remove malformed records while preserving valid bytes and null fields."""
    separator_count = len(columns) - 1
    valid_quoted = re.compile(FIELD + (b"," + FIELD) * separator_count)
    rows = 0
    escaped = False
    with destination.open("wb", buffering=1024 * 1024) as target:
        target.write(",".join(columns).encode("ascii") + b"\n")
        for path in sources:
            with path.open("rb", buffering=1024 * 1024) as source:
                next(source, None)  # Each shard has a header, even if it has no records.
                for raw in source:
                    line = raw.removesuffix(b"\n")
                    if line.count(b",") != separator_count:
                        continue
                    if b'"' in line:
                        if not valid_quoted.fullmatch(line):
                            continue
                        line = line.replace(b'"', b"")
                    if b"\r" in line or ESCAPE in line:
                        # Distinguish literal CR from the reader's possible CRLF handling.
                        line = line.replace(ESCAPE, ESCAPE + b"a").replace(b"\r", ESCAPE + b"b")
                        escaped = True
                    target.write(line + b"\n")
                    rows += 1
    return rows, escaped


def load_raw(con, data, scratch):
    for name, columns in SCHEMAS.items():
        if name in ("products", "campaigns"):
            sources = [data / f"{name}.csv"]
        else:
            folder = data / name
            sources = sorted(folder.glob("*.csv"))
        staged = scratch / f"{name}.csv"
        rows, escaped = normalize(sources, staged, columns)
        if rows:
            types = ", ".join(f"'{column}': 'VARCHAR'" for column in columns)
            projection = "*"
            if escaped:
                projection = ", ".join(
                    f"replace(replace({column}, chr(29) || 'b', chr(13)), chr(29) || 'a', chr(29)) AS {column}"
                    for column in columns
                )
            con.execute(f"""
                CREATE TEMP VIEW raw_{name} AS
                SELECT {projection} FROM read_csv(
                    {sql_string(staged.as_posix())}, columns = {{{types}}},
                    header = true, auto_detect = false, delim = ',',
                    quote = '"', escape = '"', new_line = '\\n', nullstr = ''
                )
            """)
        else:
            fields = ", ".join(f"NULL::VARCHAR AS {column}" for column in columns)
            con.execute(f"CREATE TEMP VIEW raw_{name} AS SELECT {fields} WHERE false")


def manifest_ms(value):
    delta = datetime.fromisoformat(value.replace("Z", "+00:00")) - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000


@contextmanager
def scratch_space(scratch):
    scratch.mkdir(parents=True, exist_ok=True)
    scratch = scratch.resolve()
    if os.name == "nt":
        # Inherit the explicitly selected scratch directory's Windows ACL.
        staging = scratch / f"shopping-{uuid4().hex}"
        staging.mkdir()
        try:
            yield staging
        finally:
            if staging.resolve().parent == scratch and not staging.is_symlink():
                shutil.rmtree(staging)
    else:
        with TemporaryDirectory(prefix="shopping-", dir=scratch) as temp_name:
            yield Path(temp_name)


def run(data, output, scratch):
    import duckdb

    cpus = int(os.getenv("NCPU", "2"))
    memory_gb = int(os.getenv("MEM_GB", "4"))
    with scratch_space(scratch) as staging:
        with duckdb.connect() as con:
            con.execute(f"SET threads = {max(1, cpus)}")
            con.execute(f"SET memory_limit = '{max(1, int(memory_gb * 0.8))}GB'")
            con.execute(f"SET temp_directory = {sql_string((staging / 'duckdb_tmp').as_posix())}")
            con.execute("SET preserve_insertion_order = false")
            manifest = json.loads((data / "manifest.json").read_text(encoding="utf-8"))
            con.execute("""
                CREATE TEMP TABLE input_window AS
                SELECT ?::BIGINT AS window_start_ms, ?::BIGINT AS window_end_ms
            """, [manifest_ms(manifest["window_start"]), manifest_ms(manifest["window_end"])])
            load_raw(con, data, staging)
            build_identity(con, staging)
            for stage in ("clean.sql", "session.sql", "output.sql"):
                con.execute((HERE / stage).read_text(encoding="utf-8"))
            for table in ("user_daily", "campaign_daily"):
                folder = output / table
                folder.mkdir(parents=True, exist_ok=True)
                path = (folder / "part-0.parquet").resolve().as_posix()
                con.execute(f"COPY {table} TO {sql_string(path)} (FORMAT PARQUET)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_positional", nargs="?", type=Path)
    parser.add_argument("output_positional", nargs="?", type=Path)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--scratch", type=Path)
    args = parser.parse_args()
    data = args.input or args.data_positional or Path(os.getenv("DATA_DIR", "/data"))
    output = args.output or args.output_positional or Path(os.getenv("OUTPUT_DIR", "/output"))
    scratch = args.scratch or Path(os.getenv("SCRATCH_DIR", str(output / "_scratch")))
    run(data, output, scratch)


if __name__ == "__main__":
    main()
