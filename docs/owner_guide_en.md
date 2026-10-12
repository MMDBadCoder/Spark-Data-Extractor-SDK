# Service owner and developer guide

This guide describes how to qualify, configure, install, operate, and support the Data Extractor SDK. It covers the approved `Query.show_columns()` discovery call and the existing bounded export workflow. The repository is a prototype until the live environment has passed the deployment gates below. No Spark endpoint, Hadoop topology, authentication method, or customer schema is inferred here.

## What the SDK does

The customer notebook talks to an existing Spark Connect service. The SDK constructs administrator-owned HDFS paths for a fixed daily Parquet dataset, reads the source schema for discovery, and runs native Spark Column predicates, projection, and a required row cap for export. Each run creates a fresh Connect session. Completed results are gzip CSV under a Tehran execution-date directory. A WebHDFS downloader on a separate Linux machine requires the completion manifest and checks listed file sizes.

`show_columns()` is a discovery-only call. After `.between(start, end)` and before any filter or projection, it reads the configured source schema and prints a Persian name/type table, then returns an immutable tuple of `ColumnInfo(name, data_type, nullable)` records. It requires a valid date range because it checks the corresponding fixed source partition(s). It does not require `.limit()` or `.output_files()`. It does not export, count rows, apply filters, project columns, choose an output destination, or create a manifest. Use the same query builder after discovery to add filters/projection and then supply the required `limit` and `output_files` before `run()`.

```python
from data_extractor import Query
from pyspark.sql.functions import col

query = Query("customer_search")
query.between("2026-01-01", "2026-01-07")
metadata = query.show_columns()  # inspect the Persian table first

query.where(col("status") == "ready")
query.select("customer_id", "status")
query.limit(100_000)
query.output_files(4)
query.run()
```

Metadata is descriptive. It is not an authorization boundary or a promise that the schema will remain stable between discovery and execution. The fixed source contract and live deployment must keep the schema stable. The metadata type value is Spark's type string (for example `string`, `bigint`, `decimal(12,2)`, `array<string>`, `map<string,int>`, or `struct<...>`), not a Python class name. Decimal precision/scale and nested field types matter when choosing predicates. CSV is a flat text format: date/time and decimal formatting, null markers, and complex array/map/struct cell representations require checks against the deployed Spark version and consumer. Do not describe a CSV export as a typed or lossless archive; test representative real values, especially nested values and literal `\\N` collisions.

## Deployment prerequisites and client qualification

The assumed environment is Python 3.10 or newer, an existing Spark 4.x Connect service, an HDFS-backed fixed dataset with daily partitions, an accessible WebHDFS service, and a Linux download host. The Jupyter and download hosts must resolve and reach their respective configured services. Spark cluster resource limits and scheduling belong to the cluster administrator; the SDK does not allocate executors, guarantee a Spark application per query, or cap source scan cost.

The package provisionally pins `pyspark[connect]==4.0.1`. This is a candidate client version, not a compatibility certification. Confirm the deployed Spark server minor, Python version, operating system, Java/runtime requirements imposed by the environment, and supported Python client. Resolve and qualify the complete dependency set (including gRPC/protobuf and any optional Connect dependencies) in the target environment. Build a lock or wheelhouse from the qualified set. Run the live connection, metadata discovery, native filter, output, timeout, cleanup, and downloader smoke gates before customer access. Apache describes Spark Connect as a decoupled client/server architecture; the protocol boundary does not remove the need to qualify the exact client/server pair. See the [Spark 4.0.1 Connect overview](https://spark.apache.org/docs/4.0.1/spark-connect-overview.html) and [4.0.1 installation guide](https://spark.apache.org/docs/4.0.1/api/python/getting_started/install.html).

## Network reachability and supported authentication boundary

From the notebook host, qualify DNS, routing, firewall rules, and gRPC reachability to the configured bare `sc://host:port`. The current config accepts no URL path, query, or user information, and the session uses `SparkSession.builder.remote(...)`; it does not expose a custom channel builder for Spark Connect. If deployment requires authenticated/TLS Connect channel setup or URL parameters, implement and qualify an adapter before use. The service must permit the client to create Connect sessions and read the configured HDFS paths through the server-side Hadoop identity. A fresh SDK session is a client session lifecycle choice; it does not guarantee a separate application or executor reservation.

From the Linux download host, qualify the configured WebHDFS base URL and every redirect target. WebHDFS file operations can redirect from the NameNode to a DataNode; both hosts and ports must be reachable, and the redirect's TLS certificate must be trusted if HTTPS is used. Test list/open operations and error responses in the real network. The current downloader supports simple `user.name` identity and delegation-token query authentication as implemented by the client. Kerberos/SPNEGO, proxy-user flows, OAuth refresh, and organization-specific gateways are outside the supported boundary until an adapter is implemented and qualified. Apache's [WebHDFS REST documentation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) documents redirect and authentication behavior; deployments should use docs matching their Hadoop release.

No endpoint, certificate, principal, or auth scheme is supplied as a live value in this repository. Place environment-specific values only in protected configuration and validate them with the service owners.

## Protected configuration and customer boundary

The package defaults are intentionally non-live placeholders. Set `DATA_EXTRACTOR_CONFIG` for the notebook process to an administrator-managed YAML file. The notebook process must be able to read it; the customer account must not be able to modify it. A common Unix arrangement is an administrator-owned file with mode `0640`, owned by a dedicated admin account and readable by a service group that includes the notebook runtime account. Protect parent directories too. Choose ownership and groups according to local policy; do not make the file unreadable to the Jupyter process or writable by the customer.

The schema is strict: unknown top-level and nested keys are rejected. Supported sections and keys are:

| Section | Keys | Purpose |
|---|---|---|
| `spark` | `connect_url`, `session_prefix`, `connect_timeout_seconds`, `execution_timeout_seconds` | Connect endpoint, session name prefix, acquisition/readiness and execution deadlines |
| `hdfs` | `uri`, `input_base`, `output_base` | Fixed source and result roots |
| `query` | `max_days`, `max_limit`, `max_output_files`, `max_planned_rows_per_file` | Query policy ceilings |
| `output` | `max_records_per_file`, `timezone`, `target_compressed_mb` | File row cap, fixed `Asia/Tehran` timezone, optional advisory size objective |
| `webhdfs` | `base_url`, `timeout_seconds`, `user`, `token` | Download-side endpoint and supported simple credentials |
| `logging` | `file` | SDK log destination |
| `runtime` | `lock_file` | SDK cross-process lock destination |

An illustrative file is in [`examples/admin.yaml`](../examples/admin.yaml); its `.example.invalid` addresses are placeholders. Keep secrets out of notebook source, source control, logs, and command history. The downloader uses separate `DATA_EXTRACTOR_DOWNLOAD_CONFIG` configuration, so download credentials and routes need not be exposed to the notebook runtime.

The SDK protects its documented interface, not an isolated Python process against a trusted notebook user who can execute arbitrary Python. Enforce account isolation, filesystem permissions, network policy, and access to the notebook environment outside the SDK.

## Notebook kernel and fresh Connect environment

Install the qualified client into the exact Python environment used by the notebook kernel. Register/select a dedicated kernel for that environment and preconfigure its environment variables and protected config access. The kernel should start without an already active Spark Classic session. Import the SDK before constructing `pyspark.sql.functions.col` expressions; the SDK enables Connect mode for the client process. Do not mix the SDK with a Classic Spark session or switch session modes in one kernel. Jupyter kernel selection and kernelspec environment behavior are documented in the [Jupyter Server kernel docs](https://jupyter-server.readthedocs.io/en/latest/operators/kernels.html); verify the actual deployment's launcher and service manager.

The expected notebook flow is: create `Query`, call `.between(...)`, run `.show_columns()`, inspect the Persian table, then add `.where(...)` / `.select(...)`, required `.limit(...)` / `.output_files(...)`, and `.run()`. Discovery makes a fresh Connect session and closes it under the SDK's execution lock and deadline handling. Each `run()` also creates a fresh session and uses that same exclusive SDK lock. The kernel remains occupied while a synchronous cell runs.

For a dedicated environment, use the exact qualified Python executable, install/register a kernel from that executable, and verify that the selected kernel reports the same executable and package versions:

```bash
python3.10 -m venv /opt/data-extractor-0.2.0
/opt/data-extractor-0.2.0/bin/python -m pip install --upgrade pip
/opt/data-extractor-0.2.0/bin/python -m pip install 'data-extractor[connect]==0.2.0' ipykernel
/opt/data-extractor-0.2.0/bin/python -m ipykernel install --prefix=/opt/jupyter --name=data-extractor --display-name='Data Extractor (Connect)'
/opt/data-extractor-0.2.0/bin/python --version
/opt/data-extractor-0.2.0/bin/python -m pip show data-extractor pyspark
/opt/data-extractor-0.2.0/bin/python -m pip check
```

Set `DATA_EXTRACTOR_CONFIG` in the Jupyter service/kernel environment to the protected administrator file, then start a fresh kernel. In a notebook cell, confirm `sys.executable`, `sys.version`, and `pyspark.__version__` match the qualified environment before running the customer notebook. Import `data_extractor` before importing/constructing Spark `Column` expressions. Never validate using a kernel that already has a Classic session.

## Build, install, offline dependencies, and upgrades

Build from a clean checkout with a qualified build environment:

```bash
python -m pip install -e '.[connect,dev]'
python -m build
python -m pip install 'dist/data_extractor-0.2.0-py3-none-any.whl[connect]'
python -m pip check
python -m unittest discover -s tests -v
```

The wheel must include `src/data_extractor/resources/*.yaml`. To prepare an offline installation, download the built wheel and the complete qualified dependency closure on a connected machine with the same platform/Python constraints, then transfer and install from a wheelhouse:

```bash
python -m pip download --dest wheelhouse 'dist/data_extractor-0.2.0-py3-none-any.whl[connect]'
python -m pip install --no-index --find-links wheelhouse 'data-extractor[connect]==0.2.0'
python -m pip check
```

For an upgrade, stage a new environment or wheelhouse, run local and live gates, then update the dedicated kernel to the qualified environment during a maintenance window. Keep a known-good wheelhouse and config backup for rollback. Do not change the PySpark pin just to make dependency resolution succeed; qualify the replacement against the actual Spark service first.

## Output, count, and manifest behavior

Export destinations follow `<output_base>/result/yyyy/MM/dd/<query_name>/`, where `yyyy/MM/dd` is captured in `Asia/Tehran` near submission. Existing destinations are rejected. Spark writes `part-*.csv.gz` with UTF-8 CSV, headers, quote-all behavior, double-quote quoting/escaping, and `\N` as the null marker. The requested output file count is a target partition count. Empty partitions and the configured `maxRecordsPerFile` row cap can change the physical number of files; report the measured count. `target_compressed_mb` is currently stored and validated but is not used for sampling, repartitioning, or output tuning. Administrators must measure representative data and choose `output_files` and row caps themselves; no compressed-byte ceiling is promised. Query validation also checks `ceil(limit / output_files) <= max_planned_rows_per_file`. A configured `max_limit` by itself does not ensure this condition: the chosen file count must be high enough for the requested limit and administrator's planned rows-per-file ceiling.

After the bounded export completes, the SDK rereads the output using an explicit all-string schema with multiline parsing and FAILFAST behavior to measure the exact exported row count. This adds an output scan. A row cap does not limit how much source data Spark scans. CSV is not a lossless type archive, and literal `\N` values can be ambiguous with nulls; test representative real data before release.

The COMPLETE manifest contains run identity, creation time, measured row count, and listed files with sizes. It is published only after successful worker completion, cleanup attempt, verification, and output inspection. Failed or timed-out runs are not marked complete, though partial directories or files may remain. The downloader refuses missing/incomplete manifests, validates safe file names and sizes, uses temporary files and atomic local renames, and does not overwrite local files unless explicitly told to.

## Locations, permissions, logs, and locks

The admin configuration chooses the source/output HDFS roots, local log file, and lock file. Defaults place logs and locks under `~/.cache/data_extractor/` for the process account, while the example config shows `/tmp` placeholders. The logger creates the parent directory if needed and writes UTF-8 JSON Lines: one object per record with local timestamp, level, message, and a traceback field for exceptions. It creates a new log file with mode `0600` subject to the process umask; if the file already exists, its existing mode is retained. The handler appends. The SDK does not rotate logs. The lock file is also created with mode `0600` subject to umask and remains as a lock inode after the descriptor closes; flock state is released on close. In a service deployment, set both paths to stable, administrator-controlled locations on a filesystem visible to the notebook process. The runtime account needs directory create/traverse rights, log append rights, and lock create/read/write rights. Keep configuration administrator-owned and read-only to the customer (for example `0640` with a service group that includes the notebook runtime); keep logs restricted to operations staff, especially because tracebacks can expose internal exception details. Use the host's normal log rotation and retention controls. The SDK does not create an automatic output-retention policy.

The process lock and Linux file lock allow one SDK-managed operation at a time. A timed-out worker that is still alive retains the lock, preventing overlapping jobs. This lock is local to the configured shared filesystem and is not a cluster-wide scheduler or a control over jobs launched outside this SDK.

## Monitoring and diagnosis

Use the Persian notebook progress and completion messages for customer-visible status, then correlate the execution ID and timestamp with the configured SDK log, Jupyter kernel logs, Spark Connect server logs/UI, and Hadoop/WebHDFS NameNode/DataNode logs. Access to cluster logs and UIs is an operations responsibility, not an SDK-provided endpoint. Compare the failing stage with the timeline: Connect creation/readiness, partition existence checks, Parquet schema/read, filter/projection/limit, CSV write, output reread/count, WebHDFS output inspection, or manifest publication.

Check source day presence and permissions, schema/type changes, client/server compatibility, Connect/gRPC network health, Spark executor capacity, destination permissions/quota, redirect target reachability, auth expiration, and output directory collisions. Tell customers to share the Persian error code and time of occurrence, not configuration values or tokens. The notebook runtime may display chained Python exception details even when the SDK prints a concise Persian message; customer-facing materials should explain safe error reporting, while operations staff collect full diagnosis from protected logs. Keep the execution ID when escalating.

## Deadlines, late work, partial output, and retention

Connect acquisition/readiness and execution use separate configured deadlines. The execution deadline covers Spark work, output verification, and session cleanup attempt. Final manifest publication is after worker success and uses bounded WebHDFS per-request timeouts; it can extend total call time, so the entire `run()` does not have a strict wall-clock bound. Python cannot force-kill a blocked daemon thread. The SDK attempts cancellation and session stop but cannot prove remote termination. A delayed worker must not publish COMPLETE after its caller timed out.

After a timeout, the worker may continue remotely, keep the local lock, and leave partial files. Do not retry repeatedly or remove the lock while work may still be running. Inspect server-side work and logs first; coordinate a kernel restart only as a client cleanup step, since restarting does not prove remote cancellation. Once the operation is confirmed inactive, an administrator can inspect and clean partial output and resolve a stale lock according to local filesystem/process state. Retention and cleanup are manual. These caveats also apply to discovery schema calls: they use a fresh Connect session and the same lock, deadlines, cancellation attempts, and cleanup guarantees, without producing export output.

## Release gates and evidence

Before release, run unit tests and packaging checks, inspect wheel contents, then complete the live checklist in [`docs/SMOKE_TEST.md`](SMOKE_TEST.md) with the actual deployment. The release gate must include a qualified client/server version, fresh kernel/Connect operation, discovery table and immutable metadata, representative schema types, native filter and projection, valid/missing date partitions, bounded export, measured output count, manifest timing, WebHDFS redirects and auth, downloader size checks, timeout/late-worker lock behavior, and cleanup logs. Record actual environment and outcomes in `docs/STATUS.md`; mocked tests do not establish cluster validation.

Never claim a certification, supported endpoint, or successful live qualification unless the target environment was exercised and the evidence recorded. Keep the approved PRD unchanged; update assessment/design and operational status when implementation or deployment behavior changes.

## Primary references

- [Spark 4.0.1 Connect overview](https://spark.apache.org/docs/4.0.1/spark-connect-overview.html)
- [Spark 4.0.1 installation](https://spark.apache.org/docs/4.0.1/api/python/getting_started/install.html)
- [Spark 4.0.1 configuration](https://spark.apache.org/docs/4.0.1/configuration.html)
- [Hadoop WebHDFS REST API](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)
- [Jupyter Server kernels](https://jupyter-server.readthedocs.io/en/latest/operators/kernels.html)
