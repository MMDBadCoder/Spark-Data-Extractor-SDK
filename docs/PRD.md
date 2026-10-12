# Product Requirements Document
# Spark Connect Data Extraction SDK

**Version:** 1.0 | **Status:** Approved MVP specification | **Audience:** Engineering

## 1. Product summary
Build a small installable Python SDK used from a preconfigured Jupyter Notebook by one trusted, nontechnical customer. The SDK reads daily HDFS Parquet files, applies arbitrary Spark Connect-compatible Column predicates, selects columns, limits rows, and writes gzip CSV files. A standalone Linux downloader fetches completed outputs through an existing reachable WebHDFS service. No new backend, scheduler, UI, or Spark Connect deployment is in scope.

## 2. Environment and assumptions
- Existing Spark 4.x cluster and Spark Connect host:port, reachable from Jupyter; no YARN dependency assumed.
- One fixed source dataset and stable schema; HDFS URI such as `hdfs://nn-cluster` is configured by the package administrator.
- Input paths: `<input_base>/yyyy/MM/dd/*.parquet` (inclusive date range, at most seven calendar dates).
- One trusted customer in an isolated environment, one SDK-managed job at a time.
- Typical source volume: 100 GB–1 TB per day. Spark cluster resource limits are preconfigured outside this SDK.
- Existing WebHDFS endpoint is accessible from the download machine, including any required redirects.

## 3. Goals and exclusions
Goals: minimal Notebook usage; flexible native `pyspark.sql.functions.col` filters; extensive Persian messages; no user-entered HDFS paths; bounded output records and sensible file sizing; fresh session per run; deterministic Tehran-date output paths; easy Linux download; simple wheel installation.

Excluded: arbitrary transformations outside filters/projection, multiuser support, hard isolation against malicious Python code, separate API server, asynchronous job manager, dashboard, automatic retention, deployment/configuration of Spark Connect or Hadoop.

## 4. Customer API
```python
from pyspark.sql.functions import col
from data_extractor import Query

(
    Query("customer_search")
    .between("2026-01-01", "2026-01-07")
    .where((col("a") > 10) & (col("b").startswith("asdasa") | (col("b") < 10)))
    .select("a", "b", "c")
    .limit(1_000_000)
    .output_files(4)
    .run()
)
```
`run()` must print the Persian summary itself; assignment is optional. Multiple `where()` calls are combined with AND. Use native Spark Column objects; no custom filter parser. Explain Python `&`, `|`, `~`, and parentheses in customer documentation. No SparkSession is exposed by the documented SDK API, though the trusted user can import PySpark independently.

## 5. Functional requirements
**FR-001** Query name is required, unique within the Tehran execution date, and validated against `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`.

**FR-002** Both ISO dates are required, inclusive, ordered, and cover 1–7 calendar days. Missing input days are an error rather than silently skipped.

**FR-003** The SDK builds fully qualified input HDFS URIs from administrator configuration, never customer-supplied paths.

**FR-004** `where()` accepts native Spark Connect-compatible Column expressions; schema/type errors are translated to Persian when feasible. No SQL strings, UDF authoring interface, joins, or aggregates are provided by the SDK.

**FR-005** `select()` accepts zero or more existing column names; omitted selection means all columns. Reject invalid/duplicate column names before output when possible.

**FR-006** `limit(n)` is mandatory: integer (not boolean), 1 ≤ n ≤ 1,000,000,000. Output row count never exceeds n; fewer matching rows are acceptable. Which matching rows are returned is nondeterministic without ordering.

**FR-007** `output_files(n)` is mandatory: positive integer with administrator-configured maximum. Validate estimated `ceil(limit / n)` against administrator-configured maximum rows per planned partition; reject unreasonable combinations with a clear Persian suggestion for a larger file count. This is an estimate based on requested limit, not guaranteed actual rows.

**FR-008** Write UTF-8 CSV with GZIP compression, documented header/null/escaping defaults, and Spark `part-*.csv.gz` names. Output folder: `<output_base>/result/yyyy/MM/dd/<query_name>/`, where the date is captured in `Asia/Tehran` immediately before submission. Reject existing destination; never overwrite.

**FR-009** Use `coalesce(n)` when reducing partition count is appropriate; use `repartition(n)` only when justified by imbalance. Set `maxRecordsPerFile` as a protective row cap. The requested `output_files(n)` is a *target partition count*, not a guarantee of exactly n physical CSV files: Spark may emit fewer for empty partitions or more due to maxRecordsPerFile. The SDK must explain actual file count after completion. Do not promise a strict compressed-byte ceiling; configured target compressed size is a tuning objective requiring sampling/estimation.

**FR-010** Run synchronously and allow only one SDK-managed query at a time in the configured Jupyter environment. The Notebook cell remains occupied until completion/error.

**FR-011** Create a fresh Spark Connect session for each `.run()` invocation using configured host:port. Set application/session naming prefix such as `extract_<query_name>_<short_execution_id>` where supported by Spark Connect; distinguish client session identity from Spark application name and do not promise per-request server-side application creation.

**FR-012** Session acquisition/readiness timeout defaults to 60 seconds (configurable). If connection/readiness exceeds the deadline, show a Persian error, attempt to cancel pending work, and release any created session. A client-side timeout alone cannot guarantee termination of a blocked network call or remote server operation; use a bounded worker/process or equivalent mechanism if strict timeout enforcement is required. Test actual Spark Connect cancellation semantics.

**FR-013** Always execute `try/finally` and call the supported session shutdown/release method; cancel active operations on failure/timeout where possible. Report cleanup failure to logs. Do not claim remote resource cleanup was confirmed unless it was verified. Session readiness and resource-pending states may be indistinguishable at the Connect API level; timeout should cover observable readiness, not promise precise scheduler-state detection.

**FR-014** Overall execution timeout is separately configurable (proposed default 3600 seconds). Timeout results in attempted cancellation, clear Persian status, and cleanup. Avoid retrying large jobs automatically.

**FR-015** Print Persian progress logs: validation, connecting, reading date partitions, applying filters, limiting rows, writing, counting/verification, output inspection, and completion. Internal detailed exception traces remain in log files.

**FR-016** On success print query name, execution ID, Tehran execution date, duration, verified output row count, actual number of CSV files, optional measured bytes, and a copyable download command. `run()` may additionally return a result object for advanced use.

**FR-017** Use an exact row-count strategy that does not misrepresent estimates as verified counts. A separate count action can cause a second scan; prefer counting the bounded output dataset with persistence where practical, or counting completed CSV rows with clear overhead. Document the chosen implementation and test with large datasets.

**FR-018** Write a small completion manifest (e.g., `_extract_manifest.json`) containing query name, execution ID, status COMPLETE, creation time, file names, actual row count and file sizes. Create it only after Spark output is complete and verified. Failed/partial output must not be presented as complete.

**FR-019** On error, log the original exception and present a comprehensible Persian message with an error code; examples: invalid date, invalid filter/column, missing day, existing output, too many records per file, session timeout, execution timeout, write failure, and download failure.

## 6. Download CLI
Command example: `data-extractor download customer_search --date 2026-01-07 --output ./downloads`.
- Resolve the HDFS result path internally using configured WebHDFS endpoint and Tehran execution date, without asking the customer for HDFS URLs.
- Require a COMPLETE manifest; list expected `part-*.csv.gz` files; stream each download to a temporary local file and atomically rename on success.
- Handle WebHDFS redirects, timeouts, non-2xx HTTP errors, and optionally retries; do not overwrite existing local files without explicit flag.
- Verify file sizes against the manifest, and optionally checksums if available. Return nonzero exit code and Persian message on failure.
- Provide a separate admin-configurable downloader configuration for machines outside Jupyter; do not embed internal endpoints in user examples.

## 7. Configuration and packaging
Recommended package layout:
```
project/
  pyproject.toml
  src/data_extractor/{__init__,query,session,validation,writer,result,config,messages,exceptions}.py
  src/data_extractor/resources/{defaults.yaml,messages_fa.yaml}
  cli/download.py
  notebooks/customer_example.ipynb
  docs/customer_guide_fa.docx
  tests/
```
Build with `python -m build`, install with `pip install dist/data_extractor-*.whl` (using an offline wheelhouse for dependencies as needed). Admin sets Spark Connect host/port, HDFS URI, input/output bases, timeouts, limits, and WebHDFS endpoint in package-bundled defaults or a protected external YAML file. Prefer external admin config for environment-specific secrets/addresses; embedding configuration at build time is supported but requires rebuilding to change. Customer edits only the Notebook query fields. Include a Persian message catalog and centralized structured logging.

Illustrative configuration:
```yaml
spark:
  connect_url: sc://spark-connect-host:15002
  session_prefix: extract
  connect_timeout_seconds: 60
  execution_timeout_seconds: 3600
hdfs:
  uri: hdfs://nn-cluster
  input_base: /warehouse/source
  output_base: /warehouse
query:
  max_days: 7
  max_limit: 1000000000
  max_output_files: 200
  max_planned_rows_per_file: 2000000
output:
  timezone: Asia/Tehran
  target_compressed_mb: null # set by administrator
  max_records_per_file: 2000000
webhdfs:
  base_url: http://webhdfs-host:9870
```
Values are examples, not environment discoveries. Ensure package resources are included in the built wheel. Pin PySpark client and dependency versions to the deployed Spark 4.x minor version.

## 8. Acceptance criteria
**AC-01** A Notebook user runs the example with no explicit SparkSession, HDFS address, or assignment variable.
**AC-02** Seven inclusive days accepted; eight rejected before Spark work.
**AC-03** Native Spark Column AND/OR/startswith filters and column projection work.
**AC-04** Missing/invalid/excessive limit and missing/invalid output file count produce Persian errors.
**AC-05** Unreasonable `limit/output_files` combinations are rejected according to admin policy.
**AC-06** Query output is gzip CSV at the Tehran-date HDFS path and existing output is never overwritten.
**AC-07** A new Spark Connect session is created per run and a cleanup attempt occurs on success, error, and timeout; tests confirm no client-side session remains usable after cleanup.
**AC-08** Session readiness taking longer than configured 60 seconds yields a Persian timeout and cancellation/cleanup attempt; any remote cleanup uncertainty is logged.
**AC-09** Completion output reports measured row count, actual file count, duration, and download command.
**AC-10** Downloader fetches all manifest-listed files without Hadoop UI, and refuses incomplete outputs.
**AC-11** Wheel installs in offline Jupyter environment using documented local dependencies and includes config/messages resources.
**AC-12** Persian Word guide covers first run, filter operators, examples, common errors, download, and basic troubleshooting.

## 9. Risks and design clarifications
- Spark Connect sessions do not necessarily map one-to-one to Spark applications or independently reserved executors. SDK controls session lifecycle; server controls resources.
- Strict cancellation of a hung remote operation cannot be guaranteed by Python `finally` alone. Test actual endpoint behavior and use process isolation if a hard deadline is required.
- Spark writer cannot guarantee exact requested number of physical files while also enforcing a row cap; prioritize worker safety and report actual file count.
- Exact CSV.gz size is unknown before compression; target size is advisory, not a hard maximum.
- A row `limit` is not a scan-cost limit, especially on 1 TB/day inputs.
- Persistent outputs require manual storage management.

## 10. Delivery
Deliver a buildable source tree, wheel package, one ready-to-run Notebook, Persian Word customer guide, WebHDFS CLI, example admin configuration, Persian messages, unit tests, and an end-to-end smoke-test checklist. This PRD specifies the product; it does not imply the SDK itself has already been implemented.
