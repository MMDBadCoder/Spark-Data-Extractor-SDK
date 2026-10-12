# Execution and download design

`Query` validates names, ISO dates, native Column predicates, projection, row limits, and target files. `QuerySpec` carries validated input to execution. Runtime config resolution is explicit path, `DATA_EXTRACTOR_CONFIG`, root `admin.yaml` only when running from a source checkout, then packaged defaults. The root admin file is a source convenience placeholder; production endpoints belong in a protected external file. Downloader config remains separate, using its explicit path or `DATA_EXTRACTOR_DOWNLOAD_CONFIG`, and does not read runtime config or root `admin.yaml`. The package lives at repository root and imports directly from an extracted source ZIP without installation.

`Query.show_columns()` is a separate schema-discovery operation. It requires `.between(start, end)`, uses the administrator-configured fixed source path for the requested dates, opens a fresh Connect session, prints a Persian column/type table, and returns immutable metadata. It runs before predicates or projection and intentionally does not require export limit/file settings. It performs no filters, projection, row count, export, destination selection, or manifest publication. Discovery is serialized by the same process/file lock and bounded by the same session acquisition and execution deadlines, cancellation attempts, and cleanup path as export.

The execution caller obtains the process and Linux file lock, captures the Tehran execution date, and rejects an existing destination. A daemon worker creates a fresh `SparkSession.builder.remote(url).create()` session, checks observable readiness, verifies each required daily partition, reads Parquet, filters/projects/limits, and balances the bounded output. Target one uses coalesce; larger targets repartition to address global-limit concentration. No Connect RDD access or separate partition-count scan is used.

Spark writes gzip CSV with headers, UTF-8, double-quote quote/escape, quoteAll, null marker `\N`, and maxRecordsPerFile. A string-schema multiline FAILFAST reread provides exact output row count without another unbounded source count. WebHDFS inspection supplies file names and sizes. The worker always attempts session release and logs cleanup uncertainty. The caller then publishes a COMPLETE manifest containing identity, timestamp, measured row count, and file sizes. Completion is withheld after any error/timeout.

Readiness and execution deadlines are separate. The execution deadline includes reading, writing, verification, and cleanup. Python cannot kill a daemon thread; cancellation and stop attempts do not prove server termination. The exclusive lock remains held while a timed-out worker is alive. Further submissions are refused; investigate logs and restart the kernel if necessary. A timeout can leave partial output and administrators must inspect/clean it before reusing the name.

Final manifest publication begins only after successful worker completion and uses separate WebHDFS request timeouts. Therefore the entire call has no strict wall-clock cap. This explicit FR-014 limitation avoids allowing an abandoned worker to publish COMPLETE after a timeout; stricter transactional publication/deadline semantics require a separately qualified design. A kernel restart drops local locks but does not prove remote termination: inspect remote work before retrying.

The downloader constructs the path internally, validates a COMPLETE manifest and safe part names, streams each listed gzip file to a temporary local path, verifies byte size, and atomically renames. It follows WebHDFS redirects and uses per-request timeouts. No COMPLETE means no download. The result folder is `<output>/<name>_<execution-date>/`. Local overwrite is explicit, and size checks do not provide cryptographic integrity.

Configuration/secrets are external to the customer notebook. The customer is trusted: arbitrary notebook Python can import PySpark or edit accessible files. Enforce OS permissions and network policy outside the SDK. Spark sessions are client identities, not guaranteed separate server applications or executor allocations.

## API references for deployment review

- [Spark 4.0.1 SparkSession builder create](https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.builder.create.html)
- [Spark 4.0.1 coalesce](https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce.html)
- [Spark 4.0.1 repartition](https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html)
- [Spark CSV options](https://spark.apache.org/docs/4.0.1/sql-data-sources-csv.html)

These references describe the provisional client target. Confirm the deployed minor and qualify actual session/cancellation behavior separately.
