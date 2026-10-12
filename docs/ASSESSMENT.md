# PRD assessment

The approved specification fits a small trusted-user SDK. No new backend, scheduler, Spark deployment, or multiuser platform is needed. The fixed schema, seven-day range, protected administrator settings, and bounded output make a narrow customer API practical.

The critical deployment risks are live Spark Connect cancellation semantics, endpoint client compatibility, WebHDFS redirects/authentication, source-scale scan cost, and CSV round-trip handling. Unit doubles can verify ordering and failure paths; they cannot prove remote resource release or performance on 100 GB–1 TB daily inputs.

`Query.show_columns()` adds a discovery step before filters and projection. After `.between(...)`, it opens a fresh Connect session to read the fixed administrator-configured source schema, prints a Persian column/type table, and returns immutable metadata. It requires dates, but no export limit or target file count. It performs no row count, filter, projection, export, destination selection, or manifest creation. Discovery uses the same exclusive SDK lock and acquisition/execution deadlines, cancellation attempts, and cleanup guarantees as export. Its live schema and session behavior must be included in deployment smoke qualification.

## Explicit design decisions

- PySpark 4.0.1 is a provisional optional client target until the deployed Spark 4.x minor is confirmed. Pin and qualify the transitive wheelhouse for that environment.
- Use native Column expressions and immutable validated execution data; customer paths and sessions are absent from the documented API.
- Use a daemon worker and process/file locks. A timeout returns promptly and attempts cancellation/stop. A blocked worker retains exclusivity until it exits. This prevents overlapping SDK jobs, but does not terminate a hung remote call. A stricter termination requirement needs qualified process isolation.
- A global limit can concentrate/imbalance partitions. Use `coalesce(1)` for one target and `repartition(n)` for larger targets to redistribute bounded output; avoid unsupported Connect `.rdd` and a partition-count scan. Physical file count remains unconstrained beyond Spark row caps.
- Count completed CSV with an explicit all-string schema, multiline parsing, and FAILFAST. This avoids a repeated source count and measures actual output at the cost of a bounded output scan.
- Use quoted fields and `\N` null marker so one-column null/empty rows survive counting. This differs from an empty null representation; literal null-marker collisions must be validated on the real schema. CSV is an export format, not a lossless type archive.
- The caller publishes COMPLETE only after the worker successfully finishes verification and cleanup. A timeout worker cannot publish COMPLETE. Manifest publication has its own bounded WebHDFS request timeout.
- FR-014 is partial: the Spark pipeline has an overall deadline, but final manifest publication is outside it. HTTP timeouts are per request/inactivity, not a hard whole-call deadline. Do not certify strict overall timeout acceptance from this prototype.
- Downloader supports simple user identity or delegation tokens. Kerberos/proxy authentication is an explicit deployment gap requiring an adapter.

A row limit cannot cap source scan cost. Exact compressed sizes and exact requested physical file counts are not promised. Partial failures may leave folders without manifests; administration owns cleanup and retention. See the smoke checklist before customer release.

## Packaging and configuration follow-up

The distribution now uses a single importable `data_extractor/` package at the repository root, so a source checkout or ZIP source tree can import it without adding `src/` to `PYTHONPATH`. Setuptools discovers only the root package and includes its YAML resources. The root `admin.yaml` is a non-live source-checkout placeholder. Runtime config resolution selects an explicit path, then `DATA_EXTRACTOR_CONFIG`, then that root file only when the package is inside a checkout, and finally packaged defaults. Downloader resolution uses its own explicit path or `DATA_EXTRACTOR_DOWNLOAD_CONFIG` and never inherits runtime config or checkout `admin.yaml`.

The config tests cover those precedence and isolation rules. A complete suite rerun still requires the project's declared `requests` and `pytest` dependencies, which are absent from this execution image.
