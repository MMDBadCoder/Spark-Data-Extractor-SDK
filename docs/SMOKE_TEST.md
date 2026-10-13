# End-to-end acceptance checklist

Run in a nonproduction namespace with known daily Parquet fixtures. Record Spark client/server versions, Python version, fixture schema, configuration, command outputs, logs, and observed files. Never claim live acceptance from mocked tests. All live statuses below are pending.

| AC | Check | Local evidence scope | Live status |
|---|---|---|---|
| 01 | Launch Jupyter from extracted source root and execute root-level notebook with no explicit session/path/result assignment | Source import subprocess and notebook structure | Pending |
| 02 | Accept seven inclusive days; reject eight before connection | Validation unit doubles | Pending |
| 03 | Test AND, OR, startswith, repeated where, projection and invalid predicates | Native Spark Connect Column construction without server; writer doubles | Pending |
| 04 | Missing, bool, zero, negative, excessive limits/file counts print Persian errors | Validation unit doubles | Pending |
| 05 | Reject planned rows exceeding admin cap; accept suggested larger count | Validation unit doubles | Pending |
| 06 | Tehran-date path, gzip headers, UTF-8; duplicate name does not overwrite | Writer/path unit doubles | Pending |
| 07 | Two successful runs create distinct sessions; success/error/timeout cleanup and unusable released client | Lifecycle unit doubles, cannot prove remote release | Pending |
| 08 | Delay readiness beyond deadline; Persian error, cancel/stop attempt, uncertainty logs, lock held until worker exits | Timeout unit doubles | Pending |
| 09 | Compare measured rows/file count/bytes/duration and printed download command with output | Manifest/count unit doubles | Pending |
| 10 | Download all listed files; reject missing/non-COMPLETE manifest, wrong sizes, unsafe names, existing files; test redirects/HTTP errors | HTTP unit doubles | Pending |
| 11 | Upload/extract source ZIP in Jupyter browser; run notebook and CLI from source tree | Source import and notebook structure can be checked locally; customer-host test pending | Pending |
| 12 | Open detailed Persian Markdown guide; validate RTL, code readability, first run/errors/download | Markdown content present; customer-host check remains | Pending |

## Local HTTP integration evidence (no Spark cluster)

`tests/test_webhdfs_http_e2e.py` starts a local standard-library HTTP server that
implements the NameNode redirects and DataNode transfers used by this client.
It exercises the production `WebHDFS` implementation and the CLI command path,
including manifest create/rename, COMPLETE validation, streamed file download,
byte-size verification, and refusal of a PARTIAL manifest. The test seeds the
server's in-memory namespace with a gzip CSV payload; it does not validate HDFS,
Hadoop authentication, Spark writes, Spark Connect, or a real cluster. The
config loader is substituted because the test uses ephemeral loopback settings.

The layered public-flow case also invokes `Query.run()`, the production executor
and writer, and manifest publication. Its narrowly scoped Spark-boundary double
accepts the writer's DataFrame calls and produces actual gzip CSV bytes, including
headers, Persian text with an embedded newline, empty strings, and the `\\N`
null marker. The production CLI then downloads those exact bytes over HTTP.
This provides full SDK/HTTP/CLI wiring coverage with simulated Spark behavior;
it does not prove PySpark serialization, Spark Connect behavior, Parquet reads,
or cluster execution.

In an environment with the runtime dependencies installed, run locally with
`python -m unittest discover -s tests -v`. The earlier HTTP integration results
described above belong to the prior packaging revision. This source-only revision
has not rerun the HTTP cases here because `requests` is unavailable and the
sandbox blocks local server sockets. Current runnable checks and limitations are
recorded in [STATUS.md](STATUS.md); all live status entries above remain pending.

Additional live checks:

- Use empty output, one-column null, empty string, literal `\N`, commas, quotes, embedded newlines, Persian strings, dates/decimals, and any complex types in the real schema. Verify exact CSV count and document null-marker collisions.
- Exercise coalesce one and repartition many; inspect actual files with maxRecordsPerFile caps. Do not require exactly the requested physical file count.
- Remove an input date and verify a missing-day error. Preserve existing output before a duplicate run.
- Stall execution during write/verification; caller returns timeout, no COMPLETE is published later, and a second kernel/process cannot start while worker holds the file lock. Inspect cancellation and remaining remote operations.
- Make session cleanup and manifest publication fail; verify no success claim and original details in protected logs.
- Measure a representative 100 GB–1 TB/day extraction, elapsed time, resource usage, source scan, and completed-CSV verification overhead. Confirm cluster limits externally.
- Test WebHDFS redirect host reachability, required simple/delegation authentication, partial downloads, explicit overwrite, and read-only customer configuration.
- Administrator inspects and removes partial folders manually. Confirm retention policy outside SDK.

A successful checklist qualifies only the tested endpoint/version/schema/configuration. Record evidence and sign-off before customer deployment.
