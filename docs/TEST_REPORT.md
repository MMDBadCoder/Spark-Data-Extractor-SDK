# Test and coverage report

## Reviewed local evidence

Lead review approved the local prototype on 2026-10-12 after running:

```sh
PYTHONPATH=.test-deps:src python -m coverage erase
PYTHONPATH=.test-deps:src python -m coverage run -m pytest -q
PYTHONPATH=.test-deps:src python -m coverage report
```

Result: **37 tests and 43 subtests passed** on Python 3.12 in 3.44 seconds. Coverage.py 7.16.2 measured **87% statement-and-branch coverage** across `src/data_extractor` (747 statements, 85 missed; 216 branches, 39 partial). The checked-in configuration enables branch measurement and enforces an 85% minimum.

| Module | Coverage |
|---|---:|
| `cli.py` | 82% |
| `config.py` | 88% |
| `executor.py` | 91% |
| `query.py` | 93% |
| `session.py` | 61% |
| `validation.py` | 93% |
| `webhdfs.py` | 82% |
| `writer.py` | 92% |
| Package total, including small modules | **87%** |

The suite includes native Spark Connect `Column` construction with the 4.0.1 client, lifecycle/timeout tests with injected sessions, writer tests with DataFrame doubles, WebHDFS failure tests, and three local HTTP protocol-integration tests. The broadest layered test calls public `Query.run()`, the production executor and writer, a Spark-boundary double that emits real gzip CSV, production WebHDFS manifest publication over a loopback HTTP server, and the production CLI download path. It verifies headers, Persian multiline data, empty values, the `\N` null marker, measured rows/files, session stop, COMPLETE publication, and exact downloaded bytes.

This is simulated Spark with real local HTTP and CLI wiring. It is not a Spark Connect, HDFS, or cluster end-to-end result.

## Packaging evidence

The lead also built `data_extractor-0.1.0-py3-none-any.whl` in an isolated build environment, installed it into a fresh virtual environment with `--no-deps`, invoked the installed `data-extractor --help` entry point, and verified both packaged YAML resources using `importlib.resources`. The installed distribution reported version 0.1.0. This proves wheel structure and entry-point/resource packaging; it does not qualify an offline dependency wheelhouse or the customer host.

## Residual release risks

- Real Spark Connect readiness, native predicate serialization, output semantics, interruption, `stop()`, remote cleanup, and server-version compatibility require the deployed endpoint.
- Real WebHDFS redirect routing, authentication, atomic rename semantics, and partial-upload cleanup require the deployed endpoint.
- Large-source scan cost, completed-output recount overhead, partition behavior, gzip/CSV edge cases against the real schema, and 100 GB–1 TB/day performance remain unmeasured.
- `session.py` has the lowest local coverage because its meaningful branches depend on live PySpark behavior. Raising coverage with deeper mocks would not resolve that deployment risk.
- The overall timeout still excludes final manifest publication, as documented in `ASSESSMENT.md`; strict remote cancellation is not claimed.

All live acceptance rows in `SMOKE_TEST.md` remain pending until evidence is captured from the target environment.
