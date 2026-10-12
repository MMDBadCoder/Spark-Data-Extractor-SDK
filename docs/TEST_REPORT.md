# Test and coverage report

## Reviewed local evidence

Lead review approved the local prototype on 2026-10-12 after running:

```sh
PYTHONPATH=.:.test-deps python -m coverage erase
PYTHONPATH=.:.test-deps python -m coverage run --branch -m unittest discover -s tests -v
PYTHONPATH=.:.test-deps python -m coverage report --fail-under=85
```

Result: **44 tests passed** on Python 3.12 in 3.54 seconds. Coverage.py 7.16.2 measured **87% statement-and-branch coverage** across `data_extractor` (801 statements, 87 missed; 222 branches, 40 partial). The checked-in configuration enables branch measurement and enforces an 85% minimum.

| Module | Coverage |
|---|---:|
| `cli.py` | 82% |
| `config.py` | 88% |
| `executor.py` | 92% |
| `query.py` | 92% |
| `session.py` | 61% |
| `validation.py` | 93% |
| `webhdfs.py` | 82% |
| `writer.py` | 92% |
| Package total, including small modules | **87%** |

The suite includes native Spark Connect `Column` construction with the 4.0.1 client, lifecycle/timeout tests with injected sessions, writer tests with DataFrame doubles, WebHDFS failure tests, and three local HTTP protocol-integration tests. The broadest layered test calls public `Query.run()`, the production executor and writer, a Spark-boundary double that emits real gzip CSV, production WebHDFS manifest publication over a loopback HTTP server, and the production CLI download path. It verifies headers, Persian multiline data, empty values, the `\N` null marker, measured rows/files, session stop, COMPLETE publication, and exact downloaded bytes.

Seven focused discovery tests exercise `Query.show_columns()`: dates are required while limit/file count are optional; each day must contain a Parquet file; a directory ending in `.parquet` is rejected; attached filters/projection are ignored; schema failures are translated; no writer, manifest, destination, or row-count action occurs; and a timed-out late worker retains exclusivity until cleanup. Executor regression tests confirm the shared lifecycle still creates and releases fresh sessions and prevents late COMPLETE publication.

This is simulated Spark with real local HTTP and CLI wiring. It is not a Spark Connect, HDFS, or cluster end-to-end result.

## Packaging evidence

The lead built `data_extractor-0.2.0-py3-none-any.whl` and its source distribution in an isolated build environment. The wheel installed into a fresh virtual environment with `--no-deps`; distribution metadata reported 0.2.0, the exported `ColumnInfo` imported, and both packaged YAML resources were readable through `importlib.resources`. The CLI help entry point ran with the repository's already installed runtime dependencies supplied on `PYTHONPATH`. This proves wheel structure, entry-point wiring, resource packaging, and version identity; it does not qualify a complete offline dependency wheelhouse or customer host.

The final Persian Word guide was generated as US Letter with 11-point Persian-capable RTL body text, 10-point tables, and 9-point LTR code. The canonical DOCX renderer produced two pages; both were inspected for glyphs, direction, clipping, overlap, table flow, and page breaks, with no visible defect found.

## Residual release risks

- Real Spark Connect readiness, native predicate serialization, output semantics, interruption, `stop()`, remote cleanup, and server-version compatibility require the deployed endpoint.
- Real WebHDFS redirect routing, authentication, atomic rename semantics, and partial-upload cleanup require the deployed endpoint.
- Large-source scan cost, completed-output recount overhead, partition behavior, gzip/CSV edge cases against the real schema, and 100 GB–1 TB/day performance remain unmeasured.
- `session.py` has the lowest local coverage because its meaningful branches depend on live PySpark behavior. Raising coverage with deeper mocks would not resolve that deployment risk.
- The overall timeout still excludes final manifest publication, as documented in `ASSESSMENT.md`; strict remote cancellation is not claimed.

All live acceptance rows in `SMOKE_TEST.md` remain pending until evidence is captured from the target environment.

## Root-package/config follow-up

After moving the package from `src/data_extractor/` to root `data_extractor/`, the following local checks passed:

- `python -m pip wheel . --no-deps --no-build-isolation -w /tmp/spark-sdk-wheel` built `data_extractor-0.2.0-py3-none-any.whl`.
- A subprocess imported `data_extractor` from both the checkout root and a source-tree ZIP, and loaded packaged resources from the ZIP outside the checkout. These import-only processes supplied a minimal `requests` import stub because the runtime lacks the declared dependency. The ZIP check also confirmed that a working-directory `admin.yaml` is ignored when the package is loaded from an archive.
- `python -m unittest tests.test_source_import tests.test_config tests.test_validation tests.test_writer -v` passed 23 tests, including direct imports from a clean checkout subprocess and source ZIP, explicit/environment/source-checkout/default config precedence, and downloader separation.

The full suite was attempted, but this runtime does not have `requests` or `pytest`, and dependency installation could not be completed. The full-suite figures above document the prior lead-reviewed package state, not a full rerun after this layout-only follow-up. No live Spark or HDFS validation was performed.
