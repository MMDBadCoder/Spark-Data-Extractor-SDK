# Spark Connect Data Extractor

A small Python SDK for a trusted customer using Jupyter: read administrator-configured daily HDFS Parquet, apply native Spark Column filters, and export verified gzip CSV. A Linux CLI downloads completed files through WebHDFS. The approved [PRD](docs/PRD.md) is preserved verbatim.

```python
from pyspark.sql.functions import col
from data_extractor import Query

query = Query("customer_search")
query.between("2026-01-01", "2026-01-07")
query.where((col("a") > 10) & col("b").startswith("asdasa"))
query.select("a", "b", "c")
query.limit(1_000_000)
query.output_files(4)
query.run()
```

Use a fresh Connect-only kernel; importing `data_extractor` enables Connect mode before `col()` is evaluated. Do not mix this SDK with an already active classic Spark session. Replace sample column names with the fixed dataset's actual columns. `run()` prints Persian progress and a completion summary. Dates are inclusive, at most seven days; every input day must exist. `limit` and `output_files` are required. Multiple `where` calls combine with AND. Rows selected by `limit` are nondeterministic.

## Install and configure

Python 3.10+ is required. The optional Connect extra provisionally pins PySpark 4.0.1; the deployed Spark minor version is unknown. An administrator must match and validate the client and its transitive dependencies against the real endpoint before deployment. The pin is a target assumption, not a compatibility certification.

```bash
python -m pip install -e '.[connect,dev]'
python -m build
python -m pip install 'dist/data_extractor-0.1.0-py3-none-any.whl[connect]'
export DATA_EXTRACTOR_CONFIG=/protected/admin.yaml
```

An administrator fills [examples/admin.yaml](examples/admin.yaml), protects it from customer edits, and preconfigures the notebook. Packaged defaults intentionally contain no live endpoints. External YAML overrides defaults. The SDK protects its documented API, not an isolated Python runtime against a malicious user.

For offline installation, on a connected machine with the same Python/platform, collect the wheel plus dependencies:

```bash
python -m pip download --dest wheelhouse 'dist/data_extractor-0.1.0-py3-none-any.whl[connect]'
# Transfer wheelhouse to Jupyter, then:
python -m pip install --no-index --find-links wheelhouse 'data-extractor[connect]==0.1.0'
```

Copy the [customer notebook](notebooks/customer_example.ipynb) and [Persian guide](docs/customer_guide_fa.docx) to the customer environment. The wheel includes YAML resources; notebook and guide are source-delivery artifacts.

## Download

Configure the download machine independently with [examples/downloader.yaml](examples/downloader.yaml):

```bash
export DATA_EXTRACTOR_DOWNLOAD_CONFIG=/protected/downloader.yaml
data-extractor download customer_search --date 2026-01-07 --output ./downloads
```

Use the **Tehran execution date printed by the SDK**, not an input date. The CLI requires a COMPLETE manifest and verifies downloaded sizes. It uses temporary files and atomic renames, refuses existing local files unless `--overwrite` is supplied, and stores files under `downloads/customer_search_2026-01-07/`. WebHDFS redirects must be reachable. Basic `user.name` or delegation-token authentication is supported; Kerberos/proxy environments need an adapter before deployment.

## Output and limits

Output is `<output_base>/result/yyyy/MM/dd/<name>/`, with `part-*.csv.gz`, headers, UTF-8, quoteAll and `\N` null marker, and double-quote quote/escape characters. Requested files are target partitions: empty partitions or row caps change actual file counts. Compression size is advisory. The verified row count rereads the bounded completed CSV using an explicit all-string schema with FAILFAST and multiline parsing, adding an output scan. A row limit does not bound source scan cost.

Each run creates a fresh Connect session. Deadlines return a Persian timeout and attempt cancellation/stop; they cannot prove remote cleanup. A blocked worker retains the job lock until it exits, so another run may be refused and a notebook kernel restart may be required. Partial files may remain and must be cleaned by an administrator; they never receive a COMPLETE manifest. Retention is manual.

The execution deadline covers Spark acquisition through output verification and session cleanup. Final manifest publication follows successful verification and uses WebHDFS per-request timeouts; it can add time beyond that deadline. This prototype does not promise a hard wall-clock deadline for the entire `run()` call. Restarting a kernel also cannot prove a remote job stopped; the administrator must inspect the server before resubmitting.

## Validation and delivery

See [assessment](docs/ASSESSMENT.md), [design](docs/DESIGN.md), [status](docs/STATUS.md), and [cluster smoke checklist](docs/SMOKE_TEST.md). Local mocked tests are not proof of live Spark/HDFS behavior. Publication and licensing status are in [repository notes](docs/REPOSITORY.md). No open-source license is currently selected.

Run tests with `python -m unittest discover -s tests -v` after installing dependencies. The native Column test uses the optional Connect client and skips when it is absent. It constructs real expressions without a live Spark endpoint.
