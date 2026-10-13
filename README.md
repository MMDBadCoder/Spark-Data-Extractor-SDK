# Spark Connect Data Extraction SDK

A source-only Python SDK for a trusted customer using Jupyter. It reads administrator-configured daily HDFS Parquet data, applies native Spark Connect `Column` filters, and writes verified gzip CSV output. The SDK targets Python 3.10.12 and PySpark 3.4.4 with Hadoop 2.10.2. The approved [PRD](docs/PRD.md) is retained verbatim; source delivery differs from its wheel and Word-guide requirements, as recorded in [status](docs/STATUS.md).

## Jupyter quick start

1. Upload one project ZIP in the same folder as the temporary notebook (or use Jupyter's Extract Archive menu). The ZIP can keep its original name; no terminal or rename is needed. If Jupyter has no extract menu, use the notebook `zipfile` steps in the [Persian guide](docs/customer_guide_fa.md#اگر-jupyter-دکمهٔ-extract-ندارد-استخراج-با-دفترچهٔ-موقت). Open the extracted project root, which contains `admin.yaml`, `data_extractor/`, `customer_example.ipynb`, and `docs/`.
2. An authorized administrator can edit root `admin.yaml` in Jupyter after extraction, or prepare it in the deployed copy beforehand. The version in this repository contains placeholders only. The separate downloader uses `examples/downloader.yaml` or `DATA_EXTRACTOR_DOWNLOAD_CONFIG` on the download machine.
3. Open `customer_example.ipynb` and follow its diagnostic cell. Create `Query(...)` before importing or calling `pyspark.sql.functions.col`, so PySpark 3.4.4 uses the configured Connect URL for native column expressions.
4. Replace the sample date with one confirmed to exist, then run the first export with a limit of 10 rows and one target file. Filters and column selection can be added after this first run. Keep the final cell open until completion.

## Query shape

```python
from data_extractor import Query
query = Query("customer_search")  # reads admin.yaml and configures SPARK_REMOTE
from pyspark.sql.functions import col
(query.between("2026-01-01", "2026-01-07")
 .where((col("customer_id") > 10) & col("status").startswith("ready"))
 .select("customer_id", "status")
 .limit(1_000_000)
 .output_files(4)
 .run())
```

`run()` prints Persian progress and a completion summary. Dates are inclusive and limited to seven days; all input days must exist. Multiple `where()` calls combine with AND. Use Python `&`, `|`, and `~` with parentheses. Output files are a target partition count, not a guarantee of exact physical files. Row selection is nondeterministic without ordering. Use a fresh kernel for a new admin configuration. If a conflicting `SPARK_REMOTE` is already set, reset the kernel and check configuration.

## Download completed output

On a separately configured Linux download machine:

```bash
python -m data_extractor.cli download customer_search --date 2026-01-07 --output ./downloads
```

Run from the project root or set `PYTHONPATH` to that root. Supply downloader-only settings using `examples/downloader.yaml` via `DATA_EXTRACTOR_DOWNLOAD_CONFIG`. Use the Tehran execution date reported by the notebook. The CLI requires a COMPLETE manifest and verifies file sizes. It does not require Spark dependencies to be configured for the downloader.

## Validation

Run `python -m unittest discover -s tests -v` in an environment with the SDK runtime dependencies installed. Unit tests use mocks and local files; they do not establish live Spark, HDFS, WebHDFS, cancellation, or production-scale behavior. Complete the gates in [SMOKE_TEST.md](docs/SMOKE_TEST.md) before deployment. Current findings and known limitations are in [STATUS.md](docs/STATUS.md).
