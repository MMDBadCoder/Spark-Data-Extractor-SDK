from dataclasses import dataclass


@dataclass(frozen=True)
class Result:
    query_name: str
    execution_id: str
    execution_date: str
    duration_seconds: float
    row_count: int
    file_count: int
    total_bytes: int
    output_path: str
    download_command: str
