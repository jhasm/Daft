import builtins
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Iterator

import pyarrow

from daft.execution import physical_plan
from daft.io.scan import ScanOperator
from daft.plan_scheduler.physical_plan_scheduler import PartitionT
from daft.runners.partitioning import PartitionCacheEntry
from daft.sql.sql_connection import SQLConnection

if TYPE_CHECKING:
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.table import TableProperties as IcebergTableProperties

class ImageMode(Enum):
    """
    Supported image modes for Daft's image type.
    """

    #: 8-bit grayscale
    L: int

    #: 8-bit grayscale + alpha
    LA: int

    #: 8-bit RGB
    RGB: int

    #: 8-bit RGB + alpha
    RGBA: int

    #: 16-bit grayscale
    L16: int

    #: 16-bit grayscale + alpha
    LA16: int

    #: 16-bit RGB
    RGB16: int

    #: 16-bit RGB + alpha
    RGBA16: int

    #: 32-bit floating RGB
    RGB32F: int

    #: 32-bit floating RGB + alpha
    RGBA32F: int

    @staticmethod
    def from_mode_string(mode: str) -> ImageMode:
        """
        Create an ImageMode from its string representation.

        Args:
            mode: String representation of the mode. This is the same as the enum
                attribute name, e.g. ``ImageMode.from_mode_string("RGB")`` would
                return ``ImageMode.RGB``.
        """
        ...

class ImageFormat(Enum):
    """
    Supported image formats for Daft's image I/O.
    """

    PNG: int
    JPEG: int
    TIFF: int
    GIF: int
    BMP: int

    @staticmethod
    def from_format_string(mode: str) -> ImageFormat:
        """
        Create an ImageFormat from its string representation.
        """
        ...

class JoinType(Enum):
    """
    Type of a join operation.
    """

    Inner: int
    Left: int
    Right: int
    Outer: int

    @staticmethod
    def from_join_type_str(join_type: str) -> JoinType:
        """
        Create a JoinType from its string representation.

        Args:
            join_type: String representation of the join type. This is the same as the enum
                attribute name (but snake-case), e.g. ``JoinType.from_join_type_str("inner")`` would
                return ``JoinType.Inner``.
        """
        ...

class JoinStrategy(Enum):
    """
    Join strategy (algorithm) to use.
    """

    Hash: int
    SortMerge: int
    Broadcast: int

    @staticmethod
    def from_join_strategy_str(join_strategy: str) -> JoinStrategy:
        """
        Create a JoinStrategy from its string representation.

        Args:
            join_strategy: String representation of the join strategy. This is the same as the enum
                attribute name (but snake-case), e.g. ``JoinType.from_join_strategy_str("sort_merge")`` would
                return ``JoinStrategy.SortMerge``.
        """
        ...

class CountMode(Enum):
    """
    Supported count modes for Daft's count aggregation.

    | All   - Count both non-null and null values.
    | Valid - Count only valid values.
    | Null  - Count only null values.
    """

    All: int
    Valid: int
    Null: int

    @staticmethod
    def from_count_mode_str(count_mode: str) -> CountMode:
        """
        Create a CountMode from its string representation.

        Args:
            count_mode: String representation of the count mode , e.g. "all", "valid", or "null".
        """
        ...

class ResourceRequest:
    """
    Resource request for a query fragment task.
    """

    num_cpus: float | None
    num_gpus: float | None
    memory_bytes: int | None

    def __init__(
        self,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
        memory_bytes: int | None = None,
    ): ...
    @staticmethod
    def max_resources(resource_requests: list[ResourceRequest]):
        """Take a field-wise max of the list of resource requests."""
        ...

    def __add__(self, other: ResourceRequest) -> ResourceRequest: ...
    def __repr__(self) -> str: ...
    def __eq__(self, other: ResourceRequest) -> bool: ...  # type: ignore[override]
    def __ne__(self, other: ResourceRequest) -> bool: ...  # type: ignore[override]

class FileFormat(Enum):
    """
    Format of a file, e.g. Parquet, CSV, and JSON.
    """

    Parquet: int
    Csv: int
    Json: int

class ParquetSourceConfig:
    """
    Configuration of a Parquet data source.
    """

    coerce_int96_timestamp_unit: PyTimeUnit | None
    field_id_mapping: dict[int, PyField] | None

    def __init__(
        self,
        coerce_int96_timestamp_unit: PyTimeUnit | None = None,
        field_id_mapping: dict[int, PyField] | None = None,
    ): ...

class CsvSourceConfig:
    """
    Configuration of a CSV data source.
    """

    delimiter: str | None
    has_headers: bool
    double_quote: bool
    quote: str | None
    escape_char: str | None
    comment: str | None
    buffer_size: int | None
    chunk_size: int | None

    def __init__(
        self,
        has_headers: bool,
        double_quote: bool,
        delimiter: str | None,
        quote: str | None,
        escape_char: str | None,
        comment: str | None,
        buffer_size: int | None = None,
        chunk_size: int | None = None,
    ): ...

class JsonSourceConfig:
    """
    Configuration of a JSON data source.
    """

    buffer_size: int | None
    chunk_size: int | None

    def __init__(
        self,
        buffer_size: int | None = None,
        chunk_size: int | None = None,
    ): ...

class DatabaseSourceConfig:
    """
    Configuration of a database data source.
    """

    sql: str
    conn: SQLConnection

    def __init__(self, sql: str, conn_factory: SQLConnection): ...

class FileFormatConfig:
    """
    Configuration for parsing a particular file format (Parquet, CSV, JSON).
    """

    config: (
        ParquetSourceConfig | CsvSourceConfig | JsonSourceConfig | DatabaseSourceConfig
    )

    @staticmethod
    def from_parquet_config(config: ParquetSourceConfig) -> FileFormatConfig:
        """
        Create a Parquet file format config.
        """
        ...

    @staticmethod
    def from_csv_config(config: CsvSourceConfig) -> FileFormatConfig:
        """
        Create a CSV file format config.
        """
        ...

    @staticmethod
    def from_json_config(config: JsonSourceConfig) -> FileFormatConfig:
        """
        Create a JSON file format config.
        """
        ...

    @staticmethod
    def from_database_config(config: DatabaseSourceConfig) -> FileFormatConfig:
        """
        Create a database file format config.
        """
        ...

    def file_format(self) -> FileFormat:
        """
        Get the file format for this config.
        """
        ...

    def __eq__(self, other: FileFormatConfig) -> bool: ...  # type: ignore[override]
    def __ne__(self, other: FileFormatConfig) -> bool: ...  # type: ignore[override]

class CsvConvertOptions:
    """
    Options for converting CSV data to Daft data.
    """

    limit: int | None
    include_columns: list[str] | None
    column_names: list[str] | None
    schema: PySchema | None
    predicate: PyExpr | None

    def __init__(
        self,
        limit: int | None = None,
        include_columns: list[str] | None = None,
        column_names: list[str] | None = None,
        schema: PySchema | None = None,
        predicate: PyExpr | None = None,
    ): ...

class CsvParseOptions:
    """
    Options for parsing CSV files.
    """

    has_header: bool
    delimiter: str | None
    double_quote: bool
    quote: str | None
    escape_char: str | None
    comment: str | None

    def __init__(
        self,
        has_header: bool = True,
        delimiter: str | None = None,
        double_quote: bool = True,
        quote: str | None = None,
        escape_char: str | None = None,
        comment: str | None = None,
    ): ...

class CsvReadOptions:
    """
    Options for reading CSV files.
    """

    buffer_size: int | None
    chunk_size: int | None

    def __init__(
        self,
        buffer_size: int | None = None,
        chunk_size: int | None = None,
    ): ...

class JsonConvertOptions:
    """
    Options for converting JSON data to Daft data.
    """

    limit: int | None
    include_columns: list[str] | None
    schema: PySchema | None

    def __init__(
        self,
        limit: int | None = None,
        include_columns: list[str] | None = None,
        schema: PySchema | None = None,
    ): ...

class JsonParseOptions:
    """
    Options for parsing JSON files.
    """

class JsonReadOptions:
    """
    Options for reading JSON files.
    """

    buffer_size: int | None
    chunk_size: int | None

    def __init__(
        self,
        buffer_size: int | None = None,
        chunk_size: int | None = None,
    ): ...

class FileInfo:
    """
    Metadata for a single file.
    """

    file_path: str
    file_size: int | None
    num_rows: int | None

class FileInfos:
    """
    Metadata for a collection of files.
    """

    file_paths: list[str]
    file_sizes: list[int | None]
    num_rows: list[int | None]

    @staticmethod
    def from_infos(
        file_paths: list[str], file_sizes: list[int | None], num_rows: list[int | None]
    ) -> FileInfos: ...
    @staticmethod
    def from_table(table: PyTable) -> FileInfos:
        """
        Create from a Daft table with "path", "size", and "num_rows" columns.
        """
        ...

    def extend(self, new_infos: FileInfos) -> FileInfos:
        """
        Concatenate two FileInfos together.
        """
        ...

    def __getitem__(self, idx: int) -> FileInfo: ...
    def to_table(self) -> PyTable:
        """
        Convert to a Daft table with "path", "size", and "num_rows" columns.
        """

    def __len__(self) -> int: ...

class S3Config:
    """
    I/O configuration for accessing an S3-compatible system.
    """

    region_name: str | None
    endpoint_url: str | None
    key_id: str | None
    session_token: str | None
    access_key: str | None
    max_connections: int
    retry_initial_backoff_ms: int
    connect_timeout_ms: int
    read_timeout_ms: int
    num_tries: int
    retry_mode: str | None
    anonymous: bool
    use_ssl: bool
    verify_ssl: bool
    check_hostname_ssl: bool
    requester_pays: bool | None
    force_virtual_addressing: bool | None
    profile_name: str | None

    def __init__(
        self,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        key_id: str | None = None,
        session_token: str | None = None,
        access_key: str | None = None,
        max_connections: int | None = None,
        retry_initial_backoff_ms: int | None = None,
        connect_timeout_ms: int | None = None,
        read_timeout_ms: int | None = None,
        num_tries: int | None = None,
        retry_mode: str | None = None,
        anonymous: bool | None = None,
        use_ssl: bool | None = None,
        verify_ssl: bool | None = None,
        check_hostname_ssl: bool | None = None,
        requester_pays: bool | None = None,
        force_virtual_addressing: bool | None = None,
        profile_name: str | None = None,
    ): ...
    def replace(
        self,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        key_id: str | None = None,
        session_token: str | None = None,
        access_key: str | None = None,
        max_connections: int | None = None,
        retry_initial_backoff_ms: int | None = None,
        connect_timeout_ms: int | None = None,
        read_timeout_ms: int | None = None,
        num_tries: int | None = None,
        retry_mode: str | None = None,
        anonymous: bool | None = None,
        use_ssl: bool | None = None,
        verify_ssl: bool | None = None,
        check_hostname_ssl: bool | None = None,
        requester_pays: bool | None = None,
        force_virtual_addressing: bool | None = None,
        profile_name: str | None = None,
    ) -> S3Config:
        """Replaces values if provided, returning a new S3Config"""
        ...

    @staticmethod
    def from_env() -> S3Config:
        """Creates an S3Config, retrieving credentials and configurations from the current environment"""
        ...

class AzureConfig:
    """
    I/O configuration for accessing Azure Blob Storage.
    """

    storage_account: str | None
    access_key: str | None
    anonymous: str | None
    endpoint_url: str | None = None
    use_ssl: bool | None = None

    def __init__(
        self,
        storage_account: str | None = None,
        access_key: str | None = None,
        anonymous: str | None = None,
        endpoint_url: str | None = None,
        use_ssl: bool | None = None,
    ): ...
    def replace(
        self,
        storage_account: str | None = None,
        access_key: str | None = None,
        anonymous: str | None = None,
        endpoint_url: str | None = None,
        use_ssl: bool | None = None,
    ) -> AzureConfig:
        """Replaces values if provided, returning a new AzureConfig"""
        ...

class GCSConfig:
    """
    I/O configuration for accessing Google Cloud Storage.
    """

    project_id: str | None
    anonymous: bool

    def __init__(
        self, project_id: str | None = None, anonymous: bool | None = None
    ): ...
    def replace(
        self, project_id: str | None = None, anonymous: bool | None = None
    ) -> GCSConfig:
        """Replaces values if provided, returning a new GCSConfig"""
        ...

class IOConfig:
    """
    Configuration for the native I/O layer, e.g. credentials for accessing cloud storage systems.
    """

    s3: S3Config
    azure: AzureConfig
    gcs: GCSConfig

    def __init__(
        self,
        s3: S3Config | None = None,
        azure: AzureConfig | None = None,
        gcs: GCSConfig | None = None,
    ): ...
    @staticmethod
    def from_json(input: str) -> IOConfig:
        """
        Recreate an IOConfig from a JSON string.
        """
        ...

    def replace(
        self,
        s3: S3Config | None = None,
        azure: AzureConfig | None = None,
        gcs: GCSConfig | None = None,
    ) -> IOConfig:
        """Replaces values if provided, returning a new IOConfig"""
        ...

class NativeStorageConfig:
    """
    Storage configuration for the Rust-native I/O layer.
    """

    # Whether or not to use a multithreaded tokio runtime for processing I/O
    multithreaded_io: bool
    io_config: IOConfig

    def __init__(self, multithreaded_io: bool, io_config: IOConfig): ...

class PythonStorageConfig:
    """
    Storage configuration for the legacy Python I/O layer.
    """

    io_config: IOConfig

    def __init__(self, io_config: IOConfig): ...

class StorageConfig:
    """
    Configuration for interacting with a particular storage backend, using a particular
    I/O layer implementation.
    """

    @staticmethod
    def native(config: NativeStorageConfig) -> StorageConfig:
        """
        Create from a native storage config.
        """
        ...

    @staticmethod
    def python(config: PythonStorageConfig) -> StorageConfig:
        """
        Create from a Python storage config.
        """
        ...

    @property
    def config(self) -> NativeStorageConfig | PythonStorageConfig: ...

class ScanTask:
    """
    A batch of scan tasks for reading data from an external source.
    """

    def num_rows(self) -> int:
        """
        Get number of rows that will be scanned by this ScanTask.
        """
        ...

    def size_bytes(self) -> int:
        """
        Get number of bytes that will be scanned by this ScanTask.
        """
        ...

    def estimate_in_memory_size_bytes(self, cfg: PyDaftExecutionConfig) -> int:
        """
        Estimate the In Memory Size of this ScanTask.
        """
        ...

    @staticmethod
    def catalog_scan_task(
        file: str,
        file_format: FileFormatConfig,
        schema: PySchema,
        num_rows: int,
        storage_config: StorageConfig,
        size_bytes: int | None,
        pushdowns: Pushdowns | None,
        partition_values: PyTable | None,
        stats: PyTable | None,
    ) -> ScanTask | None:
        """
        Create a Catalog Scan Task
        """
        ...

    @staticmethod
    def sql_scan_task(
        url: str,
        file_format: FileFormatConfig,
        schema: PySchema,
        num_rows: int | None,
        storage_config: StorageConfig,
        size_bytes: int | None,
        pushdowns: Pushdowns | None,
        stats: PyTable | None,
    ) -> ScanTask:
        """
        Create a SQL Scan Task
        """
        ...

    @staticmethod
    def python_factory_func_scan_task(
        module: str,
        func_name: str,
        func_args: tuple[Any, ...],
        schema: PySchema,
        num_rows: int | None,
        size_bytes: int | None,
        pushdowns: Pushdowns | None,
        stats: PyTable | None,
    ) -> ScanTask:
        """
        Create a Python factory function Scan Task
        """
        ...

class ScanOperatorHandle:
    """
    A handle to a scan operator.
    """

    @staticmethod
    def anonymous_scan(
        files: list[str],
        schema: PySchema,
        file_format_config: FileFormatConfig,
        storage_config: StorageConfig,
    ) -> ScanOperatorHandle: ...
    @staticmethod
    def glob_scan(
        glob_path: list[str],
        file_format_config: FileFormatConfig,
        storage_config: StorageConfig,
        schema_hint: PySchema | None = None,
    ) -> ScanOperatorHandle: ...
    @staticmethod
    def from_python_scan_operator(operator: ScanOperator) -> ScanOperatorHandle: ...

class PartitionField:
    """
    Partitioning Field of a Scan Source such as Hive or Iceberg
    """

    field: PyField

    def __init__(
        self,
        field: PyField,
        source_field: PyField | None = None,
        transform: PartitionTransform | None = None,
    ) -> None: ...

class PartitionTransform:
    """
    Partitioning Transform from a Data Catalog source field to a Partitioning Columns
    """

    @staticmethod
    def identity() -> PartitionTransform: ...
    @staticmethod
    def year() -> PartitionTransform: ...
    @staticmethod
    def month() -> PartitionTransform: ...
    @staticmethod
    def day() -> PartitionTransform: ...
    @staticmethod
    def hour() -> PartitionTransform: ...
    @staticmethod
    def iceberg_bucket(n: int) -> PartitionTransform: ...
    @staticmethod
    def iceberg_truncate(w: int) -> PartitionTransform: ...

class Pushdowns:
    """
    Pushdowns from the query optimizer that can optimize scanning data sources.
    """

    columns: list[str] | None
    filters: PyExpr | None
    partition_filters: PyExpr | None
    limit: int | None

    def filter_required_column_names(self) -> list[str]:
        """List of field names that are required by the filter predicate."""
        ...

def read_parquet(
    uri: str,
    columns: list[str] | None = None,
    start_offset: int | None = None,
    num_rows: int | None = None,
    row_groups: list[int] | None = None,
    predicate: PyExpr | None = None,
    io_config: IOConfig | None = None,
    multithreaded_io: bool | None = None,
    coerce_int96_timestamp_unit: PyTimeUnit | None = None,
): ...
def read_parquet_bulk(
    uris: list[str],
    columns: list[str] | None = None,
    start_offset: int | None = None,
    num_rows: int | None = None,
    row_groups: list[list[int] | None] | None = None,
    predicate: PyExpr | None = None,
    io_config: IOConfig | None = None,
    num_parallel_tasks: int | None = 128,
    multithreaded_io: bool | None = None,
    coerce_int96_timestamp_unit: PyTimeUnit | None = None,
): ...
def read_parquet_statistics(
    uris: PySeries,
    io_config: IOConfig | None = None,
    multithreaded_io: bool | None = None,
): ...
def read_parquet_into_pyarrow(
    uri: str,
    columns: list[str] | None = None,
    start_offset: int | None = None,
    num_rows: int | None = None,
    row_groups: list[int] | None = None,
    io_config: IOConfig | None = None,
    multithreaded_io: bool | None = None,
    coerce_int96_timestamp_unit: PyTimeUnit | None = None,
): ...
def read_parquet_into_pyarrow_bulk(
    uris: list[str],
    columns: list[str] | None = None,
    start_offset: int | None = None,
    num_rows: int | None = None,
    row_groups: list[list[int] | None] | None = None,
    io_config: IOConfig | None = None,
    num_parallel_tasks: int | None = 128,
    multithreaded_io: bool | None = None,
    coerce_int96_timestamp_unit: PyTimeUnit | None = None,
): ...
def read_parquet_schema(
    uri: str,
    io_config: IOConfig | None = None,
    multithreaded_io: bool | None = None,
    coerce_int96_timestamp_unit: PyTimeUnit | None = None,
): ...
def read_csv(
    uri: str,
    convert_options: CsvConvertOptions | None = None,
    parse_options: CsvParseOptions | None = None,
    read_options: CsvReadOptions | None = None,
    io_config: IOConfig | None = None,
    multithreaded_io: bool | None = None,
): ...
def read_csv_schema(
    uri: str,
    parse_options: CsvParseOptions | None = None,
    io_config: IOConfig | None = None,
    multithreaded_io: bool | None = None,
): ...
def read_json(
    uri: str,
    convert_options: JsonConvertOptions | None = None,
    parse_options: JsonParseOptions | None = None,
    read_options: JsonReadOptions | None = None,
    io_config: IOConfig | None = None,
    multithreaded_io: bool | None = None,
    max_chunks_in_flight: int | None = None,
): ...
def read_json_schema(
    uri: str,
    parse_options: JsonParseOptions | None = None,
    io_config: IOConfig | None = None,
    multithreaded_io: bool | None = None,
): ...

class PyTimeUnit:
    @staticmethod
    def nanoseconds() -> PyTimeUnit: ...
    @staticmethod
    def microseconds() -> PyTimeUnit: ...
    @staticmethod
    def milliseconds() -> PyTimeUnit: ...
    @staticmethod
    def seconds() -> PyTimeUnit: ...

class PyDataType:
    @staticmethod
    def null() -> PyDataType: ...
    @staticmethod
    def bool() -> PyDataType: ...
    @staticmethod
    def int8() -> PyDataType: ...
    @staticmethod
    def int16() -> PyDataType: ...
    @staticmethod
    def int32() -> PyDataType: ...
    @staticmethod
    def int64() -> PyDataType: ...
    @staticmethod
    def uint8() -> PyDataType: ...
    @staticmethod
    def uint16() -> PyDataType: ...
    @staticmethod
    def uint32() -> PyDataType: ...
    @staticmethod
    def uint64() -> PyDataType: ...
    @staticmethod
    def float32() -> PyDataType: ...
    @staticmethod
    def float64() -> PyDataType: ...
    @staticmethod
    def binary() -> PyDataType: ...
    @staticmethod
    def string() -> PyDataType: ...
    @staticmethod
    def decimal128(precision: int, size: int) -> PyDataType: ...
    @staticmethod
    def date() -> PyDataType: ...
    @staticmethod
    def time(time_unit: PyTimeUnit) -> PyDataType: ...
    @staticmethod
    def timestamp(time_unit: PyTimeUnit, timezone: str | None = None) -> PyDataType: ...
    @staticmethod
    def duration(time_unit: PyTimeUnit) -> PyDataType: ...
    @staticmethod
    def list(data_type: PyDataType) -> PyDataType: ...
    @staticmethod
    def fixed_size_list(data_type: PyDataType, size: int) -> PyDataType: ...
    @staticmethod
    def map(key_type: PyDataType, value_type: PyDataType) -> PyDataType: ...
    @staticmethod
    def struct(fields: dict[str, PyDataType]) -> PyDataType: ...
    @staticmethod
    def extension(
        name: str, storage_data_type: PyDataType, metadata: str | None = None
    ) -> PyDataType: ...
    @staticmethod
    def embedding(data_type: PyDataType, size: int) -> PyDataType: ...
    @staticmethod
    def image(
        mode: ImageMode | None = None,
        height: int | None = None,
        width: int | None = None,
    ) -> PyDataType: ...
    @staticmethod
    def tensor(
        dtype: PyDataType, shape: tuple[int, ...] | None = None
    ) -> PyDataType: ...
    @staticmethod
    def python() -> PyDataType: ...
    def to_arrow(
        self, cast_tensor_type_for_ray: builtins.bool | None = None
    ) -> pyarrow.DataType: ...
    def is_numeric(self) -> builtins.bool: ...
    def is_image(self) -> builtins.bool: ...
    def is_fixed_shape_image(self) -> builtins.bool: ...
    def is_tensor(self) -> builtins.bool: ...
    def is_fixed_shape_tensor(self) -> builtins.bool: ...
    def is_map(self) -> builtins.bool: ...
    def is_logical(self) -> builtins.bool: ...
    def is_temporal(self) -> builtins.bool: ...
    def is_equal(self, other: Any) -> builtins.bool: ...
    @staticmethod
    def from_json(serialized: str) -> PyDataType: ...
    def __reduce__(self) -> tuple: ...
    def __hash__(self) -> int: ...

class PyField:
    def name(self) -> str: ...
    @staticmethod
    def create(name: str, datatype: PyDataType) -> PyField: ...
    def dtype(self) -> PyDataType: ...
    def eq(self, other: PyField) -> bool: ...
    def __reduce__(self) -> tuple: ...

class PySchema:
    def __getitem__(self, name: str) -> PyField: ...
    def names(self) -> list[str]: ...
    def union(self, other: PySchema) -> PySchema: ...
    def eq(self, other: PySchema) -> bool: ...
    def estimate_row_size_bytes(self) -> float: ...
    @staticmethod
    def from_field_name_and_types(
        names_and_types: list[tuple[str, PyDataType]]
    ) -> PySchema: ...
    @staticmethod
    def from_fields(fields: list[PyField]) -> PySchema: ...
    def __reduce__(self) -> tuple: ...
    def __repr__(self) -> str: ...
    def _repr_html_(self) -> str: ...
    def _truncated_table_string(self) -> str: ...

class PyExpr:
    def alias(self, name: str) -> PyExpr: ...
    def cast(self, dtype: PyDataType) -> PyExpr: ...
    def ceil(self) -> PyExpr: ...
    def floor(self) -> PyExpr: ...
    def sign(self) -> PyExpr: ...
    def round(self, decimal: int) -> PyExpr: ...
    def sqrt(self) -> PyExpr: ...
    def sin(self) -> PyExpr: ...
    def cos(self) -> PyExpr: ...
    def tan(self) -> PyExpr: ...
    def cot(self) -> PyExpr: ...
    def arcsin(self) -> PyExpr: ...
    def arccos(self) -> PyExpr: ...
    def arctan(self) -> PyExpr: ...
    def degrees(self) -> PyExpr: ...
    def radians(self) -> PyExpr: ...
    def log2(self) -> PyExpr: ...
    def log10(self) -> PyExpr: ...
    def ln(self) -> PyExpr: ...
    def exp(self) -> PyExpr: ...
    def if_else(self, if_true: PyExpr, if_false: PyExpr) -> PyExpr: ...
    def count(self, mode: CountMode) -> PyExpr: ...
    def sum(self) -> PyExpr: ...
    def approx_percentiles(self, percentiles: float | list[float]) -> PyExpr: ...
    def mean(self) -> PyExpr: ...
    def min(self) -> PyExpr: ...
    def max(self) -> PyExpr: ...
    def any_value(self, ignore_nulls: bool) -> PyExpr: ...
    def agg_list(self) -> PyExpr: ...
    def agg_concat(self) -> PyExpr: ...
    def explode(self) -> PyExpr: ...
    def __abs__(self) -> PyExpr: ...
    def __add__(self, other: PyExpr) -> PyExpr: ...
    def __sub__(self, other: PyExpr) -> PyExpr: ...
    def __mul__(self, other: PyExpr) -> PyExpr: ...
    def __floordiv__(self, other: PyExpr) -> PyExpr: ...
    def __truediv__(self, other: PyExpr) -> PyExpr: ...
    def __mod__(self, other: PyExpr) -> PyExpr: ...
    def __and__(self, other: PyExpr) -> PyExpr: ...
    def __or__(self, other: PyExpr) -> PyExpr: ...
    def __xor__(self, other: PyExpr) -> PyExpr: ...
    def __invert__(self) -> PyExpr: ...
    def __lt__(self, other: PyExpr) -> PyExpr: ...
    def __le__(self, other: PyExpr) -> PyExpr: ...
    def __gt__(self, other: PyExpr) -> PyExpr: ...
    def __ge__(self, other: PyExpr) -> PyExpr: ...
    def __eq__(self, other: PyExpr) -> PyExpr: ...  # type: ignore[override]
    def __ne__(self, other: PyExpr) -> PyExpr: ...  # type: ignore[override]
    def is_null(self) -> PyExpr: ...
    def not_null(self) -> PyExpr: ...
    def fill_null(self, fill_value: PyExpr) -> PyExpr: ...
    def is_in(self, other: PyExpr) -> PyExpr: ...
    def name(self) -> str: ...
    def to_field(self, schema: PySchema) -> PyField: ...
    def to_sql(self) -> str: ...
    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __reduce__(self) -> tuple: ...
    def is_nan(self) -> PyExpr: ...
    def dt_date(self) -> PyExpr: ...
    def dt_day(self) -> PyExpr: ...
    def dt_hour(self) -> PyExpr: ...
    def dt_month(self) -> PyExpr: ...
    def dt_year(self) -> PyExpr: ...
    def dt_day_of_week(self) -> PyExpr: ...
    def dt_truncate(self, interval: str, relative_to: PyExpr) -> PyExpr: ...
    def utf8_endswith(self, pattern: PyExpr) -> PyExpr: ...
    def utf8_startswith(self, pattern: PyExpr) -> PyExpr: ...
    def utf8_contains(self, pattern: PyExpr) -> PyExpr: ...
    def utf8_match(self, pattern: PyExpr) -> PyExpr: ...
    def utf8_split(self, pattern: PyExpr, regex: bool) -> PyExpr: ...
    def utf8_extract(self, pattern: PyExpr, index: int) -> PyExpr: ...
    def utf8_extract_all(self, pattern: PyExpr, index: int) -> PyExpr: ...
    def utf8_replace(
        self, pattern: PyExpr, replacement: PyExpr, regex: bool
    ) -> PyExpr: ...
    def utf8_length(self) -> PyExpr: ...
    def utf8_lower(self) -> PyExpr: ...
    def utf8_upper(self) -> PyExpr: ...
    def utf8_lstrip(self) -> PyExpr: ...
    def utf8_rstrip(self) -> PyExpr: ...
    def utf8_reverse(self) -> PyExpr: ...
    def utf8_capitalize(self) -> PyExpr: ...
    def utf8_left(self, nchars: PyExpr) -> PyExpr: ...
    def utf8_right(self, nchars: PyExpr) -> PyExpr: ...
    def utf8_find(self, substr: PyExpr) -> PyExpr: ...
    def utf8_rpad(self, length: PyExpr, pad: PyExpr) -> PyExpr: ...
    def utf8_lpad(self, length: PyExpr, pad: PyExpr) -> PyExpr: ...
    def utf8_repeat(self, n: PyExpr) -> PyExpr: ...
    def image_decode(self, raise_error_on_failure: bool) -> PyExpr: ...
    def image_encode(self, image_format: ImageFormat) -> PyExpr: ...
    def image_resize(self, w: int, h: int) -> PyExpr: ...
    def image_crop(self, bbox: PyExpr) -> PyExpr: ...
    def list_join(self, delimiter: PyExpr) -> PyExpr: ...
    def list_count(self, mode: CountMode) -> PyExpr: ...
    def list_get(self, idx: PyExpr, default: PyExpr) -> PyExpr: ...
    def list_sum(self) -> PyExpr: ...
    def list_mean(self) -> PyExpr: ...
    def list_min(self) -> PyExpr: ...
    def list_max(self) -> PyExpr: ...
    def struct_get(self, name: str) -> PyExpr: ...
    def url_download(
        self,
        max_connections: int,
        raise_error_on_failure: bool,
        multi_thread: bool,
        config: IOConfig,
    ) -> PyExpr: ...
    def partitioning_days(self) -> PyExpr: ...
    def partitioning_hours(self) -> PyExpr: ...
    def partitioning_months(self) -> PyExpr: ...
    def partitioning_years(self) -> PyExpr: ...
    def partitioning_iceberg_bucket(self, n: int) -> PyExpr: ...
    def partitioning_iceberg_truncate(self, w: int) -> PyExpr: ...
    def json_query(self, query: str) -> PyExpr: ...

    ###
    # Helper methods required by optimizer:
    # These should be removed from the Python API for Expressions when logical plans and optimizer are migrated to Rust
    ###
    def _input_mapping(self) -> builtins.str | None: ...

def eq(expr1: PyExpr, expr2: PyExpr) -> bool: ...
def col(name: str) -> PyExpr: ...
def lit(item: Any) -> PyExpr: ...
def date_lit(item: int) -> PyExpr: ...
def time_lit(item: int, tu: PyTimeUnit) -> PyExpr: ...
def timestamp_lit(item: int, tu: PyTimeUnit, tz: str | None) -> PyExpr: ...
def decimal_lit(sign: bool, digits: tuple[int, ...], exp: int) -> PyExpr: ...
def series_lit(item: PySeries) -> PyExpr: ...
def udf(
    func: Callable, expressions: list[PyExpr], return_dtype: PyDataType
) -> PyExpr: ...

class PySeries:
    @staticmethod
    def from_arrow(name: str, pyarrow_array: pyarrow.Array) -> PySeries: ...
    @staticmethod
    def from_pylist(name: str, pylist: list[Any], pyobj: str) -> PySeries: ...
    def to_pylist(self) -> list[Any]: ...
    def to_arrow(self) -> pyarrow.Array: ...
    def __abs__(self) -> PySeries: ...
    def __add__(self, other: PySeries) -> PySeries: ...
    def __sub__(self, other: PySeries) -> PySeries: ...
    def __mul__(self, other: PySeries) -> PySeries: ...
    def __truediv__(self, other: PySeries) -> PySeries: ...
    def __mod__(self, other: PySeries) -> PySeries: ...
    def __and__(self, other: PySeries) -> PySeries: ...
    def __or__(self, other: PySeries) -> PySeries: ...
    def __xor__(self, other: PySeries) -> PySeries: ...
    def __lt__(self, other: PySeries) -> PySeries: ...
    def __le__(self, other: PySeries) -> PySeries: ...
    def __gt__(self, other: PySeries) -> PySeries: ...
    def __ge__(self, other: PySeries) -> PySeries: ...
    def __eq__(self, other: PySeries) -> PySeries: ...  # type: ignore[override]
    def __ne__(self, other: PySeries) -> PySeries: ...  # type: ignore[override]
    def take(self, idx: PySeries) -> PySeries: ...
    def slice(self, start: int, end: int) -> PySeries: ...
    def filter(self, mask: PySeries) -> PySeries: ...
    def sort(self, descending: bool) -> PySeries: ...
    def argsort(self, descending: bool) -> PySeries: ...
    def hash(self, seed: PySeries | None = None) -> PySeries: ...
    def __invert__(self) -> PySeries: ...
    def count(self, mode: CountMode) -> PySeries: ...
    def sum(self) -> PySeries: ...
    def mean(self) -> PySeries: ...
    def min(self) -> PySeries: ...
    def max(self) -> PySeries: ...
    def agg_list(self) -> PySeries: ...
    def cast(self, dtype: PyDataType) -> PySeries: ...
    def ceil(self) -> PySeries: ...
    def floor(self) -> PySeries: ...
    def sign(self) -> PySeries: ...
    def round(self, decimal: int) -> PySeries: ...
    def sqrt(self) -> PySeries: ...
    def sin(self) -> PySeries: ...
    def cos(self) -> PySeries: ...
    def tan(self) -> PySeries: ...
    def cot(self) -> PySeries: ...
    def arcsin(self) -> PySeries: ...
    def arccos(self) -> PySeries: ...
    def arctan(self) -> PySeries: ...
    def degrees(self) -> PySeries: ...
    def radians(self) -> PySeries: ...
    def log2(self) -> PySeries: ...
    def log10(self) -> PySeries: ...
    def ln(self) -> PySeries: ...
    def exp(self) -> PySeries: ...
    @staticmethod
    def concat(series: list[PySeries]) -> PySeries: ...
    def __len__(self) -> int: ...
    def size_bytes(self) -> int: ...
    def name(self) -> str: ...
    def rename(self, name: str) -> PySeries: ...
    def data_type(self) -> PyDataType: ...
    def utf8_endswith(self, pattern: PySeries) -> PySeries: ...
    def utf8_startswith(self, pattern: PySeries) -> PySeries: ...
    def utf8_contains(self, pattern: PySeries) -> PySeries: ...
    def utf8_match(self, pattern: PySeries) -> PySeries: ...
    def utf8_split(self, pattern: PySeries, regex: bool) -> PySeries: ...
    def utf8_extract(self, pattern: PySeries, index: int) -> PySeries: ...
    def utf8_extract_all(self, pattern: PySeries, index: int) -> PySeries: ...
    def utf8_replace(
        self, pattern: PySeries, replacement: PySeries, regex: bool
    ) -> PySeries: ...
    def utf8_length(self) -> PySeries: ...
    def utf8_lower(self) -> PySeries: ...
    def utf8_upper(self) -> PySeries: ...
    def utf8_lstrip(self) -> PySeries: ...
    def utf8_rstrip(self) -> PySeries: ...
    def utf8_reverse(self) -> PySeries: ...
    def utf8_capitalize(self) -> PySeries: ...
    def utf8_left(self, nchars: PySeries) -> PySeries: ...
    def utf8_right(self, nchars: PySeries) -> PySeries: ...
    def utf8_find(self, substr: PySeries) -> PySeries: ...
    def utf8_rpad(self, length: PySeries, pad: PySeries) -> PySeries: ...
    def utf8_lpad(self, length: PySeries, pad: PySeries) -> PySeries: ...
    def utf8_repeat(self, n: PySeries) -> PySeries: ...
    def is_nan(self) -> PySeries: ...
    def dt_date(self) -> PySeries: ...
    def dt_day(self) -> PySeries: ...
    def dt_hour(self) -> PySeries: ...
    def dt_month(self) -> PySeries: ...
    def dt_year(self) -> PySeries: ...
    def dt_day_of_week(self) -> PySeries: ...
    def dt_truncate(self, interval: str, relative_to: PySeries) -> PySeries: ...
    def partitioning_days(self) -> PySeries: ...
    def partitioning_hours(self) -> PySeries: ...
    def partitioning_months(self) -> PySeries: ...
    def partitioning_years(self) -> PySeries: ...
    def partitioning_iceberg_bucket(self, n: int) -> PySeries: ...
    def partitioning_iceberg_truncate(self, w: int) -> PySeries: ...
    def list_count(self, mode: CountMode) -> PySeries: ...
    def list_get(self, idx: PySeries, default: PySeries) -> PySeries: ...
    def image_decode(self, raise_error_on_failure: bool) -> PySeries: ...
    def image_encode(self, image_format: ImageFormat) -> PySeries: ...
    def image_resize(self, w: int, h: int) -> PySeries: ...
    def if_else(self, other: PySeries, predicate: PySeries) -> PySeries: ...
    def is_null(self) -> PySeries: ...
    def not_null(self) -> PySeries: ...
    def fill_null(self, fill_value: PySeries) -> PySeries: ...
    def murmur3_32(self) -> PySeries: ...
    def to_str_values(self) -> PySeries: ...
    def _debug_bincode_serialize(self) -> bytes: ...
    @staticmethod
    def _debug_bincode_deserialize(b: bytes) -> PySeries: ...

class PyTable:
    def schema(self) -> PySchema: ...
    def cast_to_schema(self, schema: PySchema) -> PyTable: ...
    def eval_expression_list(self, exprs: list[PyExpr]) -> PyTable: ...
    def take(self, idx: PySeries) -> PyTable: ...
    def filter(self, exprs: list[PyExpr]) -> PyTable: ...
    def sort(self, sort_keys: list[PyExpr], descending: list[bool]) -> PyTable: ...
    def argsort(self, sort_keys: list[PyExpr], descending: list[bool]) -> PySeries: ...
    def agg(self, to_agg: list[PyExpr], group_by: list[PyExpr]) -> PyTable: ...
    def pivot(
        self,
        group_by: list[PyExpr],
        pivot_column: PyExpr,
        values_column: PyExpr,
        names: list[str],
    ) -> PyTable: ...
    def hash_join(
        self,
        right: PyTable,
        left_on: list[PyExpr],
        right_on: list[PyExpr],
        how: JoinType,
    ) -> PyTable: ...
    def sort_merge_join(
        self,
        right: PyTable,
        left_on: list[PyExpr],
        right_on: list[PyExpr],
        is_sorted: bool,
    ) -> PyTable: ...
    def explode(self, to_explode: list[PyExpr]) -> PyTable: ...
    def head(self, num: int) -> PyTable: ...
    def sample_by_fraction(
        self, fraction: float, with_replacement: bool, seed: int | None
    ) -> PyTable: ...
    def sample_by_size(
        self, size: int, with_replacement: bool, seed: int | None
    ) -> PyTable: ...
    def quantiles(self, num: int) -> PyTable: ...
    def partition_by_hash(
        self, exprs: list[PyExpr], num_partitions: int
    ) -> list[PyTable]: ...
    def partition_by_random(self, num_partitions: int, seed: int) -> list[PyTable]: ...
    def partition_by_range(
        self, partition_keys: list[PyExpr], boundaries: PyTable, descending: list[bool]
    ) -> list[PyTable]: ...
    def partition_by_value(
        self, partition_keys: list[PyExpr]
    ) -> tuple[list[PyTable], PyTable]: ...
    def add_monotonically_increasing_id(
        self, partition_num: int, column_name: str
    ) -> PyTable: ...
    def __repr__(self) -> str: ...
    def _repr_html_(self) -> str: ...
    def __len__(self) -> int: ...
    def size_bytes(self) -> int: ...
    def column_names(self) -> list[str]: ...
    def get_column(self, name: str) -> PySeries: ...
    def get_column_by_index(self, idx: int) -> PySeries: ...
    @staticmethod
    def concat(tables: list[PyTable]) -> PyTable: ...
    def slice(self, start: int, end: int) -> PyTable: ...
    @staticmethod
    def from_arrow_record_batches(
        record_batches: list[pyarrow.RecordBatch], schema: PySchema
    ) -> PyTable: ...
    @staticmethod
    def from_pylist_series(dict: dict[str, PySeries]) -> PyTable: ...
    def to_arrow_record_batch(self) -> pyarrow.RecordBatch: ...
    @staticmethod
    def empty(schema: PySchema | None = None) -> PyTable: ...

class PyMicroPartition:
    def schema(self) -> PySchema: ...
    def column_names(self) -> list[str]: ...
    def get_column(self, name: str) -> PySeries: ...
    def size_bytes(self) -> int | None: ...
    def _repr_html_(self) -> str: ...
    @staticmethod
    def empty(schema: PySchema | None = None) -> PyMicroPartition: ...
    @staticmethod
    def from_scan_task(scan_task: ScanTask) -> PyMicroPartition: ...
    @staticmethod
    def from_tables(tables: list[PyTable]) -> PyMicroPartition: ...
    @staticmethod
    def from_arrow_record_batches(
        record_batches: list[pyarrow.RecordBatch], schema: PySchema
    ) -> PyMicroPartition: ...
    @staticmethod
    def concat(tables: list[PyMicroPartition]) -> PyMicroPartition: ...
    def slice(self, start: int, end: int) -> PyMicroPartition: ...
    def to_table(self) -> PyTable: ...
    def cast_to_schema(self, schema: PySchema) -> PyMicroPartition: ...
    def eval_expression_list(self, exprs: list[PyExpr]) -> PyMicroPartition: ...
    def take(self, idx: PySeries) -> PyMicroPartition: ...
    def filter(self, exprs: list[PyExpr]) -> PyMicroPartition: ...
    def sort(
        self, sort_keys: list[PyExpr], descending: list[bool]
    ) -> PyMicroPartition: ...
    def argsort(self, sort_keys: list[PyExpr], descending: list[bool]) -> PySeries: ...
    def agg(self, to_agg: list[PyExpr], group_by: list[PyExpr]) -> PyMicroPartition: ...
    def hash_join(
        self,
        right: PyMicroPartition,
        left_on: list[PyExpr],
        right_on: list[PyExpr],
        how: JoinType,
    ) -> PyMicroPartition: ...
    def pivot(
        self,
        group_by: list[PyExpr],
        pivot_column: PyExpr,
        values_column: PyExpr,
        names: list[str],
    ) -> PyMicroPartition: ...
    def sort_merge_join(
        self,
        right: PyMicroPartition,
        left_on: list[PyExpr],
        right_on: list[PyExpr],
        is_sorted: bool,
    ) -> PyMicroPartition: ...
    def explode(self, to_explode: list[PyExpr]) -> PyMicroPartition: ...
    def unpivot(
        self,
        ids: list[PyExpr],
        values: list[PyExpr],
        variable_name: str,
        value_name: str,
    ) -> PyMicroPartition: ...
    def head(self, num: int) -> PyMicroPartition: ...
    def sample_by_fraction(
        self, fraction: float, with_replacement: bool, seed: int | None
    ) -> PyMicroPartition: ...
    def sample_by_size(
        self, size: int, with_replacement: bool, seed: int | None
    ) -> PyMicroPartition: ...
    def quantiles(self, num: int) -> PyMicroPartition: ...
    def partition_by_hash(
        self, exprs: list[PyExpr], num_partitions: int
    ) -> list[PyMicroPartition]: ...
    def partition_by_random(
        self, num_partitions: int, seed: int
    ) -> list[PyMicroPartition]: ...
    def partition_by_range(
        self, partition_keys: list[PyExpr], boundaries: PyTable, descending: list[bool]
    ) -> list[PyMicroPartition]: ...
    def partition_by_value(
        self, exprs: list[PyExpr]
    ) -> tuple[list[PyMicroPartition], PyMicroPartition]: ...
    def add_monotonically_increasing_id(
        self, partition_num: int, column_name: str
    ) -> PyMicroPartition: ...
    def __repr__(self) -> str: ...
    def __len__(self) -> int: ...
    @classmethod
    def read_parquet(
        cls,
        path: str,
        columns: list[str] | None = None,
        start_offset: int | None = None,
        num_rows: int | None = None,
        row_groups: list[int] | None = None,
        predicate: PyExpr | None = None,
        io_config: IOConfig | None = None,
        multithreaded_io: bool | None = None,
        coerce_int96_timestamp_unit: PyTimeUnit = PyTimeUnit.nanoseconds(),
    ): ...
    @classmethod
    def read_parquet_bulk(
        cls,
        uris: list[str],
        columns: list[str] | None = None,
        start_offset: int | None = None,
        num_rows: int | None = None,
        row_groups: list[list[int] | None] | None = None,
        predicate: PyExpr | None = None,
        io_config: IOConfig | None = None,
        num_parallel_tasks: int | None = None,
        multithreaded_io: bool | None = None,
        coerce_int96_timestamp_unit: PyTimeUnit | None = None,
    ): ...
    @classmethod
    def read_csv(
        cls,
        uri: str,
        convert_options: CsvConvertOptions | None = None,
        parse_options: CsvParseOptions | None = None,
        read_options: CsvReadOptions | None = None,
        io_config: IOConfig | None = None,
        multithreaded_io: bool | None = None,
    ): ...
    @classmethod
    def read_json_native(
        cls,
        uri: str,
        convert_options: JsonConvertOptions | None = None,
        parse_options: JsonParseOptions | None = None,
        read_options: JsonReadOptions | None = None,
        io_config: IOConfig | None = None,
        multithreaded_io: bool | None = None,
    ): ...

class PhysicalPlanScheduler:
    """
    A work scheduler for physical query plans.
    """

    def num_partitions(self) -> int: ...
    def repr_ascii(self, simple: bool) -> str: ...
    def to_partition_tasks(
        self, psets: dict[str, list[PartitionT]]
    ) -> physical_plan.InProgressPhysicalPlan: ...
    def run(self, psets: dict[str, list[PartitionT]]) -> Iterator[PyMicroPartition]: ...

class AdaptivePhysicalPlanScheduler:
    """
    An adaptive Physical Plan Scheduler.
    """

    def next(self) -> tuple[int | None, PhysicalPlanScheduler]: ...
    def is_done(self) -> bool: ...
    # Todo use in memory info here instead
    def update(
        self,
        source_id: int,
        partition_key: str,
        cache_entry: PartitionCacheEntry,
        num_partitions: int,
        size_bytes: int,
        num_rows: int,
    ) -> None: ...

class LogicalPlanBuilder:
    """
    A logical plan builder, which simplifies constructing logical plans via
    a fluent interface. E.g., LogicalPlanBuilder.table_scan(..).project(..).filter(..).

    This builder holds the current root (sink) of the logical plan, and the building methods return
    a brand new builder holding a new plan; i.e., this is an immutable builder.
    """

    @staticmethod
    def in_memory_scan(
        partition_key: str,
        cache_entry: PartitionCacheEntry,
        schema: PySchema,
        num_partitions: int,
        size_bytes: int,
        num_rows: int,
    ) -> LogicalPlanBuilder: ...
    @staticmethod
    def table_scan(scan_operator: ScanOperatorHandle) -> LogicalPlanBuilder: ...
    def select(self, to_select: list[PyExpr]) -> LogicalPlanBuilder: ...
    def with_columns(
        self, columns: list[PyExpr], resource_request: ResourceRequest
    ) -> LogicalPlanBuilder: ...
    def exclude(self, to_exclude: list[str]) -> LogicalPlanBuilder: ...
    def filter(self, predicate: PyExpr) -> LogicalPlanBuilder: ...
    def limit(self, limit: int, eager: bool) -> LogicalPlanBuilder: ...
    def explode(self, to_explode: list[PyExpr]) -> LogicalPlanBuilder: ...
    def unpivot(
        self,
        ids: list[PyExpr],
        values: list[PyExpr],
        variable_name: str,
        value_name: str,
    ) -> LogicalPlanBuilder: ...
    def sort(
        self, sort_by: list[PyExpr], descending: list[bool]
    ) -> LogicalPlanBuilder: ...
    def hash_repartition(
        self,
        partition_by: list[PyExpr],
        num_partitions: int | None,
    ) -> LogicalPlanBuilder: ...
    def random_shuffle(self, num_partitions: int | None) -> LogicalPlanBuilder: ...
    def into_partitions(self, num_partitions: int) -> LogicalPlanBuilder: ...
    def coalesce(self, num_partitions: int) -> LogicalPlanBuilder: ...
    def distinct(self) -> LogicalPlanBuilder: ...
    def sample(
        self, fraction: float, with_replacement: bool, seed: int | None
    ) -> LogicalPlanBuilder: ...
    def aggregate(
        self, agg_exprs: list[PyExpr], groupby_exprs: list[PyExpr]
    ) -> LogicalPlanBuilder: ...
    def pivot(
        self,
        groupby_exprs: list[PyExpr],
        pivot_expr: PyExpr,
        values_expr: PyExpr,
        agg_expr: PyExpr,
        names: list[str],
    ) -> LogicalPlanBuilder: ...
    def join(
        self,
        right: LogicalPlanBuilder,
        left_on: list[PyExpr],
        right_on: list[PyExpr],
        join_type: JoinType,
        strategy: JoinStrategy | None = None,
    ) -> LogicalPlanBuilder: ...
    def concat(self, other: LogicalPlanBuilder) -> LogicalPlanBuilder: ...
    def add_monotonically_increasing_id(
        self, column_name: str | None
    ) -> LogicalPlanBuilder: ...
    def table_write(
        self,
        root_dir: str,
        file_format: FileFormat,
        partition_cols: list[PyExpr] | None = None,
        compression: str | None = None,
        io_config: IOConfig | None = None,
    ) -> LogicalPlanBuilder: ...
    def iceberg_write(
        self,
        table_name: str,
        table_location: str,
        spec_id: int,
        iceberg_schema: IcebergSchema,
        iceberg_properties: IcebergTableProperties,
        catalog_columns: list[str],
        io_config: IOConfig | None = None,
    ) -> LogicalPlanBuilder: ...
    def schema(self) -> PySchema: ...
    def optimize(self) -> LogicalPlanBuilder: ...
    def to_physical_plan_scheduler(
        self, cfg: PyDaftExecutionConfig
    ) -> PhysicalPlanScheduler: ...
    def to_adaptive_physical_plan_scheduler(
        self, cfg: PyDaftExecutionConfig
    ) -> AdaptivePhysicalPlanScheduler: ...
    def repr_ascii(self, simple: bool) -> str: ...

class PyDaftExecutionConfig:
    @staticmethod
    def from_env() -> PyDaftExecutionConfig: ...
    def with_config_values(
        self,
        scan_tasks_min_size_bytes: int | None = None,
        scan_tasks_max_size_bytes: int | None = None,
        broadcast_join_size_bytes_threshold: int | None = None,
        parquet_split_row_groups_max_files: int | None = None,
        sort_merge_join_sort_with_aligned_boundaries: bool | None = None,
        sample_size_for_sort: int | None = None,
        num_preview_rows: int | None = None,
        parquet_target_filesize: int | None = None,
        parquet_target_row_group_size: int | None = None,
        parquet_inflation_factor: float | None = None,
        csv_target_filesize: int | None = None,
        csv_inflation_factor: float | None = None,
        shuffle_aggregation_default_partitions: int | None = None,
        read_sql_partition_size_bytes: int | None = None,
        enable_aqe: bool | None = None,
        enable_new_executor: bool | None = None,
    ) -> PyDaftExecutionConfig: ...
    @property
    def scan_tasks_min_size_bytes(self) -> int: ...
    @property
    def scan_tasks_max_size_bytes(self) -> int: ...
    @property
    def broadcast_join_size_bytes_threshold(self) -> int: ...
    @property
    def sort_merge_join_sort_with_aligned_boundaries(self) -> bool: ...
    @property
    def sample_size_for_sort(self) -> int: ...
    @property
    def num_preview_rows(self) -> int: ...
    @property
    def parquet_target_filesize(self) -> int: ...
    @property
    def parquet_target_row_group_size(self) -> int: ...
    @property
    def parquet_inflation_factor(self) -> float: ...
    @property
    def csv_target_filesize(self) -> int: ...
    @property
    def csv_inflation_factor(self) -> float: ...
    @property
    def shuffle_aggregation_default_partitions(self) -> int: ...
    @property
    def read_sql_partition_size_bytes(self) -> int: ...
    @property
    def enable_aqe(self) -> bool: ...
    @property
    def enable_new_executor(self) -> bool: ...

class PyDaftPlanningConfig:
    def with_config_values(
        self,
        default_io_config: IOConfig | None = None,
    ) -> PyDaftPlanningConfig: ...
    @property
    def default_io_config(self) -> IOConfig: ...

def build_type() -> str: ...
def version() -> str: ...
def __getattr__(name) -> Any: ...
def io_glob(
    path: str,
    multithreaded_io: bool | None = None,
    io_config: IOConfig | None = None,
    fanout_limit: int | None = None,
    page_size: int | None = None,
    limit: int | None = None,
) -> list[dict]: ...

class SystemInfo:
    """
    Accessor for system information.
    """

    def __init__(self) -> None: ...
    def total_memory(self) -> int: ...
    def cpu_count(self) -> int | None: ...
