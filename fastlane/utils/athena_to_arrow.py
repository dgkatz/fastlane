from typing import List, Iterator, Dict, Any

import pyarrow as pa


def glue_table_to_pyarrow_schema(glue_table_details: dict) -> pa.Schema:
    athena_schema = _extract_dtypes_from_glue_table_details(glue_table_details=glue_table_details)
    return _athena_schema_to_pyarrow(athena_schema=athena_schema)


def _extract_dtypes_from_glue_table_details(glue_table_details: Dict[str, Any]) -> Dict[str, str]:
    dtypes: Dict[str, str] = {}
    for col in glue_table_details["Table"]["StorageDescriptor"]["Columns"]:
        dtypes[col["Name"]] = col["Type"]
    if "PartitionKeys" in glue_table_details["Table"]:
        for par in glue_table_details["Table"]["PartitionKeys"]:
            dtypes[par["Name"]] = par["Type"]
    return dtypes


def _athena_schema_to_pyarrow(athena_schema: Dict[str, str]) -> pa.Schema:
    schema = pa.Schema()
    for column, athena_type in athena_schema.items():
        schema.append(pa.field(name=column, type=_athena_dtype_to_pyarrow(athena_type)))
    return schema


def _athena_dtype_to_pyarrow(dtype: str) -> pa.DataType:
    """Athena to PyArrow data types conversion."""
    dtype = dtype.lower().replace(" ", "")
    if dtype == "tinyint":
        return pa.int8()
    if dtype == "smallint":
        return pa.int16()
    if dtype in ("int", "integer"):
        return pa.int32()
    if dtype == "bigint":
        return pa.int64()
    if dtype in ("float", "real"):
        return pa.float32()
    if dtype == "double":
        return pa.float64()
    if dtype == "boolean":
        return pa.bool_()
    if (dtype == "string") or dtype.startswith("char") or dtype.startswith("varchar"):
        return pa.string()
    if dtype == "timestamp":
        return pa.timestamp(unit="ns")
    if dtype == "date":
        return pa.date32()
    if dtype in ("binary" or "varbinary"):
        return pa.binary()
    if dtype.startswith("decimal") is True:
        precision, scale = dtype.replace("decimal(", "").replace(")", "").split(sep=",")
        return pa.decimal128(int(precision), int(scale))
    if dtype.startswith("array") is True:
        return pa.list_(_athena_dtype_to_pyarrow(dtype=dtype[6:-1]), -1)
    if dtype.startswith("struct") is True:
        return pa.struct(
            [(f.split(":", 1)[0], _athena_dtype_to_pyarrow(f.split(":", 1)[1])) for f in _split_struct(dtype[7:-1])])
    if dtype.startswith("map") is True:
        parts: List[str] = _split_map(s=dtype[4:-1])
        return pa.map_(_athena_dtype_to_pyarrow(parts[0]), _athena_dtype_to_pyarrow(parts[1]))
    raise Exception(f"Unsupported Athena type: {dtype}")


def _split_struct(s: str) -> List[str]:
    return list(_split_fields(s=s))


def _split_map(s: str) -> List[str]:
    parts: List[str] = list(_split_fields(s=s))
    if len(parts) != 2:
        raise RuntimeError(f"Invalid map fields: {s}")
    return parts


def _split_fields(s: str) -> Iterator[str]:
    counter: int = 0
    last: int = 0
    for i, x in enumerate(s):
        if x == "<":
            counter += 1
        elif x == ">":
            counter -= 1
        elif x == "," and counter == 0:
            yield s[last:i]
            last = i + 1
    yield s[last:]
