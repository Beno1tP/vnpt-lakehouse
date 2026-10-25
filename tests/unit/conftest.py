"""Unit tests run without Spark: CI installs only the job's pure-Python deps (docker/spark/requirements.txt).
bronze_ingest imports pyspark at module level, so a minimal stub stands in when pyspark is not installed."""
import sys
import types

if "pyspark" not in sys.modules:
    try:
        import pyspark  # noqa: F401
    except ImportError:
        stub = types.ModuleType("pyspark")
        sql = types.ModuleType("pyspark.sql")
        sql.SparkSession = object
        sql.functions = types.ModuleType("pyspark.sql.functions")
        sql_types = types.ModuleType("pyspark.sql.types")
        for name in ("StringType", "StructField", "StructType"):
            setattr(sql_types, name, object)
        stub.sql = sql
        sys.modules.update({"pyspark": stub, "pyspark.sql": sql,
                            "pyspark.sql.functions": sql.functions, "pyspark.sql.types": sql_types})
