"""
_pyspark_worker.py
Subprocess entry point for pyspark_runner. Do not import it.

    python _pyspark_worker.py TMP_DIR NAME [NAME ...]

For each NAME it reads NAME.in.parquet, runs candidate.py with df bound to a
Spark DataFrame of that table, and writes the resulting df to NAME.out.parquet.
On any failure it writes a one-line error.txt and stops.
"""

import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T


def spark_schema(table: pd.DataFrame) -> T.StructType:
    """Explicit schema, so columns that happen to be all-null in a small table still get a type."""
    def spark_type(dtype):
        if pd.api.types.is_bool_dtype(dtype):
            return T.BooleanType()
        if pd.api.types.is_integer_dtype(dtype):
            return T.LongType()
        if pd.api.types.is_float_dtype(dtype):
            return T.DoubleType()
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return T.TimestampType()
        return T.StringType()
    return T.StructType([T.StructField(c, spark_type(t), True) for c, t in table.dtypes.items()])


def main() -> None:
    tmp, names = Path(sys.argv[1]), sys.argv[2:]
    spark = (SparkSession.builder.master("local[1]").appName("dqeval-candidate")
             .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("OFF")
    code = compile((tmp / "candidate.py").read_text(), "candidate", "exec")
    try:
        for name in names:
            table = pd.read_parquet(tmp / f"{name}.in.parquet")
            rows  = table.astype(object).where(table.notna(), None).values.tolist()
            stamp = [i for i, c in enumerate(table.columns) if pd.api.types.is_datetime64_any_dtype(table[c])]
            for row in rows:                           # Spark wants datetime.datetime, not pandas Timestamp
                for i in stamp:
                    if row[i] is not None:
                        row[i] = row[i].to_pydatetime()
            ns = {"df": spark.createDataFrame(rows, schema=spark_schema(table))}
            exec(code, ns)
            out = ns.get("df")
            if not isinstance(out, DataFrame):
                raise TypeError("the code did not leave a Spark DataFrame in df")
            out.toPandas().to_parquet(tmp / f"{name}.out.parquet", index=False)
    except Exception as e:
        message = str(e).strip().splitlines()[0] if str(e).strip() else ""
        (tmp / "error.txt").write_text(f"{type(e).__name__}: {message}"[:500])
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
