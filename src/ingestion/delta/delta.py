import pandas as pd
from ingestion.spark import spark
from pyspark.sql import SparkSession

DEFAULT_FORMAT = "delta"


def write(
    data: pd.DataFrame,
    path: str,
    spark_session: SparkSession | None = None,
    *,
    format: str = DEFAULT_FORMAT,
    mode: str = "append",
    show: bool = False,
):
    if spark_session is None:
        spark_session = spark.create_spark_session()

    dataframe = spark_session.createDataFrame(data)

    if show:
        dataframe.show(truncate=False)

    dataframe.write.format(format).mode(mode).save(path)


def read(
    path: str,
    spark_session: SparkSession | None = None,
    *,
    format: str = DEFAULT_FORMAT,
) -> pd.DataFrame:
    if spark_session is None:
        spark_session = spark.create_spark_session()

    data = spark_session.read.format(format).load(path)
    return pd.DataFrame(data.toPandas())


def show(
    path: str,
    spark_session: SparkSession | None = None,
    *,
    format: str = DEFAULT_FORMAT,
):
    if spark_session is None:
        spark_session = spark.create_spark_session()

    spark_session.read.format(format).load(path).select("document_id", "unit_name").show()
