from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def create_spark_session(
    app_name: str = "ingestion",
    master_url: str = "local[*]",
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master_url)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session
