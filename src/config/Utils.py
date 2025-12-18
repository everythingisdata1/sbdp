from pyspark.sql import SparkSession

from src import ConfigLoader


def get_spark_session(env: str = "LOCAL") -> SparkSession:
    """
    Creates SparkSession with Log4j2 and Spark Event Logging enabled
    """

    builder = (
        SparkSession.builder
        .appName("Spark-Bulk-Data-Processing")
        .enableHiveSupport()
    )

    if env.upper() == "LOCAL":
        conf = ConfigLoader.get_spark_config(env=env)
        print(conf)
        builder = (
            builder
            .config(conf=conf)
            .master("local[2]")
            .config(
                "spark.driver.extraJavaOptions",
                "-Dlog4j.configurationFile=conf/log4j2.properties"
            )
            .config(
                "spark.executor.extraJavaOptions",
                "-Dlog4j.configurationFile=conf/log4j2.properties"
            )
            .config("spark.sql.autoBroadcastJoinThreshold", -1)
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "file:///D:/codes/py-spark/spark-bulk-data-processing/spark-events")
        )

    spark = builder.getOrCreate()

    return spark
