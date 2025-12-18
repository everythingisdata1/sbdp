from pyspark.sql import SparkSession


class Log4J:
    def __init__(self, spark: SparkSession):
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger("SBDP")

    def info(self, msg: str):
        self.logger.info(msg)

    def warn(self, msg: str):
        self.logger.warn(msg)

    def error(self, msg: str):
        self.logger.error(msg)

    def debug(self, msg: str):
        self.logger.debug(msg)
