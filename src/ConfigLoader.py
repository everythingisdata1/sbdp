import configparser
import os

from pyspark import SparkConf

SBDP_CONFIG_FILE = "conf/sbdp.conf"
SPARK_CONFIG_FILE = "conf/spark.conf"


def _load_config(file_path: str) -> configparser.ConfigParser:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Config file not found: {file_path}")

    config = configparser.ConfigParser()
    config.read(file_path)
    return config


def get_config(env):
    config = _load_config(SBDP_CONFIG_FILE)
    return dict(config.items(env))


def get_spark_config(env):
    config = _load_config(SPARK_CONFIG_FILE)
    spark_config = SparkConf()
    for key, val in config.items(env):
        spark_config.set(key, val)
    return spark_config


def get_data_filter(env, data_filter):
    conf = get_config(env=env)
    return "true" if conf[data_filter] == "" else conf[data_filter]
