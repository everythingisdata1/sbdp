import os
import sys
import uuid

from pyspark.sql.functions import col, to_json, struct

from src import ConfigLoader, DataLoader, Transformations
from src.config import Utils
from src.lib import Log4J

# Ensure log directory exists
os.makedirs("logs", exist_ok=True)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: SBDP- {dev, qa ,prod} {load_date}- Arg is missing")
        sys.exit(-1)
    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]
    job_run_id = "SBDP-" + str(uuid.uuid4())

    print(f"[INITIALISED] SBDP Spark JOB for env {job_run_env} and job id {job_run_id}")
    config = ConfigLoader.get_config(env=job_run_env)
    enable_hive = True if config['enable.hive'] == True else False
    hive_db = config['hive.database']

    print("Creating Spark Session.. ")
    spark = Utils.get_spark_session(job_run_env)
    spark.sparkContext.setLogLevel("INFO")
    logger = Log4J(spark=spark)
    logger.info("[START] :: Spark Bulk Data Processing started")

    logger.error("Reading SBDP parties Csv to DF")
    parties_df = DataLoader.read_party(spark=spark, env=job_run_env, enable_hive=enable_hive, hive_db=hive_db)
    relations_df = Transformations.get_relations(df=parties_df)

    logger.error("Reading SBDP Address Csv to DF")
    address_df = DataLoader.read_address(spark=spark, env=job_run_env, enable_hive=enable_hive, hive_db=hive_db)
    relation_address_df = Transformations.get_relations(df=address_df)

    logger.error("Joining Party Relation and Address DF")
    party_address_Df = Transformations.join_party_address(party_df=relations_df, address_df=relation_address_df)

    logger.error("Reading SBDP Account Csv to DF")
    account_df = DataLoader.read_account(spark=spark, env=job_run_env, enable_hive=enable_hive, hive_db=hive_db)
    contract_df = Transformations.get_contract(df=account_df)

    logger.error("Joining Account and Parties")
    data_df = Transformations.join_contract_party(contract_df=contract_df, party_df=party_address_Df)

    logger.error("Apply Header and create Event ")
    final_df = Transformations.apply_header(spark=spark, df=data_df)

    # TODO
    kafka_kv_df = final_df.select(col('payload.contactIdentifier.newValue').alias("key"),
                                  to_json(struct('*')).alias("value"))

    kafka_key = config['kafka.key']
    kafka_secret = config['kafka.api_secret']

    (final_df.write.format("kakfa")
     .option()
     .save()
     )

    logger.info("Finished Data sending data to Kafka")
