from pyspark.sql.connect.session import SparkSession

from src import ConfigLoader


def get_account_schema():
    return f"""load_date date,active_ind int,account_id string,source_sys string,
                account_start_date timestamp, legal_title_1 string,legal_title_2 string,
                tax_id_type string,tax_id string, branch_code string, country string """


def get_party_schema():
    schema = """load_date date,account_id string,party_id string,
    relation_type string,relation_start_date timestamp"""
    return schema


def get_address_schema():
    schema = """load_date date,party_id string,address_line_1 string,
    address_line_2 string,city string,postal_code string,
    country_of_address string,address_start_date date"""
    return schema



def _read_data(spark: SparkSession, env, enable_hive, hive_db, hive_table, paths, filter_key, schema):
    runtime_filter = ConfigLoader.get_data_filter(env, filter_key)
    if enable_hive:
        return spark.sql(f"""select * from {hive_db}.{hive_table}""")
    else:
        return (spark.read
                .format("csv")
                .option("header", "true")
                .schema(schema)
                .load(paths)
                .where(runtime_filter)
                )


def read_account(spark: SparkSession, env, enable_hive, hive_db):
    return _read_data(spark=spark, env=env, enable_hive=enable_hive, hive_db=hive_db, hive_table='accounts',
                      paths="test_data/accounts/", filter_key='account.filter', schema=get_account_schema())


def read_parties(spark, env, enable_hive, hive_db):
    return _read_data(spark=spark, env=env, enable_hive=enable_hive, hive_db=hive_db, hive_table='parties',
                      paths="test_data/parties/", filter_key='party.filter', schema=get_party_schema())


def read_address(spark, env, enable_hive, hive_db):
    return _read_data(spark=spark, env=env, enable_hive=enable_hive, hive_db=hive_db, hive_table='party_address',
                      paths="test_data/party_address/", filter_key='address.filter', schema=get_address_schema())
