from pyspark.sql import functions as F, DataFrame
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.functions import collect_list, struct


def get_insert_operation(column, alias):
    return F.struct(
        F.lit("INSERT").alias("operation"),
        column.alias('newValue'),
        F.lit(None).alias('oldValue')
    ).alias(alias)


def get_contract(df: DataFrame):
    contract_title = F.array(F.when(~F.isnull("legal_title_1"),
                                    F.struct(F.lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                                             F.col("legal_title_1").alias("contractTitleLine")).alias("contractTitle")),
                             F.when(~F.isnull("legal_title_2"),
                                    F.struct(F.lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                                             F.col("legal_title_2").alias("contractTitleLine")).alias("contractTitle"))
                             )

    contract_title_nl = F.filter(contract_title, lambda x: ~F.isnull(x))

    tax_identifier = F.struct(F.col("tax_id_type").alias("taxIdType"),
                              F.col("tax_id").alias("taxId")).alias("taxIdentifier")

    df.printSchema()
    df_with_struct = df.select(
        F.col("account_id"),
        get_insert_operation(F.col("account_id"), "contractIdentifier"),
        get_insert_operation(F.col("source_sys"), "sourceSystemIdentifier"),
        get_insert_operation(F.col("account_start_date"), "contactStartDateTime"),
        get_insert_operation(contract_title_nl, "contractTitle"),
        get_insert_operation(tax_identifier, "taxIdentifier"),
        get_insert_operation(F.col("branch_code"), "contractBranchCode"),
        get_insert_operation(F.col("Country"), "contractCountry")
    )
    df_with_struct.printSchema()
    return df_with_struct


def get_relations(df: DataFrame):
    df.printSchema()
    df_with_struct = df.select(
        "account_id",
        "party_id",
        get_insert_operation(F.col("party_id"), "partyIdentifier"),
        get_insert_operation(F.col("relation_type"), "partyRelationshipType"),
        get_insert_operation(F.col("relation_start_date"), "partyRelationStartDateTime"),
    )
    return df_with_struct


def get_address(df: DataFrame):
    print("START:: get Address")
    df.printSchema()
    address = F.struct(
        F.col("address_line_1").alias("addressLine1"),
        F.col("address_line_2").alias("addressLine2"),
        F.col("city").alias("addressCity"),
        F.col("postal_code").alias("addressPostalCode"),
        F.col("country_of_address").alias("addressCountry"),
        F.col("address_start_date").alias("addressStartDate"),
    )
    df_with_struct = df.select("party_id", get_insert_operation(address, "partyAddress"))
    print("END:: get Address")
    df_with_struct.printSchema()
    return df_with_struct


def join_party_address(party_df: DataFrame, address_df: DataFrame):
    party_address_df = (party_df.join(address_df, "party_id", "left_outer")
    .groupby("account_id").agg(
        F.collect_list(
            F.struct(
                "partyIdentifier",
                "partyRelationshipType",
                "partyRelationStartDateTime",
                "partyAddress",
            ).alias("partyDetails")
        ).alias("partyRelations")
    ))
    print("Party_adderss_df")
    party_address_df.printSchema()
    return party_address_df


def join_contract_party(contract_df: DataFrame, party_df: DataFrame):
    contract_party_df = contract_df.join(party_df, "account_id", "left_outer")

    contract_party_df.printSchema()
    return contract_party_df


def apply_header(spark: SparkSession, df: DataFrame):
    header_info = [("SBDL-Contract", 1, 0), ]
    header_df = spark.createDataFrame(data=header_info).toDF("eventType", "majorSchemaVersion", "minorSchemaVersion")

    event_df = (
        header_df.hint("broadcast")
        .crossJoin(df)
        .select(
            F.struct(
                F.expr("uuid()").alias("EventIdentifier"),
                F.col("eventType"), F.col("majorSchemaVersion"), F.col("minorSchemaVersion"),
                F.lit(F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssZ")).alias("eventDateTime")
            ).alias("eventHandler"),
            F.array(F.struct(
                F.lit("contractIdentifier").alias("keyField"),
                F.col("account_id").alias("keyValue")
            )).alias("keys"),
            F.struct(
                F.col("contractIdentifier"),
                F.col("sourceSystemIdentifier"),
                F.col("contactStartDateTime"),
                F.col("contractTitle"),
                F.col("taxIdentifier"),
                F.col("contractBranchCode"),
                F.col("contractCountry"),
                F.col("partyRelations")
            ).alias("payload")
        )
    )

    return event_df
