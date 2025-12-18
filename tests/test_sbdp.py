@pytest.fixture(scope='session')
def spark():
    return get_spark_session()


def test_bulk_test(spark):
    print(spark.version)

    assert spark.version == '4.0.1'
