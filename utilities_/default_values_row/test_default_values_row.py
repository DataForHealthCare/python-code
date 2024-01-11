import pytest
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType
    )

# Import the class to test
from Databricks.SharedModules.default_values_row import DefaultRow

# Parameter assignment from job task.
try:
    environment = dbutils.widgets.get("environment")
except:
    environment = 'development'

# Determination of output location based on job task parameter.
if environment == 'production':
    catalog = 'enriched'
else:
    catalog = 'enriched_dev'

spark = SparkSession.builder.getOrCreate()

# Unit test that checks if the schema from the input dataframe match the returned StructType schema. 
# Expected results: (PASS, PASS, PASS)
@pytest.mark.parametrize(
    'test_df',
    [
        (spark.createDataFrame([(1, 'a'), (2, 'b')], ['id', 'name'])),
        (spark.createDataFrame([(10, 'individual'), (22, 'group')], ['test_cnt', 'product_nm'])),
        (spark.createDataFrame([(datetime.datetime(9999, 1, 1), 'self_insured'), (datetime.datetime(9999, 1, 1), 'non_self_insured')], ['end_ts', 'name_nm']))
    ] 
)
def test_diagnose_object(test_df):
    assert DefaultRow.diagnose_object(test_df) == StructType([test_df.schema.fields[0], test_df.schema.fields[1]])

# Test to verify the 'validate_column_suffix' returns the suffix when present (preceded by an underscore) and returns the string (column name) when no suffix is provided. 
# Expected results: (PASS, PASS, PASS, FAIL, FAIL)
@pytest.mark.parametrize(
    'column_name',
    [
        ('copay_options_yn'),
        ('access_group_code'),
        ('financial'),
        (9),
        (False)
    ]
)
def test_validate_column_suffix(column_name):
    name = column_name.lower()
    col = name.split("_")
    suffix = col[-1]
    assert DefaultRow.validate_column_suffix(column_name) == suffix

 # Test that the method 'replicate_schema' returns the expected list of tuple with the column datatype and column name.
#  Expected results: (PASS, FAIL, FAIL)
@pytest.mark.parametrize(
    'data_frame',
    [
        (spark.createDataFrame([(1, 'a'), (2, 'b')], ['id', 'name'])),
        (spark.createDataFrame([(.99, 'a'), (.1, 'b')], ['id', 'name'])),
        (spark.createDataFrame([(False, 'a'), (True, 'b')], ['id', 'name']))
    ]
)
def test_replicate_schema(data_frame):
    assert DefaultRow.replicate_schema(data_frame) == [(LongType(), 'id'), (StringType(), 'name')]

# Test that the method 'assign_default_values' returns the expected tuple of default values.
#  Expected results: (PASS, FAIL, FAIL)
@pytest.mark.parametrize(
    'data_types',
    [
        ([(StringType(), 'name')]),
        ([(DoubleType(), 'project_nm')]),
        ([(LongType(), 'options_yn')])
    ]
)
def test_assign_default_values(data_types):
    assert DefaultRow.assign_default_values(data_types) == ('Not Available',)

# Test to make sure the adsditional default row is added to the main dataframe used within the module.
# Expected results: (PASS)
@pytest.mark.parametrize(
    'default_row, schema, main_df',
    [
        (
            (9999, 'Not Available'),
            StructType([StructField("id", LongType(), False), StructField("name", StringType(), False),]),
            spark.createDataFrame([(1, 'a'), (2, 'b')], ['id', 'name'])
        )
    ]
)
def test_connect_row(default_row, schema, main_df):
    default_values_df = spark.createDataFrame([default_row], schema)
    final_df = main_df.union(default_values_df)
    assert final_df.count() == main_df.count() + 1

# Unit test to verify the 'default_values_row.py' shared module returns a dataframe with data.
# Expected results: (PASS)
@pytest.mark.parametrize(
    'data_frame',
        [
             (spark.createDataFrame([(10, 'individual'), (22, 'group')], ['test_cnt', 'product_nm']))
        ]
)
def test_add_row(data_frame):
    diagnosis = DefaultRow.diagnose_object(data_frame)
    replication = DefaultRow.replicate_schema(data_frame)
    assigning = DefaultRow.assign_default_values(replication)
    connection = DefaultRow.connect_row(assigning, diagnosis, data_frame)
    assert connection.isEmpty() == False\
        and connection is not None