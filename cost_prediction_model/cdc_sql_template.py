# Databricks notebook source
# MAGIC %md
# MAGIC ## Change data capture template

# COMMAND ----------

# MAGIC %md
# MAGIC #### Usage:
# MAGIC - This notebook contains generic code that will run using the changes you have made to index.yaml and extract_config.yaml.
# MAGIC   - *DO NOT* make changes to this notebook!
# MAGIC - refer to [README](https://adb-1501466301957626.6.azuredatabricks.net/?o=1501466301957626#notebook/2271376384770036/command/2271376384770037) for instructions.
# MAGIC - refer to [writing_messy_code](https://confluence.healthpartners.com/confluence/pages/viewpage.action?pageId=297468803) if you are unfamilliar with using config and yaml files in databricks

# COMMAND ----------

# MAGIC %md
# MAGIC #### Installs & Imports

# COMMAND ----------

# Installs & imports
%pip install --trusted-host artifactory.healthpartners.com pyyaml -i https://artifactory.healthpartners.com/artifactory/api/pypi/python-hosted-remote/simple
%pip install --trusted-host artifactory.healthpartners.com great-expectations -i https://artifactory.healthpartners.com/artifactory/api/pypi/python-hosted-remote/simple
import pandas as pd
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
import yaml
import json
import sys
import warnings
import Databricks.SharedModules.notification_utils as notif
import Databricks.SharedModules.delta_table_utils as dtutil
from Databricks.SharedModules.general import get_config_item, get_catalog_suffix
from Databricks.SharedModules.gx_tool import SimpleGXTool
from datetime import date, datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC #### Environment setup
# MAGIC - Set up the environment and catalog variables by checking the current system environment

# COMMAND ----------

# From the Schedule button in the top right, create a new workflow for your job. Set key = source_environment, value = prd and key = target_environment, value = dev.
# Going forward, run your notebook using the workflow, rather than interactively. You'll need to click into the job to see the output, rather than looking here.
# Source and target environments from the job task
# Comment out to run in notebook
source_environment = dbutils.widgets.get('source_environment')
target_environment = dbutils.widgets.get('target_environment')

catalog ='standardized{}'.format(get_catalog_suffix(target_environment))

try:
    test_warning = bool(dbutils.widgets.get('test_warning'))
except:
    test_warning = False

try:
    test_error = bool(dbutils.widgets.get('test_error'))
except:
    test_error = False

print('######################## ENVIRONMENT SETUP')
print('source_environment: ', source_environment)
print('target_environment: ', target_environment)
print(f'Catalog: {catalog}')
print('test_warning: ', test_warning)
print('test_error: ', test_error)

# # Uncomment below and comment out above to run in notebook
# 	# Parameter assignment from job task.
# try:
#     environment = dbutils.widgets.get("environment")
# except:
#     environment = 'development'
# # Determination of output location based on job task parameter.
# if environment == 'production':
#     catalog = 'standardized' 
# else:
#     catalog = 'standardized_dev'
# source_environment = 'dev'
# target_environment = 'dev'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup
# MAGIC - instantiate a python variable containing the info from your index.yaml file 

# COMMAND ----------

with open('./config/index.yaml', 'r') as f:
  index_yaml = yaml.safe_load(f)

schema = index_yaml['schema']
table = index_yaml['table']
table_name = table['name']
full_table_name = f"{catalog}.{schema}.{table_name}"
print('schema: ', schema)
print('table name: ', table_name)
print('fully qualified table name: ', full_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Quality Import
# MAGIC - Run the data_quality notebook to gain access to it's functions

# COMMAND ----------

# MAGIC %run ./config/data_quality

# COMMAND ----------

def configure_great_expectations():
    dbutils.notebook.run('./config/great_expectations_config',
                        timeout_seconds=0,
                        arguments={'target_environment': target_environment})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extract source data
# MAGIC - The function in the following cell creates a dataframe from the source data defined by the query in extract_config.yaml.
# MAGIC - This function will raise an exception if the source dataframe is not populated

# COMMAND ----------

def extract():
    extract_qry = get_config_item('./config/extract_config.yaml', 'source1_extract')
    my_table_name_sdf = spark.sql(extract_qry.format(suffix=get_catalog_suffix(source_environment)))
    # check to see source dataframe isn't empty
    source_dataframe_populated(my_table_name_sdf, full_table_name, source_environment, target_environment)

    return my_table_name_sdf

# COMMAND ----------

def get_primary_key_columns():    
    primary_keys_list = []
    for pk_num in range(1,len(table['primary_keys'])+1):
        primary_key_column_name = [table['primary_keys']['pk_column_'+str(pk_num)]]
        primary_keys_list += primary_key_column_name
    return primary_keys_list

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation
# MAGIC - This function performs various data checks using functions in the data_quality.py file.
# MAGIC - This function also uses gx_tool.py to invoke expectations and raise warmings or exceptions when the expectations are violated 

# COMMAND ----------

def validate(my_table_name_sdf, catalog, schema, table):
    # Run GX config file to import its functions
    # configure_great_expectations()
    # Verify record count is plausible
    # - Sends warning if number of records loaded is less than warning_threshold
    # - Raises error if number of records loaded is less than erorr_threshold
    try:
        x = spark.sql(f"SELECT * from {full_table_name}")
    except:
        data_completeness(catalog=catalog,
                                schema=schema,
                                table=table,
                                to_load_sdf=my_table_name_sdf,
                                spark=spark,
                                source_environment = source_environment,
                                target_environment = target_environment,
                                warning_threshold= index_yaml['data_completeness_warning_threshold'],
                                error_threshold= index_yaml['data_completeness_error_threshold'])

    # Verify data is unique by the natural key
    # - Verifies that the pyspark dataframe is not duplicated by the key_columns
    primary_keys_list = get_primary_key_columns()
    key_validation(sdf=my_table_name_sdf,
                   full_table_name=full_table_name,
                   source_environment=source_environment, 
                   target_environment=target_environment, 
                   key_columns=primary_keys_list)
    
    # Verify source dataframe is populated
    # - Sends a warning if the number of records in the source pyspark dataframe is less than warning_num_obs. 
    # - Raises an error if less than error_num_obs.
    source_dataframe_populated(extract_sdf= my_table_name_sdf,
                               full_table_name= full_table_name, 
                               source_environment= source_environment, 
                               target_environment= target_environment,
                                warning_num_obs= index_yaml['source_populated_warning_num_obs'],
                                error_num_obs= index_yaml['source_populated_error_num_obs'])

    # Verify columns with type date are in valid range
    # - Sends warning if invalid dates present but not greater than the allowed number
    # - Raises error if number of invalid dates exceeds the allowed value
    date_cols = []
    for col_name, col_data_type in my_table_name_sdf.dtypes:
        if col_data_type == 'date':
            date_cols.append(col_name)
    print(f"date colunns: {date_cols}")
    date_range_validation(sdf= my_table_name_sdf,
                          date_columns= date_cols,
                          bad_dates_allowed= index_yaml['bad_dates_allowed'],
                          min_allowable_dt= (datetime.now() - timedelta(days=365*200)).date(),
                        #   max_allowable_dt= (datetime.now() - timedelta(days=365)).date())
                          max_allowable_dt= datetime.now().date())

    # Create and run a great expectations checkpoint(s)
    # gx_tool = SimpleGXTool(catalog=catalog, schema=schema, table=table)
    # gx_tool.add_checkpoint(
    #             expectation_suite_name='{catalog}_{schema}_{table}_expectation_suite_error'.format(
    #                 catalog=catalog,
    #                 schema=schema,
    #                 table=table
    #                 ),
    #             dataframe=my_table_name_sdf)
    # gx_tool.run_validation()

    # gx_tool.add_checkpoint(
    #             expectation_suite_name='{catalog}_{schema}_{table}_expectation_suite_warning'.format(
    #                 catalog=catalog,
    #                 schema=schema,
    #                 table=table
    #                 ),
    #             dataframe=my_table_name_sdf)
    # gx_tool.run_validation()

# COMMAND ----------

# TODO: Add cell to create the schema if it doesnt exist
# create the schema

# spark.sql("""
#         CREATE SCHEMA IF NOT EXISTS standardized{suffix}.YOURSCHEMANAME
#         """.format(suffix=get_catalog_suffix(target_environment)))
# spark.sql("""
#         CREATE SCHEMA IF NOT EXISTS standardized{suffix}.YOURSCHEMANAME
#         """.format(suffix=get_catalog_suffix(target_environment)))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the initial table declared in index.yaml
# MAGIC - This next cell creates the DDL for the new databricks delta table.
# MAGIC - These initial tables will not include primary key or null constraints, these will be added in subsequent cells.

# COMMAND ----------

# create the table specified in index.yaml (no null constraints or primary keys yet)
def create_initial_table():
    # dynamically build string to declare new columns in query
    column_string_list = []
    column_list = list(table['columns'])
    for column_num in range(0,len(table['columns'])):
        column_name = column_list[column_num]
        column = table['columns'][column_list[column_num]]
        column_string = [f'''{column_name.upper()} {column['type']} COMMENT "{column['comment']}"''']
        column_string_list += column_string
    delim = ',\n'
    columns_string = delim.join(column_string_list)

    # build full table query
    create_table_query = f'''CREATE TABLE IF NOT EXISTS {full_table_name} (
    {columns_string})
        USING DELTA
        TBLPROPERTIES (DELTA.EnableChangeDataFeed = true)
    COMMENT "{table['comment']}"'''.format(catalog=catalog)
    print("##### CREATE TABLE QUERY: #####")
    print(create_table_query)
    print("\n")
    spark.sql(create_table_query)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add Null Constraints
# MAGIC - The following cell will add null constraints to all columns included in the primary key and any additional columns specified as not null

# COMMAND ----------

# add not null constrains
def add_null_constraints():
    print("##### NULL CONSTRAINT STATEMENTS: ##### ")
    # add null contraints for any columns that are part of the primary key
    for pk_num in range(1,len(table['primary_keys'])+1):
        primary_key_column = table['primary_keys']['pk_column_'+str(pk_num)]
        add_null_prim_keys_query = f'''
        ALTER TABLE {full_table_name}
        ALTER COLUMN {primary_key_column.upper()} SET NOT NULL'''.format(catalog=catalog)
        print(add_null_prim_keys_query)
        spark.sql(add_null_prim_keys_query)
    # add null constrains for any additional columns you want to specify as not allowing nulls
    if table['additional_not_null_columns'] is not None:
        for null_num in range(1,len(table['additional_not_null_columns'])+1):
            null_column = table['additional_not_null_columns']['null_col'+str(null_num)]
            add_additional_nulls_query = f'''
            ALTER TABLE {full_table_name}
            ALTER COLUMN {null_column.upper()} SET NOT NULL'''.format(catalog=catalog)
            print(add_additional_nulls_query)
            spark.sql(add_null_prim_keys_query)
    print("\n")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add Primary Key
# MAGIC - The following cell will add primary key constraints for the columns specified in index.yaml

# COMMAND ----------

# Add primary keys
def add_primary_key_constraints():
    print("##### PRIMARY KEY CONSTRAINT STATEMENTS: #####")
    primary_key_columns = []
    for pk_num in range(1,len(table['primary_keys'])+1):
            primary_key_column_name = [table['primary_keys']['pk_column_'+str(pk_num)]]
            primary_key_columns = primary_key_columns + (primary_key_column_name)
            pk_matching_string_to_add = f"target.{primary_key_column_name} == source.{primary_key_column_name}"
    delim = ','
    prim_keys_string = delim.join(primary_key_columns)
    add_prim_keys_query = f'''
            ALTER TABLE {full_table_name}
            ADD CONSTRAINT {table_name.upper()}_PK PRIMARY KEY ({prim_keys_string})'''.format(catalog=catalog)
    print(add_prim_keys_query)
    print("\n")
    spark.sql(add_prim_keys_query)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Helper Functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load index.yaml file
# MAGIC - Extract the source query from your extract_config.yaml file via safe_load
# MAGIC - This query will specify which data you want to load into your target table

# COMMAND ----------

# Start configuration by opening up yaml file	
def get_config_item(config_path, key):
	with open(config_path, 'r') as file:
	    config = yaml.safe_load(file)
	return config[key]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Identify source column names
# MAGIC The function in the following builds a string containing the names of the columns from your source data. These names are needed for the matching portion of the merge into statement

# COMMAND ----------

# get column names from source table and create formatted string
def get_source_column_names():
    source_query = get_config_item('./config/extract_config.yaml', 'source_query')
    source_df = spark.sql(source_query)
    source_column_list = []
    for column in source_df.columns:
        source_column_list += [f"source_table.{column}"]
    delim = ','
    source_columns_string = delim.join(source_column_list)
    print(source_column_list)
    print(source_columns_string)
    return source_columns_string

# COMMAND ----------

# MAGIC %md
# MAGIC #### Merge Condition (Primary Key Matching)
# MAGIC The function in the following cell builds a string containing the merge condition that will be used to match the primary keys of the source and target tables in the merge statement.

# COMMAND ----------

def get_merge_condition_string(table):
    primary_keys_matchings_list = []
    for pk_num in range(1,len(table['primary_keys'])+1):
        primary_key_column_name = table['primary_keys']['pk_column_'+str(pk_num)]
        primary_keys_matchings_list = primary_keys_matchings_list + [f"source_table.{primary_key_column_name} = target.{primary_key_column_name}"]
    delim = ' AND '
    merge_condition = delim.join(primary_keys_matchings_list)
    return merge_condition

# COMMAND ----------

# MAGIC %md
# MAGIC #### Column Matching Condition
# MAGIC - The function in the following cell builds a string containing the column matching condition that will be used to load date into the new table.

# COMMAND ----------

def get_column_matchings_string(table):
    column_matchings_list = []
    columns_list = list(table['columns'])
    for column_num in range(0,len(columns_list)):
        column_name = columns_list[column_num]
        column_matchings_list = column_matchings_list + [f"{column_name} = target.{column_name}"]
    delim = ','
    column_matchings_string = delim.join(column_matchings_list)
    return column_matchings_string

# COMMAND ----------

# MAGIC %md
# MAGIC #### Identify Target Columns
# MAGIC - The function in the following cell builds a string containing the names of the columns in the target table.

# COMMAND ----------

def get_target_columns_string(table):
    columns_list = list(table['columns'])
    delim = ','
    return delim.join(columns_list)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Merge Data Into Table
# MAGIC - The following cell will load the data specified in the extract_config.yaml query into the new table.
# MAGIC - This cell works by creating strings representing portions of the merge into statement.

# COMMAND ----------

#merge source data into each new table
def load_data_to_table():
    source_query = get_config_item('./config/extract_config.yaml', 'source_query')
    # create string for primary key matching portion of merge query
    merge_condition = get_merge_condition_string(table)
    # create string for column matching portion of merge query and all columns string
    column_matchings_string = get_column_matchings_string(table)
    target_columns_string = get_target_columns_string(table)
    source_columns_string = get_source_column_names()
    print("##### MERGE CONDITION STRING: #####")
    print(merge_condition)
    print("\n")
    print("##### COLUMN MATCHINGS STRING: #####")
    print(column_matchings_string)
    print("\n")
    print("##### TARGET COLUMNS STRING: #####")
    print(target_columns_string)
    print("\n")
    table_ = table
    query = f'''MERGE INTO {catalog}.{index_yaml['schema']}.{table_name} target
    USING (
        {source_query}) source_table
        ON ({merge_condition})
        WHEN MATCHED THEN
            UPDATE
            SET {column_matchings_string}
        WHEN NOT MATCHED THEN
            INSERT ({target_columns_string}) 
            VALUES ({source_columns_string})
        WHEN NOT MATCHED BY SOURCE THEN
            DELETE'''.format(catalog=catalog)
    print(query)
    spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Main function
# MAGIC - The main function will invoke the functions declared above.
# MAGIC - First the source data is extracted and validated.
# MAGIC - Then the target table is created if it doesn't yet exist. If the table exists this step is skipped.
# MAGIC - Finally the source date is merged into the target table with records no longer present in the source being deleted from the target table.

# COMMAND ----------

def main():
    # Extract
    my_table_name_sdf = extract()

    # Validate
    validate(my_table_name_sdf,
            catalog='standardized{}'.format(get_catalog_suffix(target_environment)),
            schema=schema,
            table=table_name)
    full_table_name = f"{catalog}.{schema}.{table_name}"
    try:
        x = spark.sql(f"SELECT * from {full_table_name}")
    except:
        create_initial_table()
        add_null_constraints()
        add_primary_key_constraints()
    load_data_to_table()

# COMMAND ----------

main()
