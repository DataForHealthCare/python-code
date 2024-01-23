import pyspark
import datetime
from pyspark.sql import SparkSession
from typing import List
from pyspark.sql.types import (
    DateType, 
    DecimalType,
    DoubleType,
    IntegerType, 
    LongType,
    StringType, 
    StructType, 
    TimestampType
    )

"""
    Assign Default Values Row Module

    Definition: 
        This Databricks shared module allows the user to dynamically create a row of default values for table objects in the Unity Catalog.

    How to Implement: 
        This file is intended to be imported as a module and instantiated from the following class method:
            * add_row - returns a spark dataframe with an appended row of default values based on a pre-determined grid. 

    Recommended Code Usage: 
        Import - from default_values_row import DefaultRow
        Object Creation - DefaultRow.add_row(---spark dataframe object---)
        View Results - DefaultRow.add_row(---spark dataframe object---).display()

    Default Values: DataType | Suffix | Returned Value
        StringType | 'yn' | 'N'
        StringType | abrv', 'amt', 'cd', 'desc', 'id', 'nm', 'no', 'txt' | '~'
        StringType | None | 'Not Available'
        IntegerType | 'amt', 'cnt' | 0
        IntegerType | 'cd', 'id', 'no', 'type' | -1
        IntegerType | None | -9999
        LongType | 'amt', 'cnt' | -10
        LongType | 'cd', 'id', 'no' | -100
        LongType | None | -99999
        DoubleType | 'amt', 'cnt' | -0.1
        DoubleType | 'cd', 'id', 'no' | -1.0
        DoubleType | None | -0.9999
        DecimalType | 'amt', 'cnt' | -0.11
        DecimalType | 'cd', 'id', 'no' | -1.1
        DecimalType | None | -0.99999
        TimestampType | 'ts' | datetime.datetime(9999, 1, 1)
        TimestampType | None | datetime.datetime(1111, 1, 1)
        DateType | 'dt' | datetime.date(9999, 1, 1)
        DateType | None | datetime.date(1111, 1, 1)
        *Outlier Data Type | None or Not Applicable | Not Available

    Output Results:
        A spark dataframe for use within a notebook, template or Python file.
"""

class DefaultRow: 
    """
        DefaultRow class is meant to be a container for static methods related to this module. 
        The class is also state wrapper for a local SparkSession for usage in .py files, notebooks and applications outside of the Databricks environment.

        Variables (Constants)
        ----------
        SPARK: SparkSession.builder()
          
        Output
        ----------
        No intended output.
    """
    SPARK = SparkSession.builder \
           .appName("default_values_row") \
           .master("local[*]") \
           .config("spark.driver.allowMultipleContexts", "true") \
           .getOrCreate()
           
    @staticmethod    
    def diagnose_object(df: pyspark.sql.DataFrame) -> StructType: 
        """
        diagnose_object takes in a dataframe and maps table fields to StructType variable and returns this format as the evaulated schema.

        Parameters
        ----------
        df: pyspark.sql.DataFrame
            The inital dataframe intended to have the default row appened.
        
        Output
        ----------
        schema: StructType
            The returned suffix from the column name or column name without self-contained underscores.
        """
        schema = StructType(
            [_ for _ in df.schema.fields]
            )
        return schema   
        
    @staticmethod
    def validate_column_suffix(column_name: str) -> str:
        """
        validate_column_suffix takes in a column name with a type string, coverts camel case StructType name to lowercase,
        splits to column name into a list if the string contains an underscore, and returns the last list item as the suffix to the coulmn name. 

        Parameters
        ----------
        column_name: str
            The column name from the evaluated dataframe schema. 
        
        Output
        ----------
        col[-1]: str
            The returned suffix from the column name or column name without self-contained underscores.
        """
        name = column_name.lower()
        col = name.split("_")
        return col[-1]

    @staticmethod
    def replicate_schema(df: pyspark.sql.DataFrame) -> List[type]: 
        """
        replicate_schema takes in a dataframe, evalutes the dataframe schema and returns a list contains tuples with the column datatype and column name.

        Parameters
        ----------
        df: pyspark.sql.DataFrame
            The dataframe passed into the module for appending.
        
        Output
        ----------
        datatypes: List[type]
            A list containing the tuples as follows [(data type, column name)]
        """
        data_types = [(
            _.dataType, DefaultRow.validate_column_suffix(_.name))\
                 for _ in df.schema.fields]
        return data_types

    @staticmethod
    def assign_default_values(data_types: List[type]) -> tuple:
        """
        assign_default_values takes in a list containing tuples, evaluates the columns by data type and suffix,
        assigns a default value to each column and creates a list of tuples based on those assignments. 

        Parameters
        ----------
        data_types: List[tuple]
            A list of tuples containing the current dataframe schema's datatypes and column names. 
            
        Output
        ----------
        [tuple(default_row)]: tuple
            The data for the new dataframe created in the connect_row method.
        """
        default_row = []
        for data_type in data_types:
            match data_type:
                case (StringType(), 'flg' | 'yn'):
                    default_row.append('N')
                case (StringType(), 'abrv' | 'amt' | 'cd' | 'desc' | 'id' | 'nm' | 'no' | 'txt'):
                    default_row.append('~')
                case (StringType(), str):
                    default_row.append('Not Available')
                case (IntegerType(), 'amt' | 'cnt'):
                    default_row.append(0)
                case (IntegerType(), 'cd' | 'id' | 'no' | 'type'):
                    default_row.append(-1)
                case (IntegerType(), int):
                    default_row.append(-9999)
                case (LongType(), 'amt' | 'cnt'):
                    default_row.append(-10)
                case (LongType(), 'cd' | 'id' | 'no'):
                    default_row.append(-100)
                case (LongType(), int):
                    default_row.append(-99999)
                case (DoubleType(), 'amt' | 'cnt'):
                    default_row.append(-0.1)
                case (DoubleType(), 'cd' | 'id' | 'no'):
                    default_row.append(-1.0)
                case (DoubleType(), float):
                    default_row.append(-0.9999)
                case (DecimalType(), 'amt' | 'cnt'):
                    default_row.append(-0.11)
                case (DecimalType(), 'cd' | 'id' | 'no'):
                    default_row.append(-1.1)
                case (DecimalType(), float):
                    default_row.append(-0.99999)
                case (TimestampType(), 'ts'):
                    default_row.append(datetime.datetime(9999, 1, 1))
                case (TimestampType(), str):
                    default_row.append(datetime.datetime(1111, 1, 1))
                case (DateType(), 'dt'):
                    default_row.append(datetime.date(9999, 1, 1))
                case (DateType(), str):
                    default_row.append(datetime.date(1111, 1, 1))
                case _:
                    default_row.append('Not Available')
        return tuple(default_row)

    @staticmethod    
    def connect_row(
        default_row: tuple, 
        schema: StructType,
        main_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        connect_row takes in a the original dataframe to define both the schema and to union with the new dataframe, 
        and a tuple containing the data for the default values row.
         
        Parameters
        ----------
        default_row: List[tuple]
            The data for the row containing default values.
        schema: pyspark.sql.DataFrame
            The schema used to define the new dataframe.
        main_df: pyspark.sql.DataFrame
            Original dataframe containing all data.
        
        Output
        ----------
        df: pyspark.sql.DataFrame
            The dataframe containing both the original values and the appended row of default values.
        """
        default_values_df = DefaultRow.SPARK.createDataFrame([default_row], schema)
        final_df = main_df.union(default_values_df)
        return final_df

    @staticmethod
    def add_row(
        df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        add_row takes in the original dataframe before the union is acheived with the new dataframe containing default row values.

        Parameters
        ----------
        df: pyspark.sql.DataFrame
            The dataframe passed in as an argument for the rull_all method.
        
        Output
        ----------
        df: pyspark.sql.DataFrame
            Output dataframe used for inserting an object into the Unity Catalog.
        """
        diagnosis = DefaultRow.diagnose_object(df)
        replication = DefaultRow.replicate_schema(df)
        assigning = DefaultRow.assign_default_values(replication)
        connection = DefaultRow.connect_row(assigning, diagnosis, df)
        return connection