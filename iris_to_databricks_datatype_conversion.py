import iris 
import pyodbc 
import openpyxl
import pandas as pd


def main():
# IRIS connection information
  connection_string = "ahfprd:1982/pra"
  username = "dssdatastage"
  password = "Super1or"

  connection = iris.connect(connection_string, username, password)
  cursor = connection.cursor()

# Databricks connection information
  databricks_conn = pyodbc.connect("Driver={Simba Spark ODBC Driver};" +
                      "HOST=adb-3434007450447828.8.azuredatabricks.net;" +
                      "PORT=443;" +
                      "Schema=default;" +
                      "SparkServerType=3;" +
                      "AuthMech=3;" +
                      "UID=token;" +
                      "PWD=dapice5202a6d220fe5e81d18cd7b7123975-2;" +
                      "ThriftTransport=2;" +
                      "SSL=1;" +
                      "HTTPPath=sql/protocolv1/o/3434007450447828/1003-180825-9ywg66xx",
                      autocommit=True)

  try:
    # Schema variables
    databricks_schema = 'adminsystemsprembill'
    admin_sys_schema = 'prembill'

    # Get table names
    db_cursor = databricks_conn.cursor()
    db_cursor.execute(f"""
                    SHOW TABLES IN cleansed.{databricks_schema}
                    """)
    db_describe = db_cursor.fetchall() 

    # Create the Excel workbook
    workbook = openpyxl.Workbook()
    workbook.save(f"t_claims_datatype_comparison_{databricks_schema}.xlsx")

    with pd.ExcelWriter(f"t_claims_datatype_comparison_{databricks_schema}.xlsx", mode="a", engine="openpyxl") as excel_sheet:

      # Loop through the table names
      for _ in db_describe:
          table_name = _[1]

          # Databricks query
          db_cursor = databricks_conn.cursor()
          db_cursor.execute(f"""
                            DESCRIBE cleansed.{databricks_schema}.{table_name} 
                            """)
          db_describe = db_cursor.fetchall() 
       
          # IRIS Query
          cursor.execute(f"""
                          SELECT COLUMN_NAME, DATA_TYPE 
                          FROM INFORMATION_SCHEMA.COLUMNS 
                          WHERE TABLE_SCHEMA = '{admin_sys_schema}' 
                          AND TABLE_NAME = '{table_name}'
                          """) 
          columns = cursor.fetchall() 

          # Dataframe architecture
          comp = []
          cols = ['cache_col', 'cache_datatype', 'databricks_col', 'databricks_datatype', 'difference_indicator']

          # Compare column datatypes
          for _ in columns:
            for col in db_describe:
              if _[0] in col:
                  if _[1] == 'varchar' and col[1] == 'string'\
                      or _[1] == 'time' and col[1] == 'timestamp'\
                      or _[1] == col[1]:
                      comp.append(
                        [*_, col[0], col[1], None]
                        )
                  else:
                      comp.append(
                        [*_, col[0], col[1], 'x']
                        )

          df = pd.DataFrame(
            comp, columns=cols
          )

          # Write the paramrized dataframe to the spreadsheet
          df.to_excel(excel_sheet, sheet_name=f'{table_name}'[:30], index=False)
 
  except Exception as ex:
    print(ex)
  finally:
    if cursor:
      cursor.close()
    if connection:
      connection.close()
    if db_cursor:
      db_cursor.close()
    if databricks_conn:
      databricks_conn.close()

if __name__ == "__main__":
  main()