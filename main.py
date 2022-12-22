from __future__ import annotations
import datetime
import pymssql, psycopg2
import pandas as pd
from io import StringIO

dbname=""
ms_username=""
ms_pwd=""
ms_host=""

default_schema='dbo'
pg_host="172.16.20.165"
pg_port="5432"
pg_user="postgres"
pg_password="Sa12345678"

class Utilities:
  @staticmethod
  def exclude_empty_str(arr):
    return list(filter(lambda x: x != '', arr))

  @staticmethod
  def select(array, field_name):
    return list(map(lambda x: x[field_name], array))      

class Logger:
  @staticmethod
  def error(msg):
    return print(f'\033[91m{Logger.datetimenow()} [error] {msg} \033[0m')

  @staticmethod
  def warn(msg):
    return print(f'\033[93m{Logger.datetimenow()} [warn] {msg} \033[0m')

  @staticmethod
  def info(msg):
    return print(f'\033[92m{Logger.datetimenow()} [info] {msg} \033[0m')

  @staticmethod
  def datetimenow():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

class SqlserverClient(object):
  def __init__(self, host, dbName, userName, password):
    self.host=host
    self.dbname=dbName
    self.username=userName
    self.password=password

  def create_cursor(self):
    conn= pymssql.connect(host=self.host, database=self.dbname, user=self.username,password=self.password, charset = 'utf8')
    return conn.cursor(as_dict=True)

  def fetch_all_tables(self):
    try:
      cursor = self.create_cursor()
      sqlraw = "select [name] FROM [sys].[tables]"
      cursor.execute(sqlraw)
      return Utilities.select(cursor.fetchall(), 'name')
    except pymssql.DatabaseError as e:
      Logger.error({"error": e, "para": [self.host, self.dbname, self.username, self.password]})
      return []

  def fetch_all_columns_of(self, table: str):
    try:
      cursor = self.create_cursor()
      sqlraw=f'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS '
      sqlraw = sqlraw + f"WHERE TABLE_NAME=N'{table}'"
      cursor.execute(sqlraw)
      return Utilities.select(cursor.fetchall(), 'COLUMN_NAME')
    except pymssql.DatabaseError as e:
      Logger.error({"mssql fetch all columns error": e, "para": [self.host, self.dbname, table]})
      return []

  def fetch_data_row_of(self, table, columns, page, count):
    try: 
      cursor = self.create_cursor()
      columns=','.join(list(map(lambda x: f"(case when [{x}] is not null then convert(nvarchar(max), [{x}] ,21) else 'None' end) as [{x}]",columns)))
      declares="DECLARE @PageIndex INT = %s, @PageSize INT = %s"
      subquery=f"select *, ROW_NUMBER() over(order by (select 0)) as RowId FROM dbo.[{table}]"
      sqlraw=f'{declares};select {columns} from ({subquery}) as tb where RowId BETWEEN (@PageIndex - 1) * @PageSize + 1 AND @PageIndex * @PageSize'
      cursor.executemany(sqlraw, [(page, count)])
      return cursor.fetchall()
    except pymssql.DatabaseError as e:
      Logger.error({"mssql fetch all data error": e, "para": [self.host, self.dbname, table]})
      return []

class pgClient:
  def __init__(self, host, dbName, schema, userName, password):
    self.host=host
    self.dbname=dbName
    self.username=userName
    self.password=password
    self.schema=schema

  def create_conn(self):
    conn= psycopg2.connect(
      host= self.host, 
      port="5432", 
      user=self.username, 
      password=self.password, 
      database=self.dbname, 
      options=f"-c search_path={self.schema}")
    conn.set_client_encoding('UTF8')
    return conn;
  
  def createCursor(self):
    conn=psycopg2.connect(
      host= self.host, 
      port="5432", 
      user=self.username, 
      password=self.password, 
      database=self.dbname, 
      options=f"-c search_path={self.schema}")
    return (conn, conn.cursor())

  def is_tb_exist(self, table):
    cursor=self.create_conn().cursor()
    cursor.execute(f"SELECT EXISTS (SELECT * FROM pg_tables WHERE tablename='{table}');")
    res= cursor.fetchone()
    return 'True' in str(res)

  def truncate_table(self, table):
    conn=self.create_conn()
    cursor=conn.cursor()
    cursor.execute(f'truncate table {self.schema}."{table}"')
    conn.commit()

  def import_data(self, table, columns, data):
    df=pd.DataFrame(data)
    df = df.apply(lambda x : x.replace('\n', '\\n'))
    output= StringIO()
    df.to_csv(output, sep=',', index=False, header=False)
    csv_data= output.getvalue()
    conn=self.create_conn()
    cursor=conn.cursor()
    col=str.join(',',list(map(lambda x: f'"{x}"',columns)))
    f = StringIO()
    f.write(csv_data)
    f.seek(0)
    cursor.copy_expert(f"COPY dbo.\"{table}\" ({col}) FROM STDIN WITH NULL as 'None' CSV DELIMITER ',';", f)
    conn.commit()
    cursor.close()    
    return

if __name__ == '__main__':
  pgclient =pgClient(pg_host, dbname, default_schema, pg_user, pg_password)
  mssql_client =SqlserverClient(ms_host, dbname, ms_username, ms_pwd)  
  tables=mssql_client.fetch_all_tables()
  for table in tables:
    if not pgclient.is_tb_exist(table):
      Logger.warn(f'skip pgsql import table: {table}, since table not exist in pgsql.')
      continue
    try:
      columns = mssql_client.fetch_all_columns_of(table)
      page=1
      size=100
      pgclient.truncate_table(table)
      while True:
        data=mssql_client.fetch_data_row_of(table, columns, page, size)
        if not data:
          break;        
        pgclient.import_data(table, columns, data)
        Logger.info(f'{dbname} copy [{table}] from mssql {ms_host} to postgres {pg_host} success(page: {page}, size: {size})')
        page+=1
    except psycopg2.Error as e:
      Logger.error(f"pgsql import table: {table}, error: {e}")
