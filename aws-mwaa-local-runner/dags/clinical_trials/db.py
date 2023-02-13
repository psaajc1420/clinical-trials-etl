import sys
import psycopg2

import pandas as pd

class Database:
  def __init__(self, config):
    self.host = config.HOST
    self.username = config.USERNAME
    self.password = config.PASSWORD
    self.port = config.PORT
    self.dbname = config.DBNAME
    self.conn = None
    
  def _check_table_exists(self, schema, table):

    self.connect()

    # define query to check if a table exists 
    query = """
        SELECT max(1) as column FROM information_schema.tables
        WHERE table_schema = '{}'
        AND table_name = '{}';
    """.format(schema, table)

    # create cursor
    pg_cursor = self.conn.cursor()

    # execute query
    pg_cursor.execute(query)
    query_results = pg_cursor.fetchall()

    # check results
    if query_results[0][0] == 1:
        return True            
    else:
        return False

  def connect(self):
    """Connect to Postgres database"""

    if self.conn is None:
      try:
        self.conn = psycopg2.connect(
          database=self.dbname,
          user=self.username,
          password=self.password,
          host=self.host,
          port=self.port
        )
      except psycopg2.DatabaseError as e:
        self._print_psycopg2_exception(e)
        raise e
      finally:
        print('Connection opened successfully.')

  def select_rows(self, query):
    """Run a SQL query to select rows from table."""
    self.connect()
    with self.conn.cursor() as cur:
      cur.execute(query)
      records = [row for row in cur.fetchall()]
      cur.close()
      return records

  def insert_many(self, df, table):
    """
      Insert many in connected database
    """
    self.connect()
    cursor = self.conn.cursor()
    try:
      col_names = ', '.join(df.columns)
      
      placeholders = ', '.join(["%s"]*len(df.columns))

      
      for _, row in df.iterrows():
        row_values = [None if pd.isna(value) else value for value in row.tolist()]
        cursor.execute(f"""
          INSERT INTO {table} ({col_names})
          SELECT {placeholders}
          WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE {' AND '.join([f'{col} = %s' for col in df.columns])})
        """, tuple(row_values + row_values))
      self.conn.commit()
  
    except (Exception, psycopg2.DatabaseError) as error:
      print("Error: %s" % error)
      self.conn.rollback()
      self.conn.close()
      return 1
    
    print('batch done')
    cursor.close()
    self.conn.close()

  # define a function that handles and parses psycopg2 exceptions
  def _print_psycopg2_exception(self, err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()

    # get the line number when exception occured
    line_num = traceback.tb_lineno

    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_num)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type)

    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err.diag)

    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")


  
