import sys
import psycopg2

from io import StringIO

class Database:
  def __init__(self, config):
    self.host = config.HOST
    self.username = config.USERNAME
    self.password = config.PASSWORD
    self.port = config.PORT
    self.dbname = config.DBNAME
    self.conn = None
    
     
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
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, header=False)
    buffer.seek(0)
    
    cursor = self.conn.cursor()
    try:
      cursor.copy_from(buffer, table, sep=",")
      self.conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
      print("Error: %s" % error)
      self.conn.rollback()
      cursor.close()
      return 1
    print("batch done")
    cursor.close()

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


  
