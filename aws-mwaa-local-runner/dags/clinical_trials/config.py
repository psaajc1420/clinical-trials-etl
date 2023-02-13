from airflow.models import Variable

DBNAME = Variable.get('POSTGRES_DB')
USERNAME = Variable.get('POSTGRES_USER')
PASSWORD = Variable.get('POSTGRES_PASSWORD')
HOST = Variable.get('HOST')
PORT = Variable.get('PORT')

# BS envs
BS = Variable.get('BS')