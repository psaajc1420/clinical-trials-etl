from os import environ, path
from dotenv import load_dotenv

# Load variables from .env
basedir = path.abspath(path.dirname(__file__))
print(basedir)
load_dotenv(path.join(basedir, '.env'))


# Database config
DBNAME = environ.get('AWS_RDS_DBNAME')
USERNAME = environ.get('AWS_RDS_USERNAME')
PASSWORD = environ.get('AWS_RDS_PASSWORD')
HOST = environ.get('AWS_RDS_HOST')
PORT = environ.get('AWS_RDS_PORT')
