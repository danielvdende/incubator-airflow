
from future.standard_library import install_aliases
from sqlalchemy.ext.declarative import declarative_base
from airflow import settings

install_aliases()

Base = declarative_base()
ID_LEN = 250
XCOM_RETURN_KEY = 'return_value'
Stats = settings.Stats
_CONTEXT_MANAGER_DAG = None

