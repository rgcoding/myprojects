import logging
from database import Database
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class MSSqlServer(Database):
    """
    Wrapper for all ms sqlserver calls.  Handles connections, parsing results
    and errors in a consistent way.   Can parse multi-sql statments where
    statements are separated by semi-colons

    Attributes:
        WAREHOUSE: Snowflake warehouse
        DATABASE Snwoflake DB to use as default
        SCHEMA: Snowflake Schema to use as default
    """

    def __init__(
            self, host=None, user=None, password=None, port=None,
            database=None, driver=None):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database = database
        self.driver = driver
        self.engine = None
        # self.connect_attributes(ssl_mode, ssl_cert, ssl_key)
        self.define_engine()

    def define_engine(self):
        self.engine = create_engine('mssql+pyodbc://%s:%s@%s:%s/%s?driver=%s' %
                                 (self.user, self.password, self.host, self.port, self.database, self.driver))
        # self.engine = create_engine('mssql+pyodbc://crescolabs:4BCd3k787UAclj2209Bnreaz82@MSSQL_DSN')

    def truncate_table(self, tbl_name):
        
        try:
            Session = sessionmaker(bind=self.engine)
            session = Session()
            session.execute('''IF OBJECT_ID('{table}') IS NOT NULL TRUNCATE TABLE {table}'''.format(table=tbl_name))
            session.commit()
        except Exception as e:
            raise RuntimeError(e)
        finally:
            session.close()
