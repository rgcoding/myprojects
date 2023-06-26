import logging
import re
import os
import pandas as pd
from database import Database
from sqlalchemy import create_engine
from sqlalchemy import exc
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class Postgres(Database):
    """
    Wrapper for all snowflake calls.  Handles connections, parsing results
    and errors in a consistent way.   Can parse multi-sql statments where
    statements are separated by semi-colons

    Attributes:
        WAREHOUSE: Snowflake warehouse
        DATABASE Snwoflake DB to use as default
        SCHEMA: Snowflake Schema to use as default
    """

    def __init__(
            self, host=None, user=None, password=None, port=None,
            database=None, ssl_mode=None, ssl_cert=None, ssl_key=None):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database = database
        self.ssl_mode = ssl_mode
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key
        self.engine = None
        # self.connect_attributes(ssl_mode, ssl_cert, ssl_key)
        self.define_engine()

    # def connect_attributes(self, ssl_mode, ssl_cert, ssl_key):
    #     self.ssl_mode = ssl_mode
    #     self.ssl_cert = ssl_cert
    #     self.ssl_key = ssl_key

    def define_engine(self):
        self.engine = create_engine('postgresql+psycopg2://%s:%s@%s:%s/%s' %
                                    (self.user,self.password, self.host, self.port, self.database),
                                    connect_args={'sslmode': self.ssl_mode,'sslcert': self.ssl_cert,
                                                  'sslkey': self.ssl_key}, echo=True)
