import logging
import re
import os
import io
import secrets
import pandas as pd
import sshtunnel
import psycopg2
from database import Database
from sqlalchemy import create_engine
from sqlalchemy import exc
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class Postgres(Database):
    """
    Wrapper for all Postgres calls.  Handles connections, parsing results
    and errors in a consistent way.   Can parse multi-sql statments where
    statements are separated by semi-colons
    """

    def __init__(
            self, host=None, user=None, password=None, port=None,
            database=None, ssl_mode=None, ssl_cert=None, ssl_key=None,
            ssh_user=None, ssh_host=None, ssh_port=None, ssh_pkey=None,secret_name=None, region_name=None, driver='PostgreSQL'):
            self.pg_secrets = secrets.Secrets(secret_name, region_name)
            self.host = self.pg_secrets.get_secret_value('HOST')
            self.user = self.pg_secrets.get_secret_value('USER')
            self.password = self.pg_secrets.get_secret_value('PASSWORD')
            self.port = int(self.pg_secrets.get_secret_value('PG_PORT'))
            self.database = self.pg_secrets.get_secret_value('DATABASE')
            self.driver = driver

            self.ssh_user = self.pg_secrets.get_secret_value('SSH_USER')
            self.ssh_host = self.pg_secrets.get_secret_value('SSH_HOST')
            self.ssh_port = int(self.pg_secrets.get_secret_value('SSH_PORT'))
            self.ssh_pkey = self.pg_secrets.get_secret_value('SSH_PKEY')
 
            self.engine = None
            self.define_engine()

    def define_engine(self):
        self.engine = create_engine('postgresql://%s:%s@%s:%s/%s?driver=%s' %
                                         (self.user, self.password, self.host, self.port, self.database, self.driver))

    def execute_sql_via_ssh_tunnel(self, sql):
        with sshtunnel.open_tunnel((self.ssh_host, self.ssh_port),
                                       ssh_username=self.ssh_user,
                                       ssh_pkey=self.ssh_pkey,
                                       remote_bind_address=(self.host, self.port),
                                       local_bind_address=('localhost', 47122)
                                       ) as tunnel:

            with psycopg2.connect(
                        user=self.user, password=self.password,host=tunnel.local_bind_host, port=tunnel.local_bind_port) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                column_names = [item[0] for item in cursor.description]
                result = cursor.fetchall()
                df = pd.DataFrame(result, columns=column_names)
                return df



    def update_sql_via_ssh_tunnel(self, sql):
        with sshtunnel.open_tunnel((self.ssh_host, self.ssh_port),
                                       ssh_username=self.ssh_user,
                                       ssh_pkey=self.ssh_pkey,
                                       remote_bind_address=(self.host, self.port),
                                       local_bind_address=('localhost', 47122)
                                       ) as tunnel:
            with psycopg2.connect(
                        user=self.user, password=self.password,host=tunnel.local_bind_host, port=tunnel.local_bind_port) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)

    def df_to_pg(self, df, tablename):
        with sshtunnel.open_tunnel((self.ssh_host, self.ssh_port),
                                        ssh_username=self.ssh_user,
                                        ssh_pkey=self.ssh_pkey,
                                        remote_bind_address=(self.host, self.port),
                                        local_bind_address=('localhost', 47122)
                                        ) as tunnel:
            with psycopg2.connect(
                        user=self.user, password=self.password,host=tunnel.local_bind_host, port=tunnel.local_bind_port) as conn:
                df.to_sql(tablename, con=conn, index=False, if_exists='replace')

    def write_pandas_df(self, data_frame,table_name,schema,column_dtypes=None,if_exists='replace'):
        with sshtunnel.open_tunnel((self.ssh_host, self.ssh_port),
                                        ssh_username=self.ssh_user,
                                        ssh_pkey=self.ssh_pkey,
                                        remote_bind_address=(self.host, self.port),
                                        local_bind_address=('localhost', 47122)
                                        ) as tunnel:
            '''with psycopg2.connect(
                        user=self.user, password=self.password, port=tunnel.local_bind_port) as conn:'''

            conn = create_engine('postgresql+psycopg2://%s:%s@%s:%s/%s' %
                                (self.user,self.password, tunnel.local_bind_host, tunnel.local_bind_port, self.database), echo=True)
            try:
                print('In write pandas...')
                data_frame.to_sql(chunksize=10000,name=table_name,
                                    schema=schema,
                                    con=conn,
                                    if_exists=if_exists,
                                    index=False,dtype=column_dtypes)
                
            except Exception as e:
                raise RuntimeError(e)
           