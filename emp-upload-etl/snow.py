import logging
import re
import os, sys
import json
import pandas as pd
import secrets
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import exc
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class Snowflake:
    """
    Wrapper for all snowflake calls.  Handles connections, parsing results
    and errors in a consistent way.   Can parse multi-sql statments where
    statements are separated by semi-colons

    Attributes:
        WAREHOUSE: Snowflake warehouse
        DATABASE Snwoflake DB to use as default
        SCHEMA: Snowflake Schema to use as default
    """


    def connect_attributes(self, WAREHOUSE, DATABASE, SCHEMA,ROLE):
        self.ACCOUNT = self.snow_secrets.get_secret_value('SNOW_ACCOUNT')
        self.USER = self.snow_secrets.get_secret_value('USER')
        self.PASSWORD = self.snow_secrets.get_secret_value('PASS')
        self.DATABASE = DATABASE
        self.SCHEMA = SCHEMA
        self.WAREHOUSE = WAREHOUSE
        self.ROLE = ROLE

    def define_engine(self):
        self.engine = create_engine(
            'snowflake://%s:%s@%s/%s/%s?warehouse=%s&role=%s' % (self.USER,
                self.PASSWORD, self.ACCOUNT, self.DATABASE,
                self.SCHEMA,self.WAREHOUSE,self.ROLE)
        )

    def sql(self, SQL):
        """
        Parse a single SQL statements and return results

        Parameters:
            SQL:  Single SQL statement

        Return:
            results:  SQLAlchemy row proxy with show_results
        """

        results = None
        try:
            connection = self.engine.connect()
            trans = connection.begin()
            if isinstance(SQL, (list, tuple)):
                SQL = SQL[0]
            sql_line = SQL.replace("\n","\n  ")
            sql_line = re.sub(r'CREDENTIALS.*\([^)]*\)', '', sql_line,
                flags=re.IGNORECASE)
            log.info(f"running:\n\n {sql_line}")
            results = connection.execute(SQL).fetchall()
            trans.commit()
            connection.close()
        except exc.SQLAlchemyError as e:
            e = re.sub(r'CREDENTIALS.*\([^)]*\)', '', str(e),flags=re.IGNORECASE)
            log.exception("ERROR %s" % e)
            trans.rollback()
            connection.close()
            raise RuntimeError(e)
        finally:
            connection.close()
        return results

    def sql_multi(self, sql_multi, show_results=True):
        """
        Parse a multi-stanza SQL statement returning a list of results.

        Parameters:
            SQL:  SQL statement that can contain multiple stanzas
            show_results:  Boolean to indicast if results should be printed
                as log info statements.

        Return:
            results:  list of SQLAlchemy row proxies with statement results
        """

        results = []
        try:
            connection = self.engine.connect()
            trans = connection.begin()
            results = []
            for sql in sql_multi:
                sql_line = sql.replace("\n","\n  ") # append space so awslogger will ignore newline
                sql_line = re.sub(r'CREDENTIALS.*\([^)]*\)', '', sql_line,
                    flags=re.IGNORECASE)
                log.info(f"running:\n\n {sql_line}")
                res = connection.execute(sql)
                res = res.fetchall() #return proxy results as list
                results.append(res)
                if show_results == True:
                    try: # print out results
                        res_list = []
                        for r in res:
                            res_list.append(str(r))
                        log.info("\n ".join(res_list))
                    except:
                        log.info(res)

            trans.commit()
            connection.close()
        except exc.SQLAlchemyError as e:
            e = re.sub(r'CREDENTIALS.*\([^)]*\)', '', str(e),flags=re.IGNORECASE)
            log.exception("ERROR %s" % e)
            trans.rollback()
            connection.close()
            raise RuntimeError(e)
        finally:
            connection.close()

        return results

    def write_pandas_df(self, data_frame,table_name,schema,column_dtypes=None,if_exists='append'):
        try:
            connection = self.engine.connect()
            data_frame.to_sql(chunksize=10000,name=table_name,
                                schema=schema,
                                con=connection,
                                if_exists=if_exists,
                                index=False,dtype=column_dtypes)
        except Exception as e:
            raise RuntimeError(e)
        finally:
            connection.close()

    def read_sql_as_pandas_df(self, sql):
        try:
            connection = self.engine.connect()
            df = pd.read_sql(sql, con=connection)
            return df
        except Exception as e:
            raise RuntimeError(e)
        finally:
            connection.close()

    def __init__(
            self,
            WAREHOUSE=None,
            DATABASE=None,
            SCHEMA='PUBLIC',
            SECRET_NAME=None,
            REGION_NAME=None,
            ROLE=None):
        self.snow_secrets = secrets.Secrets(SECRET_NAME, REGION_NAME)
        self.connect_attributes(WAREHOUSE, DATABASE, SCHEMA,ROLE)
        self.define_engine()


if __name__ == "__main__":
           snowflake = Snowflake(WAREHOUSE='COMPUTE_WH', DATABASE='SALESFORCE_DB', ROLE='ETL_LOAD_ROLE', SECRET_NAME='/prod/db/snowflake', REGION_NAME='us-west-2')
           print(snowflake.ACCOUNT)