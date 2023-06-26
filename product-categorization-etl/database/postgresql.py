# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 14:47:40 2020

Connects to Postgres db hosted on EC2 via Bastion Stunnel

@author: Natasha.Polishchuk
"""

import psycopg2 #prereqs python3-dev and libpq-dev may be required to run on Lambda
from database import Database
from sqlalchemy import create_engine
import sshtunnel
import pandas as pd


class pg(Database):
    """
    Wrapper for postgres calls using private key.  Handles connections, parsing results
    and errors in a consistent way.   Can parse multi-sql statements where
    statements are separated by semi-colons
    """

    def __init__(
            self, host=None, user=None, password=None, port=None,
            database=None, ssl_mode=None, ssl_cert=None, ssl_key=None,
            ssh_user=None, ssh_host=None, ssh_port=None, ssh_pkey=None, driver='PostgreSQL'):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database = database
        self.driver = driver
        #self.ssl_mode = ssl_mode
        #self.ssl_cert = ssl_cert
        #self.connect_attributes(ssl_mode, ssl_cert, ssl_key)
        #self.ssl_key = ssl_key
        
        self.ssh_user = ssh_user
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_pkey = ssh_pkey
        
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
                                       local_bind_address=('0.0.0.0', 10022)
                                       ) as tunnel:
            with psycopg2.connect(
                        user=self.user, password=self.password, port=tunnel.local_bind_port) as conn:
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
                                       local_bind_address=('0.0.0.0', 10022)
                                       ) as tunnel:
            with psycopg2.connect(
                        user=self.user, password=self.password, port=tunnel.local_bind_port) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                