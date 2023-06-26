# Steps to generate MSSql ODBC libs for lambda
use https://github.com/lambci/docker-lambda to simulate a lambda environment
```
$ docker run -it --rm --entrypoint bash -e ODBCINI=/opt/odbc.ini -e ODBCSYSINI=/opt/ lambci/lambda:build-python3.6
```

download and install unixODBC from http://www.unixodbc.org/download.html
```
$ curl ftp://ftp.unixodbc.org/pub/unixODBC/unixODBC-2.3.7.tar.gz -O
$ tar xzvf unixODBC-2.3.7.tar.gz
$ cd unixODBC-2.3.7
$ ./configure --sysconfdir=/opt --disable-gui --disable-drivers --enable-iconv --with-iconv-char-enc=UTF8 --with-iconv-ucode-enc=UTF16LE --prefix=/opt
$ make
$ make install
$ cd ..
$ rm -rf unixODBC-2.3.7 unixODBC-2.3.7.tar.gz
```
download and install ODBC driver for MSSQL 17
https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-2017
```$ curl https://packages.microsoft.com/config/rhel/6/prod.repo > /etc/yum.repos.d/mssql-release.repo
$ yum install e2fsprogs.x86_64 0:1.43.5-2.43.amzn1 fuse-libs.x86_64 0:2.9.4-1.18.amzn1 libss.x86_64 0:1.43.5-2.43.amzn1
$ ACCEPT_EULA=Y yum install msodbcsql17 --disablerepo=amzn*
$ export CFLAGS="-I/opt/include"
$ export LDFLAGS="-L/opt/lib"
$ cd /opt
$ cp -r /opt/microsoft/msodbcsql17/ .
$ rm -rf /opt/microsoft/
```

install pyodbc for use with python.
Notice the folder structure to support python 3.7 runtime
https://docs.aws.amazon.com/lambda/latest/dg/configuration-layers.html#configuration-layers-path
```
$ mkdir /opt/python/
$ cd /opt/python/
$ pip install pyodbc -t .
$ pip install sqlalchemy -t .

$ cd /opt
$ cat <<EOF > odbcinst.ini
[ODBC Driver 17 for SQL Server]
Description=Microsoft ODBC Driver 17 for SQL Server
Driver=/opt/msodbcsql17/lib64/libmsodbcsql-17.5.so.2.1
UsageCount=1
EOF

$ cat <<EOF > odbc.ini
[ODBC Driver 17 for SQL Server]
Driver = ODBC Driver 17 for SQL Server
Description = My ODBC Driver 17 for SQL Server
Trace = No
EOF

$ cd /opt
$ zip -r9 ~/pyodbc-layer.zip .
```

Time to test to test it locally:
unzip the content of your layer to your local environment, to /var/opt/ for example:
```
unzip pyodbc-layer.zip -d $HOME/test/opt
```
In your local environment, have your lambda function handy at $HOME/test/task/lambda_function.py
```
from sqlalchemy import create_engine
import pyodbc


def lambda_handler(event, context):
    engine = create_engine('mssql+pyodbc://username:password'
                           '@hob-labels-prod.cjcyi9ibddks.us-east-2.rds.amazonaws.com:1433/hob_labels'
                           '?driver=ODBC Driver 17 for SQL Server')
    conn = engine.connect()
    print(engine.table_names())
    return {"Execution": "Success"}


if __name__ == "__main__":
    """Main method for local testing of lambda function"""
    lambda_handler(None, None)
```

```
docker run --rm -v $HOME/test/task:/var/task -v $HOME/test/opt:/opt lambci/lambda:python3.6 lambda_function.lambda_handler '{"some": "event"}'
```