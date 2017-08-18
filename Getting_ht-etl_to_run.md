## Lukas notes on getting ht-etl to run on Windows 10

* Follow instructions in [README.md](https://github.com/anidata/ht-etl/blob/master/README.md)
* In Anaconda Navigator GUI
    * Make & activate a Python 2.7 virtual environment
    * Install **pandas 0.20.3**
    * Install **luigi 2.3**
    * Install **psycopg2 2.7.1**
    * Install **sqlalchemy 1.1.13**
    * Install **networkx 1.11**
* Open the ht-etl Python files in Spyder
    (make sure Spyder loads under the python 2.7 environment within Anaconda Navigator)
* In Spyder's IPython console: ```!pip install -e C:\\l\\Anidata\\ht-etl``` (your filepath to top level of ht-etl directory)

### I opted to connect to a PostgreSQL server running on localhost instead of Docker.

* Download and unzip ```crawler_er.tar.gz``` from (https://github.com/anidata/ht-archive)
    * To unzip, first install [7-Zip](http://www.7-zip.org/), then right-click on the file and select 7-Zip
    * You have to first unzip the .gz, then the .tar.
    * Result should be a ```crawler.sql``` file.
* [Download & install PostgreSQL](https://www.postgresql.org/download/)
    * **Save the password it makes you enter during installation process! We'll call this ```your_password``` below**
* In PGAdmin
    * Server -> PostgreSQL 9.6 (top left in PGAdmin window)
    * Password: ```your_password```
    * PostgreSQL 9.6 -> Databases -> Create -> Database, call it "crawler"
* Open psql shell (it's under Windows Start menu -> PostgreSQL 9.6 -> SQL Shell (psql))
* In psql shell
    * Server=localhost
    * Database=**crawler** (NOT postgres)
    * Port=5432
    * Username=postgres
    * Password=```your_password```
* In the psql shell
    * Prompt should now be ```crawler=#```
    * ```CREATE ROLE dbadmin WITH SUPERUSER LOGIN PASSWORD '1234';```
    * ```\i 'C:/l/Anidata/crawler.sql';``` -> path is wherever you saved ```crawler.sql```


Not sure if I need to be inside the ht-etl directory (in IPython Console) for next step to work

Note: below, "your_password" is the password that PostgreSQL makes you enter during installation process.

In IPython Console: ```!luigi --module htetl.main_jobs LoadEntityIds --host localhost --database crawler --user postgres --password your_password --local-scheduler```

Not sure why it didn't use luigi.cfg as they said I should do in the README.md
