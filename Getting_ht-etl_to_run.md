## Getting ht-etl email parser to run from scratch

**These instructions are for Windows 10, running Python 2.7 with Anaconda (and Spyder editor with IPython console - bundled with Anaconda),
  and a PostgreSQL server running on localhost instead of Docker (because Lukas still had trouble getting Docker to work).**

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
* In Spyder's IPython console: ```!pip install -e C:\\your\\path\\to\\ht-etl```

**Better way: open the Anaconda prompt, type "activate python_27" (if that's what your python 2.7 env was called),
 then when you see the python_27 in the prompt, THEN do the pip install and then run Luigi from command prompt (after doing the chdir)
 for some reason my IPython console still ran the cmd under Python 3.6 even though Spyder was running under 2.7.
 Look up why this is the case when you do the ! in IPython Console - does it not remember command state from one command to next?**

### I opted to connect to a PostgreSQL server running on localhost instead of Docker.

* Download and unzip ```crawler_er.tar.gz``` from (https://github.com/anidata/ht-archive)
    * To unzip, first install [7-Zip](http://www.7-zip.org/), then right-click on the file and select 7-Zip.
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
    * Prompt should now be ```crawler=#```
    * ```CREATE ROLE dbadmin WITH SUPERUSER LOGIN PASSWORD '1234';```
    * ```\i 'C:/your/path/to/crawler.sql';```
* In IPython console, navigate to ht-etl directory and run luigi.
    * ```import os```
    * ```os.chdir('C:\\your\\path\\to\\ht-etl')```
    * ```!luigi --module htetl.main_jobs ParseEmails --host localhost --database crawler --user postgres --password your_password --local-scheduler```
    * It should say the something like the following. Although this shows Luigi "worked" and was able to access the database, it didn't
      run any tasks because Luigi output() and run() methods were not specified in LoadEntityIds.
```
        Did not run any tasks
        This progress looks :) because there were no failed tasks or missing external dependencies
```

# when I run Luigi on command line using '!' in IPython console, does it run under Python 3.6 instead of Python 2.7 as I thought it did?
# TODO try running Luigi from the IPython console directly using the if name=='main' described in Anidata startup demo workshop

Luigi [parameter naming gotcha](http://luigi.readthedocs.io/en/stable/command_line.html)
"Note that if a parameter name contains ‘_’, it should be replaced by ‘-‘. For example, if MyTask had a parameter called ‘my_parameter’:
    ```$ luigi --module my_module MyTask --my-parameter 100 --local-scheduler```"