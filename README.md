# CherryPyformance

A two part tool for monitoring callstack statistics of CherryPy appliations and analysing/displaying these statistics.

## Client

The client is a small-footprint package to be imported into your application, the configuration you supply will dictate which CherryPy handlers, which functions and which application database will be wrapped for callstack and SQL profiling. The configuration also controls how often these collected stats are flushed and where they are pushed to.

It works by subscribing to the CherryPy engine at startup, wrapping listed functions, wrapping database connection objects and activating custom CherryPy tools which wrap cherrypy handlers. It then collects cProfile callstack data and SQL call durations from them. These stats are written to temporary buffers and flushed periodically.

### Client Installation

Run the setup of the client by running the following in the shell/cmd terminal:
```setup.py install```
Then configure the configuration JSON file to suit your application and insert the following lines into your application:
```
import cherry_pyformance
cherry_pyformance.initialise()
```

### Client Requirements
* Python 2.6/7
* CherryPy

## Server

The server is a CherryPy application which accepts stats pushed by the cherry_pyformance client storing them for analysis and browsing.

### Server Instructions
Go to the server directory.
Copy 'server_config.json.template' to 'server_config.json'.
Enter your database and server details into the config file.
Then run
```
python stats_server.py
```

The server UI then runs on the host and port specified in the config.

### Upgrading Server

When you recieve a new version of the server side of cherry pyformance, you may need to update your database if you wish to keep your old data. To do this, you will need to use Alembic to migrate your database. First, look in the 'alembic.ini' file for the 'sqlalchemy.url' line and modify the argument to have your database's username/password where it says 'username' and 'password'. This simply run
```
alembic upgrade head
```
from the command line to upgrade your database to the latest version.

### Server Requirements
* Python 2.6/7
* CherryPy
* Mako
* SQLAlchemy
* Alembic
* SQLParse
