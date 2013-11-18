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
cherry_pyformance.initialise('path/to/config.json')
```

### Client Requirements
* Python 2.6/7
* CherryPy
* SQLParse

## Server

The server is a CherryPy application which accepts stats pushed by the cherry_pyformance client storing them for analysis and browsing.

### Server Instructions
Go to the server directory and run
```
python stats_server.py <db_username> <db_password> <server_host> [<server_port>]
```
(or follow the help instrustions by running `python stats_server.py --help`)

### Server Requirements
* Python 2.6/7
* CherryPy
* Mako
* SQLAlchemy
