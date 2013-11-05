# CherryPyformance

A two part tool for monitoring callstack statistics of CherryPy appliations and analysing/displaying these statistics.

## Client

The client is a small-footprint package to be imported into your application, the JSON configuration you supply will dictate which CherryPy handlers and which functions will be wrapped for callstack profiling. The configuration also controls how often these collected stats are flushed and where they are pushed to. Currently stats are either written to disk as JSON or sent to a stats server.

It works by subscribing to the CherryPy engine at startup, wrapping functions and activating custom CherryPy tools which wrap handlers and collecting cProfile callstack data from them. These stats are written to a temporary buffer and flushed periodically.

### Client Installation

Place the stats directory in the root of your application, alter the configuration JSON supplied and insert
```
import stats.stats_profiler
```
into your application.

## Server

The server is a CherryPy application which accepts stats pushed by the CherryPyformance client storing them for analysis and browsing.

