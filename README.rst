========
Backuper
========

The main idea of the project - provide the universal interface
for doing backups of any databases (RDMBS, NoSQL).

Project is written in Golang. Based on gRPC.

**Services**

- master (server/master)
    Responsible for triggering backups on *minions*.

- minion (server/slave)
    Responsible for running, stopping, scheduling backups.

- dashboard (server)
    Wrapper for *master*. Just web interface for user interactions with *master* service.


**Project structure**

- cmd/
    Main packages for *master*, *minion*, *dashboard*

- configs/
    All configuration related to services.

- internal/
    Implementation of backends.
    Including caching, connection pools, rendering htmls,
    handlers, scheduled cron jobs, RPC interfaces.

- pkg/
    Implementation of plugins.

- web/
    HTML templates for *dashboard*

**How to run**

.. code-block::

    # GOOS=linux -- for cross-compilation use
    go run cmd/master/master.go
    go run cmd/minion/minion.go
    go run cmd/dashboard/dashboard.go
