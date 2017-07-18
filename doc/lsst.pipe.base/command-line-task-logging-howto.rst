.. _command-line-task-logging-howto:

###############################
Logging with command-line tasks
###############################

:ref:`Command-line tasks <using-command-line-tasks>` generate logging output that can be used to monitor and help debug processing.
This page describes some features and patterns for customizing and taking advantage of the logging output.

.. _command-line-task-logging-howto-level:

How to set logging level in general
===================================

In general, you can change the logging level with the :option:`--loglevel` (:option:`-L`) argument.
For example, to reduce the verbosity of logging to only including warnings and more severe messages:

.. code-block:: bash

   task.py REPOPATH --output output --id --loglevel warn

.. _command-line-task-logging-howto-logger-level:

How to set the logging level for a specific logger
==================================================

To change the logging level for a specific logger, use the ``--loglevel logger=level`` syntax.
For example, to enable debug-level logging for the ``processCcd.calibrate`` logger:

.. code-block:: bash

   processCcd.py REPOPATH --output output --id --loglevel processCcd.calibrate=debug

You may provide multiple values to multiple values to :option:`--loglevel` to specify the level of multiple loggers, along with the global level.
For example, to set the default log level to ``warn``, but enable ``info``-level logging for the ``processCcd.calibrate`` logger:

.. code-block:: bash

   processCcd.py REPOPATH --output output --id --loglevel warn processCcd.calibrate=info

Equivalently, you can also specify multiple :option:`--loglevel` arguments:

.. code-block:: bash

   processCcd.py REPOPATH --output output --id --loglevel warn --loglevel processCcd.calibrate=info

.. _command-line-task-logging-howto-longlog:

Using the verbose logging format
================================

Set the :option:`--longlog` argument to get a verbose logging message.
This format is especially useful for parallel data processing since the data IDs of the data being processed are included in each message to let you later separate the processing streams.
An example message looks like:

.. code-block:: text

   INFO  2017-07-18T04:40:14.014 processCcd.calibrate ({'taiObs': '2013-11-02', 'pointing': 671, 'visit': 904014, 'dateObs': '2013-11-02', 'filter': 'HSC-I', 'field': 'STRIPE82L', 'ccd': 1, 'expTime': 30.0})(calibrate.py:545)- Photometric zero-point: 30.674190

Fields are:

- Log level.
- Timestamp.
- Logger name (the Python namespace, without the root ``lsst`` package).
- Fully-qualified data ID.
- Code module and source line.
- Log message.

.. _command-line-task-logging-howto-tee:

How to log to the console and a file simultaneously
===================================================

Logging output from command-line tasks goes to standard output (stdout) and standard error (sterr) console streams.
There isn't a built-in facility to stream logging output to the console and a file simultaneously.
But you can use the common program :command:`tee` to simultaneous send logging output to both the console and a file.

In a :command:`bash` or :command:`csh`-like shell, simply append a command-like task invocation with ``|& tee filename.log``.
For example:

.. code-block:: bash

   task.py REPOPATH --output output --id |& tee filename.log
