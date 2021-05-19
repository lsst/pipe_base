.. _command-line-task-argument-reference:

####################################
Command-line task argument reference
####################################

This page describes the command-line arguments and environment variables common to command-line tasks.

Signature and syntax
====================

The basic call signature of a command-line task is:

.. code-block:: text

   task.py REPOPATH [@file [@file2 ...]] [--output OUTPUTREPO | --rerun RERUN] [named arguments]

See :ref:`command-line-argument-files` for details on ``@file`` syntax.

For named arguments that take multiple values do not use a ``=`` after the argument name.
For example, ``--config-file foo.py bar.py``, **not** ``--config-file=foo bar``.

Status code
===========

A command-line task returns a status code of ``0`` if all data IDs were successfully processed.
If the command-line task failed to process one or more data IDs, the status code is equal to the number of data IDs that failed.
See also: :option:`--noExit`.

Positional arguments
====================

.. option:: REPOPATH

   **Input Butler data repository URI or path.**

   The input Butler data repository is always the first argument to a command-line task.
   This argument is required for all command-line task runs, except when printing help (:option:`--help`).

   In general, this is a URI that depends on the Butler backend.
   For example, ``swift://host/path`` for a Swift backend or ``file://path`` for a POSIX backend.

   .. TODO DM-11671 Link to Butler docs on URIs.

   For POSIX backends, this may also be an absolute file path or a path relative to the current working directory.
   
   If the :envvar:`PIPE_INPUT_ROOT` environment variable is set, then the ``REPOPATH`` is relative to that.
   See :ref:`command-line-task-envvar-examples`.

   For background, see :doc:`command-line-task-data-repo-howto`.

   See also :option:`--rerun` argument to specify input and output reruns within this Butler repository.

Named arguments
===============

An output data repository must be specified with either :option:`--output` **or** :option:`--rerun`.
Other named arguments are optional.

.. option:: --calib <calib_repo>

   **Calibration data repository URI path.**

   The path may be absolute, relative to the current working directory, or relative to :envvar:`PIPE_CALIB_ROOT` (when set).
   See :ref:`command-line-task-envvar-examples`.

.. option:: -c <name=val>, --config <name=val>

   **Task configuration overrides.**

   The ``-c``/``--config`` argument can appear multiple times.

   See :ref:`command-line-task-config-howto-config` for more information.

.. option:: -C <configfile>, --config-file <configfile>, --configfile <configfile>

   **Task configuration override file(s).**

   The ``-C``/``-config-file``/``--configfile`` argument can appear multiple times.

   See :ref:`command-line-task-config-howto-configfile` for more information.

.. option:: --clobber-config

   **Backup and overwrite existing config files.**

   Normally a command-line task checks existing config files in a Butler repository to ensure that the current configurations are consistent with previous pipeline executions.
   This argument disables this check, which may be useful for development.

   This argument is safe with :option:`-j` multiprocessing, but not necessarily with other forms of parallel execution.

   See :ref:`command-line-task-prov-howto-config` for more information.

.. option:: --clobber-output

   **Remove and re-create the output repository if it already exists.**

   This argument is safe with :option:`-j` multiprocessing, but not necessarily with other forms of parallel execution.

.. option:: --clobber-versions

   **Backup and then overwrite existing package version provenance.**

   Normally a command-line task checks that the Science Pipelines package versions are the same as for previous executions that wrote to an output repository or rerun.
   This argument disables this check, which may be useful for development.

   This argument is safe with :option:`-j` multiprocessing, but not necessarily with other forms of parallel execution.

   See :ref:`command-line-task-prov-howto-versions` for more information.

.. option:: -h, --help

   **Print help.**

   The help is equivalent to this documentation page, describing command-line arguments.
   This help does not describe the command-line task's specific functionality.

.. option:: --id [[<dataid>] ...]

   **Butler data IDs.**

   Specify data IDs to process using data ID syntax.
   For example, ``--id visit=12345 ccd=1,2^0,3``.
   For more information, see :ref:`command-line-task-dataid-howto`.

   An ``--id`` argument without values indicates that **all** data available in the input repository will be processed (see :ref:`command-line-task-dataid-howto-wildcard`).

   For many-to-one processing tasks the ``--id`` argument specifies **output** data IDs, while :option:`--selectId` is used for **input** data IDs.

   The ``--id`` argument can appear multiple times.
   See :ref:`command-line-task-dataid-howto-multi-arg`.

.. option:: -L <level|component=level> [level|component=level...], --loglevel <level|component=level> [level|component=level...]

   **Log level.**

   Supported levels are: ``trace``, ``debug``, ``info``, ``warn``, ``error``, or ``fatal``.

   Log levels can be set globally (``-L debug``) or for a specific named logger (``-L pipe.base=debug``).

   Specify multiple arguments to control the global and named logging levels simultaneousy (``-L warn pipe.base=debug``).

   The ``-L``/``--loglevel`` argument can appear multiple times.

   For more information, see :ref:`command-line-task-logging-howto`.

.. option:: --longlog

   **Enable the verbose logging format.**

   See :ref:`command-line-task-logging-howto-longlog` for more information.

.. option:: --debug

   **Enable debugging mode.**

   Some tasks support debug variables that can be set, while running from the command line, to display additional information.
   Each task documents which debug variables it supports.
   See `the lsstDebug documentation <http://doxygen.lsst.codes/stack/doxygen/x_masterDoxyDoc/base_debug.html>`_ for information about how to enable specific debug variables using a :file:`debug.py` file.

   .. TODO DM-11675 cross-link to debug framework docs in lsst.base module.

.. option:: --doraise

   **Raise an exception on error.**

   This mode causes the task to exit early if it encounters an error, rather than logging the error and continuing.

.. option:: --no-backup-config

   **Disable copying config to file~N backup.**

.. option:: --no-versions

   **Disable package version consistency validation.**

   This mode permits data processing even if outputs exist in the output data repository or rerun from a different version of Science Pipelines packages.

   This mode is useful for development should not be used in production processing.

   See also :option:`--clobber-versions`.

   See :ref:`command-line-task-prov-howto-versions` for more information.

.. option:: --output <output_repo>

   **Output data repository URI or path.**

   The output data repository will be created if it does not exist.

   The path may be absolute, relative to the current working directory, or relative to :envvar:`PIPE_CALIB_ROOT` (when set).
   See :ref:`command-line-task-envvar-examples`.

   ``--output`` may not be used with the :option:`--rerun` argument.

   See :doc:`command-line-task-data-repo-howto` for background.

.. option:: -j <processes>, --processes <processes>

   **Number of processes to use.**

   When processes is larger than 1 the task uses the Python `multiprocessing` module to parallelize processing of multiple datasets across multiple processors.

   See also :option:`--timeout`.

   See :ref:`command-line-task-parallel-howto` for more information.

.. option:: --profile <profile>

   **Dump cProfile statistics to the named file.**

   See the cProfile_ documentation.

.. option:: --rerun <[input:]output>

   **Specify output rerun (and optionally the input rerun as well).**

   Reruns are data repositories relative to the root repository, :option:`REPOPATH`.
   ``--rerun output`` is equivalent to ``--output REPOPATH/rerun/output``.

   An input rerun can also, optionally, be specified.
   ``--rerun input:output`` sets the input repository path to ``REPOPATH/rerun/input`` the output repository path to ``REPOPATH/rerun/output``.

   If an argument to `--rerun` starts with a `/`, it will be interpreted as an absolute path rather than as being relative to the root input data repository.

   The arguments supplied to `--rerun` may refer to symbolic links to directories.
   Data will be read or written from the links' targets.

   See :doc:`command-line-task-data-repo-howto` for more information.

.. option:: --show <config|history|data|tasks|run>

   **Print metadata without processing.**

   Permitted values are:

   - ``config``: show configuration state; add ``=PATTERN`` to limit to configuration entries matching the glob pattern.

   - ``history=PATTERN``: show where the configuration entries that match the glob pattern were set.

   - ``data``: show data IDs resolved by the :option:`--id` argument.

   - ``tasks``: show sub-tasks run by the command-line task.

   Multiple values can be shown at once.
   For example, ``--show config data``.

   Normally the command-line task will exit before processing any data.
   If you want to *also* run the task after showing metadata, append the ``run`` value.
   For example, ``--show config data run``.

.. option:: --selectId

   **Input data IDs for many-to-one tasks.**

   For many-to-one processing tasks, such as coaddition, the :option:`--selectId` argument is used to specify input data IDs, while :option:`--id` is used to specify *output* data IDs.
   The syntax for :option:`--selectId` is identical to that of :option:`--id`.

   For more information about dataId selection syntax, see :ref:`command-line-task-dataid-howto`.

.. option:: -t timeout, --timeout timeout

   **Multiprocessing timeout (in seconds).***

   See also :option:`-j`.

.. option:: --noExit

   **(Advanced) prevent the command-line task from exiting directly to the shell with a non-zero status code if there are one or more processing failures.**

   If there are failures, by default, a command-line task exits to the directly shell with a status code equal to the number of data IDs that it failed to process.
   This means that the command-line task does not return to the :ref:`run script <creating-a-command-line-task-run-script>` that originally called the `lsst.pipe.base.CmdLineTask.parseAndRun` method if there is an error.
   Some command-line tasks (such as the MPI-enabled scripts in ``pipe_drivers``) need `lsst.pipe.base.CmdLineTask.parseAndRun` to always return to the run script.
   In that case, use this ``--noExit`` argument.

   When ``--noExit`` is used, the command-line task will not exit to the shell from `lsst.pipe.base.CmdLineTask.parseAndRun` if failures are encountered.
   Instead, it will return normally to the run script that called `~lsst.pipe.base.CmdLineTask.parseAndRun`.
   In this case, it is up to the run script to set an appropriate shell status code.

   See also :option:`--doraise`.

.. _command-line-argument-files:

Argument files
==============

Arguments can be written to a plain text file and referenced with an ``@filepath`` command-line argument.
The contents of argument files are identical to what you'd write on the command line, with these rules:

- Text can be split across multiple lines.
  For example, you can have one argument per line.

- Do not use ``\`` as a continuation character.

- Include comments with a ``#`` character.
  Content on a line after the ``#`` character is ignored.

- Blank lines and lines starting with ``#`` are ignored.

You can mix argument files with other command-line arguments (including additional :option:`--id` and :option:`--config` arguments).

You can include multiple ``@filepath`` references in the same command.

Example
-------

For example, the file :file:`foo.txt` contains:

.. code-block:: text

   --id visit=54123^55523 raft=1,1^2,1 # data ID
   --config someParam=someValue --config-file configOverrideFilePath

You can then reference it with ``@foo.txt``, along with additional command-line arguments:

.. code-block:: bash

   task.py repo @foo.txt --config anotherParam=anotherValue --output outputPath

.. _command-line-task-envvar:

Environment variables
=====================

The :envvar:`PIPE_INPUT_ROOT`, :envvar:`PIPE_CALIB_ROOT`, and :envvar:`PIPE_OUTPUT_ROOT` environment variables let you more easily specify Butler data repositories.

Each environment variable is used as a root directory for relative paths provided on the command line.
If you set an absolute path on the command line, the environment variable is ignored.
:ref:`see examples <command-line-task-envvar-examples>`.

.. The default value for each of these environment variables is the current working directory.

.. envvar:: PIPE_INPUT_ROOT

   Root directory for the input Butler data repository argument, :option:`REPOPATH`.

.. envvar:: PIPE_CALIB_ROOT

   Root directory for the calibration Butler data repository argument (--calib).

.. envvar:: PIPE_OUTPUT_ROOT

   Root directory for the output Butler data repository argument (--output).

.. _command-line-task-envvar-examples:

Path environment variable examples
----------------------------------

These examples feature :envvar:`PIPE_INPUT_ROOT` to help specify the input data repository along with :option:`REPOPATH`, which is the first positional argument of any command.

1. The data repository path is :file:`$PIPE_INPUT_ROOT/DATA` (or :file:`DATA` if :envvar:`PIPE_INPUT_ROOT` is undefined):
   
   .. code-block:: bash

      processCcd.py DATA [...]

2. The data repository path is :file:`$PIPE_INPUT_ROOT` (or current working directory if :envvar:`PIPE_INPUT_ROOT` is undefined):

   .. code-block:: bash

      processCcd.py . [...]

3. The data repository path is an absolute path:
   
   .. code-block:: bash

      processccd.py /DATA/a [...]

   :envvar:`PIPE_INPUT_ROOT` is ignored in this case:

The same behavior applies to the named arguments:

- :option:`--calib` with :envvar:`PIPE_CALIB_ROOT`.
- :option:`--output` with :envvar:`PIPE_OUTPUT_ROOT`.

.. _cProfile: https://docs.python.org/library/profile.html
