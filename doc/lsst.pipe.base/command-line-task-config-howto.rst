.. _command-line-task-config-howto:

##############################
Configuring command-line tasks
##############################

:ref:`Command-line tasks <using-command-line-tasks>` are highly configurable.
This page describes how to review configurations with :option:`--show config <--show>`,  and change configurations on the command line with :option:`--config`, :option:`--config-file`, or :option:`--configfile`.

One special category of configuration is subtask retargeting.
Retargeting uses concepts discussed on this page, but with additional considerations described in :doc:`command-line-task-retargeting-howto`.

.. _command-line-task-config-howto-show:

How to show a command-line task's current configuration
=======================================================

The :option:`--show config <--show>` argument allows you to review the current configuration details for command-line tasks. 

.. tip::

   :option:`--show config <--show>` puts the command-line task in a dry-run mode where no data processed.
   Once you're done reviewing configurations, remove the :option:`--show` argument from the command-line task invocation to actually process data.

   Alternatively, add ``run`` to the :option:`--show` argument to show configurations *and* process data: :option:`--show config run <--show>`.

.. _command-line-task-config-howto-show-all:

Viewing all configurations
--------------------------

Use :option:`--show config <--show>` to see all current configurations for a command-line task.
For example:

.. code-block:: bash

   processCcd.py REPOPATH --output output --show config

Note that both the input and output Butler data repositories repository must be set to show camera-specific task configuration defaults.
Instead of an :option:`--output` argument you can specify a :option:`--rerun`, see :doc:`command-line-task-data-repo-howto`.

.. _command-line-task-config-howto-show-subset:

Viewing a subset of configurations
----------------------------------

To filter the :option:`--show config <--show>` output, include a search term with wildcard matching (``*``) characters.
For example, this will show any configuration that includes the string ``calibration``:

.. code-block:: bash

   processCcd.py REPOPATH --output output --show config="*calibration*"

.. _command-line-task-config-howto-config:

How to set configurations with command-line arguments
=====================================================

Command-line tasks can be configured through a combination of two mechanisms: arguments on the command line (:option:`--config`) or through configuration files (:option:`--config-file` or :option:`--configfile`).
In general, simple configurations can be made through the command line, while complex configurations and :ref:`subtask retargeting <command-line-task-retargeting-howto>` must done through configuration files (see :ref:`command-line-task-config-howto-configfile`).

To change a configuration value on the command line, pass that configuration name and value to the :option:`--config` argument.
For example, to set a configuration named ``skyMap.projection`` to a value ``"TAN"``:

.. code-block:: bash

   task.py REPOPATH --output output --config skyMap.projection="TAN"

You can provide multiple :option:`--config` arguments on the same command line or set multiple configurations with a single :option:`--config` argument:

.. code-block:: bash

   task.py REPOPATH --output output --config config1="value1" config2="value2"

Only simple configuration values can be set through :option:`--config` arguments, such as:

- **String values**. For example: ``--config configName="value"``.
- **Scalar numbers**. For example: ``--config configName=2.5``.
- **Lists of integers**. For example: ``--config intList=2,4,-87``.
- **List of floating point numbers**. For example: ``--config floatList=3.14,-5.6e7``.
- **Boolean values**. For example: ``--config configName=True configName2=False``.

Specific types of configurations you **cannot** perform with the :option:`--config` argument are:

- You cannot :doc:`retarget a subtask <command-line-task-retargeting-howto>` specified by a `lsst.pex.config.ConfigurableField` (which is the most common case).
- For items in registries, you can only specify values for the active (current) item.
  See :ref:`command-line-task-retargeting-howto-registry-active-config`.
- You cannot specify values for lists of strings.
- You cannot specify a subset of a list.
  You must specify all values at once.

For these more complex configuration types you must use configuration files, which are evaluated as Python code.

.. _command-line-task-config-howto-configfile:

How to use configuration files
==============================

You can also provide configurations to a command-line task through a *configuration file*.
In fact, configuration files are Python modules; anything you can do in Python you can do in a configuration file.

Configuration files give you full access to the configuration API, allowing you to import and :doc:`retarget subtasks <command-line-task-retargeting-howto>`, and set configurations with complex types.
These configurations can only be done through configuration files, not through command-line arguments.

Use a configuration file by providing its file path through a :option:`-C`/:option:`--config-file`/:option:`--configfile` argument:

.. code-block:: bash

   task.py REPOPATH --output output --config-file taskConfig.py

Multiple configuration files can be provided through the same :option:`--config-file` argument and the :option:`--config-file` argument itself can be repeated.

In a configuration file, configurations are attributes of a ``config`` object.
If on the command line you set a configuration with a ``--config skyMap.projection="TAN"`` argument, in a configuration file the equivalent statement is:

.. code-block:: python

   config.skyMap.projection = "TAN"

``config`` is the root configuration object for the command-line task.
Settings for the command-line task itself are attributes of ``config``.
In that example, ``config.skyMap`` is a subtask and ``projection`` is a configuration of that ``skyMap`` subtask.

.. _command-line-task-config-howto-obs:

About configuration defaults and camera configuration override files
====================================================================

Command-line task configurations are a combination of configurations you provide and defaults from the observatory package and the task itself.

When a command-line task is run, it loads two camera-specific configuration files, if found: one for the observatory package, and one for a specific camera defined in that observatory package.
For an example observatory package named ``obs_package``, these configuration override files are, in order:

- ``obs_package/config/taskName.py`` (overrides for an observatory package in general).
- ``obs_package/config/cameraName/taskName.py`` (overrides for a specific camera, named “\ ``cameraName``\ ”).

The ``taskName`` is the command-line task, such as ``processCcd`` for :command:``processCcd.py`` (this is the `lsst.pipe.base.CmdLineTask._DefaultName` class variable).

Here are two examples:

- :file:`obs_lsstSim/config/makeCoaddTempExp.py`: specifies which version of the image selector task to use for co-adding LSST simulated images using the ``obs_lsstSim`` observatory package.
- :file:`obs_subaru/config/hsc/isr.py``: provides overrides for the instrument signature removal (aka detrending) task for the ``hsc`` camera (Hyper Suprime-Cam) in the ``obs_subaru`` observatory package.

Overall, the priority order for setting task configurations is configurations is (highest priority first):

1. User-provided :option:`--config` and :option:`--config-file` arguments (computed left-to-right).
2. Camera specific configuration override file in an observatory package.
3. General configuration override file in an observatory package.
4. Task defaults.

.. TODO DM-11687 include an example with --show history to see configuration histories.
