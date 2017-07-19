.. _command-line-task-prov-howto:

####################################################
Working with provenance checks in command-line tasks
####################################################

:ref:`Command-line tasks <using-command-line-tasks>` include provenance checks to ensure datasets in a Butler repository are processed with consistent software versions and task configurations.
This page provides background on what these provenance checks are, and how they can be overridden when running command-line tasks.

.. _command-line-task-prov-howto-config:

How to override configuration checks with the :option:`--clobber-config` argument
=================================================================================

When a command-line task outputs datasets it also persists the task's configuration to the Butler output repository.
If the same command-line task is later run on the same Butler output repository, that task will ensure that the configuration is identical to the earlier run's configuration.
If the current and previous task configurations are different, the command-line task will abort with an error:

.. code-block:: text

   Config does not match existing task config <name> on disk; tasks configurations must be consistent within the same output repo (override with --clobber-config)

This check ensures that datasets in a Butler repository are uniformly processed.

You may often experiment with different task configurations while prototyping a pipeline.
The best way to avoid configuration conflicts is to run each experimental command-line task execution with a different output :ref:`rerun <command-line-task-data-repo-using-reruns>`.
Creating separate reruns gives you the ability to later analyze how dataset differ with task configuration.
See :ref:`command-line-task-data-repo-using-reruns` for details on how to do this.

If you cannot create a new output rerun, and must re-use an existing data repository, you can overwrite (clobber) existing task configuration metadata with the current configurations to bypass the consistency check.
Use the :option:`--clobber-config` argument to do so:

.. code-block:: bash

   task.py REPOPATH --output output --id --clobber-config ...

.. warning::

   :option:`--clobber-config` is subject to race conditions because it is modifying a file in the Butler repository.
   :option:`--clobber-config` is safe to use with :option:`-j`/:option:`--processes` but not other forms of parallel processing.

.. _command-line-task-prov-howto-versions:

How to override software version checks with :option:`--no-versions` or :option:`--clobber-versions`
====================================================================================================

If a command-line task attempts to write datasets to a Butler output repository that already contains data processed with a different version of LSST Science Pipelines software, a "``Version mismatch``" error is triggered.
This error should be heeded in most cases because datasets created by one version of the LSST Science Pipelines may not be compatible with another version.
Developers may need to suppress this error during testing, though.
There are two ways to do this: :option:`--no-versions`, and :option:`--clobber-versions`.

Use :option:`--no-versions` to simply bypass version checking:

.. code-block:: bash

   task.py REPOPATH --output output --id --no-versions ...

This argument is thread-safe and likely ideal for developers.

An alternative method of bypassing software version provenance checks is the :option:`--clobber-versions` argument:

.. code-block:: bash

   task.py REPOPATH --output output --id --clobber-versions ...

Like :option:`--clobber-config`, the :option:`--clobber-versions` argument replaces software version provenance information in the Butler data repository with current version information.
This argument is useful if you wish to continue using an existing Butler data repository with a different version of the Science Pipelines, and are confident that there are not dataset incompatibilities.

.. warning::

   :option:`--clobber-versions` is subject to race conditions because it is modifying a file in the Butler repository.
   :option:`--clobber-versions` is safe to use with :option:`-j`/:option:`--processes` but not other forms of parallel processing.

.. seealso::

   For details about how software version information is retrieved, see the `lsst.base.packages` documentation.
