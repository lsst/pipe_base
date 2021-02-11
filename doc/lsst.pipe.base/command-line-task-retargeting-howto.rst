.. FIXME DM-11558 re-address this topic with DM-11558 to improve accuracy.
.. See also questions in https://github.com/lsst/pipe_base/pull/37/files#diff-7be10bd28b721e80b8ced2d45c26d119

.. _command-line-task-retargeting-howto:

##########################################
Retargeting subtasks of command-line tasks
##########################################

Subtasks of :ref:`command-line tasks <using-command-line-tasks>` are dynamically retargetable, meaning that you can configure which task class is run by a parent class.
Subtask retargeting is a special case of :ref:`command-line task configuration <command-line-task-config-howto>`.

A common use of retargeting is to change a default subtask for a camera-specific one.
For example:

- `lsst.obs.subaru.isr.SuprimeCamIsrTask`: a version of instrument signature removal (ISR or detrending) for Suprime-Cam and Hyper Suprime-Cam.
- `lsst.obs.sdss.selectSdssImages.SelectSdssImagesTask`: an version of the task that selects images for co-addition of SDSS stripe 82 images.

How you retarget a subtask and override its config parameters depends on whether the task is specified as an `lsst.pex.config.ConfigurableField` (the most common case) or as an `lsst.pex.config.RegistryField`.
Both scenarios are described on this page.

.. _command-line-task-retargeting-howto-show-subtasks:

Viewing configured subtasks
===========================

To see what subtasks are currently configured to run with a command-line task, use the :option:`--show tasks <--show>` argument (this runs the command-line task in a dry-run mode that shows tasks but does not process data).
For example:

.. code-block:: bash

   processCcd.py REPOPATH --output output --show tasks

An example of the printed output is:

.. code-block:: text

   sub-tasks:
   calibrate: lsst.pipe.tasks.calibrate.CalibrateTask
   calibrate.applyApCorr: lsst.meas.base.applyApCorr.ApplyApCorrTask
   calibrate.astromRefObjLoader: lsst.meas.algorithms.loadIndexedReferenceObjects.LoadIndexedReferenceObjectsTask
   calibrate.astrometry: lsst.meas.astrom.astrometry.AstrometryTask
   calibrate.astrometry.matcher: lsst.meas.astrom.matchOptimisticB.matchOptimisticBContinued.MatchOptimisticBTask
   ...

This subtask hierarchy is interpreted as follows:

- The ``calibrate`` subtask of the command-line task (:command:`processCcd.py`) is configured to use `lsst.pipe.tasks.calibrate.CalibrateTask`.
- ``calibrate`` (again, implemented by `~lsst.pipe.tasks.calibrate.CalibrateTask`) has a subtask named ``astrometry``, which is currently configured to use the `lsst.meas.astrom.astrometry.AstrometryTask` task.
- ``calibrate.astrometry`` has a subtask named ``matcher``, which is implemented by `~lsst.meas.astrom.matchOptimisticB.matchOptimisticBContinued.MatchOptimisticBTask`.

Note that if the ``calibrate.astrometry`` task is retargeted to a different task class, the subtask of ``calibrate.astrometry`` *may* change (for example, ``calibrate.astrometry.matcher`` may no longer exist).

.. _command-line-task-retargeting-howto-configurablefield:

How to retarget a subtask configured as a ConfigurableField with a configuration file
=====================================================================================

To retarget a subtask specified as an `lsst.pex.config.ConfigurableField`, you must use a configuration file (specified by :option:`--config-file` or :option:`--configfile`, see :ref:`command-line-task-config-howto-configfile`).
Inside a configuration file, retargeting is done in two steps:

1. Import the Task class you intend to use.
2. Assign that class to to the subtask configuration using the `~lsst.pex.config.ConfigurableField.retarget` method.

For example, to retarget the subtask named ``configurableSubtask`` with a class ``FooTask``, this configuration file should contain:

.. code-block:: python

   from ... import FooTask
   config.configurableSubtask.retarget(FooTask)

.. TODO make this a realistic example.

Once a subtask is retargeted, you can configure it as normal by setting attributes to configuration parameters.
For example:

.. code-block:: python

   config.configurableSubtask.subtaskParam1 = newValue

.. warning::

   When you retarget a task specified by an `lsst.pex.config.ConfigurableField` you lose all :ref:`configuration overrides <command-line-task-config-howto-obs>` for both the old and new task.
   This limitation is not shared by `lsst.pex.config.RegistryField`.

.. _command-line-task-retargeting-howto-registry-configfile:

How to retarget a subtask configured as a RegistryField with a configuration file
=================================================================================

To retarget a subtask specified as an `lsst.pex.config.RegistryField`, set the field's `~lsst.pex.config.RegistryField.name` attribute in a configuration file (using :option:`--config-file` or :option:`--configfile`).
Here is an example that assumes a task ``FooTask`` is defined in module :file:`.../foo.py` and registered using name ``foo``:

.. code-block:: python

   import .../foo.py
   config.registrySubtask.name = "foo"

Besides retargeting the registry subtask, there are two ways to configure parameters for tasks in a registry:

- :ref:`Set parameters for the active subtask <command-line-task-retargeting-howto-registry-active-config>` using the `~lsst.pex.config.RegistryField`\ ’s `~lsst.pex.config.RegistryField.active` attribute.
- :ref:`Set parameters for any registered task <command-line-task-retargeting-howto-registry-config>` using dictionary notation and the subtask's registered name.

These configuration methods are described next.

.. _command-line-task-retargeting-howto-registry-active-config:

Configure the active subtask configured as a RegistryField
----------------------------------------------------------

You may configure the retargeted subtask in a configuration file by setting the subtask configuration's `~lsst.pex.config.RegistryField.active` attribute.
For example:

.. code-block:: python

   config.registrySubtask.active.subtaskParam1 = newValue

These configurations can also be specified directly on the command line as a :option:`--config` argument.
For example:

.. code-block:: bash

   --config registrySubtask.active.subtaskParam1=newValue

.. _command-line-task-retargeting-howto-registry-config:

Configure any subtask in a registry
-----------------------------------

Alternatively, you can then configure parameters for any subtask in the registry using key-value access.
For example:

.. code-block:: python

   config.registrySubtask["foo"].subtaskParam1 = newValue
