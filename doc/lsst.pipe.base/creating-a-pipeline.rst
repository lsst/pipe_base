.. _pipe_base_creating_pipeline:

###################
Creating a Pipeline
###################

**Note**
This guide assumes some knowledge about
`PipelineTask`\ s, and so if you would like you can check out
:doc:`Creating a PipelineTask <creating-a-pipelinetask>` for info on what
a `PipelineTask` is and how to make one. Otherwise, this guide attempts to be
mostly stand alone, and should be readable with minimal references.

....

`PipelineTask`\ s are bits of algorithmic code that define what data they
need as input, what they will produce as an output, and a ``run`` method
which produces this output. `Pipeline`\ s are high level documents that
create a specification that is used to run one or more `PipelineTask`\ s.
This how-to guide guide will introduce you to the basic syntax of a
`Pipeline` document, and progressively take you through; configuring tasks,
verifying configuration, specifying subsets of tasks, creating `Pipeline`\ s
using composition, a basic introduction to options when running `Pipeline`\
s, and discussing common conventions when creating `Pipelines`.

.. _pipeline_creating_intro:

.. _pipeline_creating_format:

----------------
A Basic Pipeline
----------------

`Pipeline` documents are written using yaml syntax. If you are unfamiliar with
yaml, there are many guides across the internet, but the basic idea is that it
is a simple markup language to describe key, value mappings, and lists of
values (which may be further mappings).

`Pipeline`\ s have two required keys, ``description`` and ``tasks``. The value
associated with the ``description`` should provide a reader an understanding of
what the `Pipeline` is intended to do and is written as plain text.

The second required key is ``tasks``. Unlike ``description``, which has plain
text as a value, the 'value' associated with the ``tasks`` keys is another
key-value mapping. This section defines what work this pipeline will do. The
keys of the inner mapping are labels which will be used to refer to an
individual task. These labels can be any name you choose, the only
restriction is that they must be unique amongst all the tasks. The values in
this mapping can be a number of things, which you will see through the course
of this guide, but the most basic is a string that gives the fully qualified
`PipelineTask` that is to be run.

This is a lot of text to digest, so take a look at the following example as a
'picture' is worth a thousand words.

.. code-block:: yaml

    description: A demo pipeline in the how-to guide
    tasks:
      characterizeImage:  lsst.pipe.tasks.characterizeImage.CharacterizeImageTask

Writing this and saving it to a file with a .yaml extension is all it takes
to have a simple pipeline. The ``description`` reflects that this `Pipeline`
is intended just for this guide. The ``tasks`` section contains only one
entry. The label used for this entry, ``characterizeImage``, happens to match
the module of the task it points to. It could have been anything, but the
name was suitably descriptive, so it was a good choice.

If run, this `Pipeline` would execute `CharacterizeImageTask` processing the
datasets declared in that task, and write the declared outputs.

Having a pipeline to run a single `PipelineTask` does not seem very useful.
The examples below (and in subsequent sections) are a bit more realistic.

.. code-block:: yaml

    description: A demo pipeline in the how-to guide
    tasks:
      isr: lsst.ip.isr.IsrTask
      characterizeImage: lsst.pipe.tasks.characterizeImage.CharacterizeImageTask
      calibrate: lsst.pipe.tasks.calibrate.CalibrateTask

This `Pipeline` contains 3 tasks to run, all of which are steps in processing
a single frame.  The order that the tasks are executed is not determined by
the ordering of the tasks in the pipeline, but by the
definition/configuration of the `PipelineTask`\ s.  It is the job of the
execution system to work out this order, as such you may write the tasks in
any order in the `Pipeline`.  The following `Pipeline` is exactly the same
from an execution point of view. With that said, be kind to human readers and
if possible write the tasks in the order you expect the tasks to execute most
often so readers can gain some intuition.

.. code-block:: yaml

    description: A demo pipeline in the how-to guide
    tasks:
      characterizeImage: lsst.pipe.tasks.characterizeImage.CharacterizeImageTask
      calibrate: lsst.pipe.tasks.calibrate.CalibrateTask
      isr: lsst.ip.isr.IsrTask

Tasks define their inputs and outputs which are used to construct an
execution graph of the specified tasks. A consequence of this is if a
pipeline does not define all the tasks required to generate all needed inputs
and outputs it will get caught before any execution occurs.

.. _pipeline_creating_config:

-----------------
Configuring Tasks
-----------------

`PipelineTask`\ s (and their subtasks) contain a multitude of
configuration options that alter the way the task executes. Because
`Pipeline`\ s are designed to do a specific type of processing (per the
description field) some tasks may need specific configurations set to
enable/disable behavior in the context of the specific `Pipeline`.

To configure a task associated with a particular label, the value associated
with the label must be changed from the qualified task name to a new
sub-mapping. This new sub mapping should have two keys, ``class`` and
``config``.

The ``class`` key should point to the same qualified task name as before. The
value associated with the ``config`` keyword is itself a mapping where
configuration overrides are declared. The example below shows this behavior
in action.

.. code-block:: yaml

  description: A demo pipeline in the how-to guide
  tasks:
    isr:
      class: lsst.ip.isr.IsrTask
      config:
        doVignette: true
        vignetteValue: 0.0
    characterizeImage: lsst.pipe.tasks.characterizeImage.CharacterizeImageTask
    calibrate:
      class: lsst.pipe.tasks.calibrate.CalibrateTask
      config:
        astrometry.matcher.maxOffsetPix: 300

This example shows the `Pipeline` from the previous section with
configuration overrides applied to two of the tasks. The label ``isr`` is now
associated with the keys ``class`` and ``config``. The class location is
associated with ``class`` keyword instead of the label directly. The
``config`` keyword is associated with various `~lsst.pex.config.Field`\ s and
the configuration appropriate for this `Pipeline` specified as an additional
yaml mapping.

The complete complexity of `lsst.pex.config` can't be represented with simple
yaml mapping syntax. To account for this, ``config`` blocks in `Pipeline`\ s
support two special fields: ``file`` and ``python``.

The ``file`` key may be associated with either a single value pointing to a
filesystem path where a `lsst.pex.config` file can be found, or a yaml list
of such paths. The file paths can contain environment variables that will be
expanded prior to loading the file(s). These files will then be applied to
the task during configuration time to override any default values.

Sometimes configuration is too complex to express with yaml syntax, yet it is
simple enough that it does not warrant its own config file. The ``python``
key is designed to support this use case. The value associated with the key
is a (possibly multi-line) string with valid python syntax. This string is
evaluated and applied during task configuration exactly as if it had been
written in a file or typed out in an interpreter. The following example expands
the previous one to use the ``python`` key.

.. code-block:: yaml

  description: A demo pipeline in the how-to guide
  tasks:
    isr:
      class: lsst.ip.isr.IsrTask
      config:
        doVignette: true
        vignetteValue: 0.0
    characterizeImage: lsst.pipe.tasks.characterizeImage.CharacterizeImageTask
    calibrate:
      class: lsst.pipe.tasks.calibrate.CalibrateTask
      config:
        astrometry.matcher.maxOffsetPix: 300
        python: |
          flags = ['base_PixelFlags_flag_edge', 'base_PixelFlags_flag_saturated', 'base_PsfFlux_flags']
          config.calibrate.astrometry.sourceSelector['references'].flags.bad = flags


.. _pipeline_creating_parameters:

----------
Parameters
----------

As you saw in the pervious section, each task defined in a `Pipeline` may
have its own configuration. However, it is sometimes useful for configuration
fields in multiple tasks to share the same value. `Pipeline`\ s support this
with a concept called ``parameters``. This is a top level section in the
`Pipeline` document specified with a key named ``parameters``.

The ``parameters`` section is a mapping of key-value pairs. The keys can then
be used throughout the document in the key-value section of config blocks
instead of using of the concrete parameter value.

To make this a bit clearer take a look at the following example, making note
that only config fields relevant for this example are shown.

.. code-block:: yaml

  parameters:
    calibratedSingleFrame: calexp
  tasks:
    calibrate:
      class: lsst.pipe.tasks.calibrate.CalibrateTask
      config:
        connections.outputExposure = parameters.calibratedSingleFrame
    makeWarp:
      class: lsst.pipe.tasks.makeCoaddTempExp.MakeWarpTask
      config:
        connections.calExpList = parameters.calibratedSingleFrame
    forcedPhotCcd:
      class: lsst.meas.base.forcedPhotCcd.ForcedPhotCcdTask
      config:
        connections.exposure = parameters.calibratedSingleFrame

The above example used ``parameters`` to link the dataset type names for
multiple tasks, but ``parameters`` can be used anywhere that more than one
config field use the same value, it is not restricted to dataset types.

:ref:`pipeline-running-intro` introduces how to run `Pipeline`\ s and will
talk about how to dynamically set a ``parameters`` value at `Pipeline`
invocation time.

.. _pipeline_creating_contracts:

----------------------------------
Verifying Configuration: Contracts
----------------------------------

The `~lsst.pipe.base.config.Config` classes associated with
`~lsst.pipe.base.task.Task`\ s provide a method named ``verify`` which can be
used to verify that all supplied configuration is valid. These verify methods
however, are shared by every instance of the config class. This means they
can not be specialized for the context in which the task is being used.

When writing `Pipelines` it is sometimes important to verify that
configuration values are either set in such a way to ensure expected
behavior, and/or consistently set between one or more tasks.  `Pipelines`
support this sort of verification with a concept called ``contracts``.  These
``contracts`` are useful for ensuring two separate config fields are set to
the same value, or ensuring a config parameter is set to a required value in
the context of this pipeline.  Because configuration values can be set
anywhere from the `Pipeline` definition to the command-line invocation of the
pipeline, these ``contracts`` ensure that required configuration is
appropriate prior to execution.

``contracts`` are expressions written with Python syntax that should evaluate
to a boolean value. If any ``contract`` evaluates to false, the `Pipeline`
configuration is deemed to be inconsistent, an error is raised, and
execution of the `Pipeline` is halted.

Defining contracts involves adding a new top level key to your document named
``contracts``. The value associated with this key is a yaml list of
individual contracts. Each list member may either be the ``contract``
expression or a mapping that contains the expression and a message to include
with an exception if the contract is violated. If the contract is defined as
a mapping, the expression is associated with a key named ``contract`` and the
message is a simple string associated with a key named ``msg``.

The expressions in the ``contracts`` section reference configuration
parameters for one or more tasks identified by the assigned label in the
``task`` section.  The syntax is similar to that of a task config override
file where the ``config`` variable is replaced with the task label associated
with the task to configure. An example contract to go along with our above
pipeline would be as follows:

.. code-block:: yaml

    contracts:
      - characterizeImage.applyApCorr == calibrate.applyApCorr"

This same contract can be defined in a mapping with an associated message as
below:

.. code-block:: yaml

    contracts:
      - contract: "characterizeImage.applyApCorr ==\
                   calibrate.applyApCorr"
        msg: "The aperture correction sub tasks are not consistent"

It is important to note how ``contracts`` relate to ``parameters``. While a
``parameter`` can be used to set two configuration variables to the same
value at the time `Pipeline` definition is read, it does not offer any
validation. It is possible for someone to change the configuration of one of
the fields before a `Pipeline` is run. Because of this, ``contracts`` should
always be written without regards to how ``parameters`` are used.

.. _pipeline_creating_subsets:

-------
Subsets
-------

`Pipelines` are the definition of a processing workflow from some input data
products to some output data products. Frequently, however, there are sub
units within a `Pipeline` that define a useful unit of the `Pipeline` to run
on their own. This may be something like processing single frames only.

You, as the author of the `Pipeline`, can define one or more of the
processing units by creating a section in your `Pipeline` named ``subsets``.
The value associated with the ``subsets`` key is a new mapping. The keys of
this mapping will be the labels used to refer to an individual ``subset``.
The values of this mapping can either be a yaml list of the tasks labels to
be associated with this subset, or another yaml mapping. If it is the latter,
the keys must be ``subset``, which is associated with the yaml list of task
labels, and ``description``, which is associated with a descriptive message
of what the subset is meant to do. Take a look at the following two examples
which show the same ``subset`` defined in both styles.

.. code-block:: yaml

  subsets:
    processCcd:
      - isr
      - characterizeImage
      - calibrate

.. code-block:: yaml

  subsets:
    processCcd:
      subset:
        - isr
        - characterizeImage
        - calibrate
      description: A set of tasks to run when doing single frame processing

Once a ``subset`` is created the label associated with it can be used in
any context where task labels are accepted.  Examples of this will be shown
in :ref:`pipeline-running-intro`.

.. _pipeline_creating_imports:

-----------
Importing
-----------

Similar to ``subsets``, which allow defining useful units within a
`Pipeline`, it's sometimes useful to construct a `Pipeline` out of other
`Pipelines`. This is known as importing a `Pipeline`.

Importing other pipelines begins with a top level key named ``imports``.
The value associated with this key is a yaml list. The values of this list
may be strings corresponding to a filesystem path of the `Pipeline` to
import. These paths may contain environment variables to help in writing
paths in a platform agnostic way.

Alternatively, the elements of the imports list may be yaml mapping. This
mapping begins with a key named ``location`` who's value is the same as the
path described above. The mapping can optionally contain the keys
``include``, ``exclude``, and ``importContracts``. The keys ``include`` and
``exclude`` can be used to specify which labels (or labeled subsets) to
include or exclude, respectively, when inheriting a ``Pipeline``. The values
associated with these keys are specified as a yaml list, and these two keys
are mutually exclusive, only one can be specified at at time. The
``importContracts`` key is optional and is associated with a boolean value
that controls wether ``contracts`` from the imported pipeline should be
included when importing, with a default value of true.

A few further notes about including and excluding. When specifying labels
with ``include`` or ``exclude``, it is possible to use a labeled subset in
place of a label. This will have the same effect as typing out all of the
labels listed in the subset. Another important point is the behavior of
labels that are not imported (either because they are excluded, or they are
not part of the include list). If any omitted label appears as part of a
subset, then the subset definition is not imported.

The order that `Pipelines` are listed in the ``imports`` section is not
important. Another thing to note is that declared labels must be unique
amongst all inherited `Pipelines`.

Once one or more pipelines is imported, the ``tasks`` section is processed.
If any new ``labels`` (and thus `PipelineTask`\ s) are declared they simply
extend the total `Pipeline`.

If a ``label`` declared in the the ``tasks`` section was declared in one of
the imported ``Pipelines``, one of two things happen. If the label is
associated with the same `PipelineTask` that was declared in the imported
pipeline, this definition will be extended. This means that any configs
declared in the imported `Pipeline` will be merged with configs declared in
the current `Pipeline` with the current declaration taking config precedence.
This behavior allows tasks to be extended in the current `Pipeline`.

If the ``label`` declared in the current `Pipeline` is associated with a
different `PipelineTask` than the ``label`` in the imported declaration, then
the label with be considered re-declared and the declaration in the current
`Pipeline` will be used.  The declaration defined in the imported `Pipeline`
is dropped.


.. _pipeline_creating_obs_package:

--------------------------------------
obs\_* package overrides for Pipelines
--------------------------------------

`Pipeline`\ s support automatically loading `~lsst.pipe.base.Task`
configuration files defined in obs packages.  A top level key named
`instrument` is associated with a string representing the fully qualified
class name of the python camera object.  For instance, for an ``obs_subaru``
`Pipeline` this would look like:

.. code-block:: yaml

  instrument: lsst.obs.subaru.HyperSuprimeCam

The ``instrument`` key is available to all `Pipelines`, but by convention
obs\_* packages typically will contain `Pipelines` that are customized for
the instrument they represent, inside a directory named ''pipelines''.  This
includes relevant configs, `PipelineTask` (re)declarations, instrument label,
etc.  These pipelines can be found inside a directory named `pipelines` that
lives at the root of each obs\_ package.

These `Pipeline`\ s enable you to run a `Pipeline` that is configured for the
desired camera, or can serve as a base for further `Pipeline`\ s to import.

.. _pipeline-running-intro:

------------------------------------------
Command line options for running Pipelines
------------------------------------------
This section is not intended to serve as a tutorial for processing data from
the command line, for that refer to `lsst.ctrl.mpexec` or `lsst.ctrl.bps`.
However, both of these tools accept URI pointers to a `Pipeline`.  These URIs
can be altered with a specific syntax which will control how the `Pipeline`
is loaded.

The simplest form of a `Pipeline` specification is the URI at which the
`Pipeline` can be found. This URI may be any supported by
`lsst.resources.ResourcePath`. In the case that the pipeline resides in a file
located on a filesystem accessible by the machine that will be processing the
`Pipeline` (i.e. a file URI), there is no need to preface the URI with
``file://``, a bare file path is assumed to be a file based URI.

File based URIs also support shell variable expansion. If, for instance, the
URI contains ``$ENV_VAR``, the variable will be expanded prior to evaluating
the path. A file based URI to a pipeline in an lsst package directory would
look something like:

``$PIPE_TASKS_DIR/pipelines/DRP.yaml``

As an example of an alternative URI, here is one based on s3 storage:

``s3://some_bucket/pipelines/DRP.yaml``

For any type of URI, `Pipelines` may be specified with additional parameters
specified after a # symbol. The most basic parameter is simply a label.
Loading a `Pipeline` with this label specified will cause only this label to
be loaded. It will be as if the `Pipeline` only contained that label. This is
useful when you want to run only one `PipelineTask` out of an existing
pipeline. Other fields such as contracts (that only contain a reference to
the supplied label) and instrument will also be loaded. Using the example
above, a URI of this form would look something like:

``$PIPE_TASKS_DIR/pipelines/DRP.yaml#characterizeImage``

Akin to loading a single label, multiple labels may be specified by
separating each one with a comma like in the following example.

``$PIPE_TASKS_DIR/pipelines/DRP.yaml#isr,characterizeImage,calibrate``

Make note, when supplying labels in this way, it is possible to supply a list
of labels who's task do not define a complete processing flow. While there is
nothing wrong with the `Pipeline` itself, there may be missing data products
due to the task that produces it not being in the list of labels to run. When
this happens no processing will be able to be done, and you should look at the
input and output dataset types for each of tasks corresponding to the labels
specified.

As mentioned above, subsets are useful for specifying multiple labels at one
time.  As such labeled subsets can be used in the parameter list to indicate
all labels associated with the subset should be run.  Also like labels,
multiple labeled subsets can be used by separating each with a comma.  As you
can see labeled subsets are essentially synonymous with labels and can be
used interchangeably.  As such, it is also possible to mix task labels and
labeled subsets when specifying a parameter list. A `Pipeline` URI that
specifies a subset based on our previous example would look like the following:

``$PIPE_TASKS_DIR/pipelines/DRP.yaml#processCcd``

.. _pipeline_conventions:

--------------------
Pipeline conventions
--------------------

Below is a list of conventions that are commonly used when writing
`Pipelines`\ s. These are not hard requirements, but their use helps maintain
consistency throughout the software stack.

* The name of a Pipeline file should follow class naming conventions (camel
  case with first letter capital).
* Preface a Pipeline name with an underscore if it is not intended to be
  inherited and or run directly, which is referred to as a private pipeline
  (it is part of a larger pipeline).
* Use inheritance to avoid really long documents, using 'private' `Pipeline`\ s
  named as above.
* `Pipeline`\ s should contain a useful description of what the `Pipeline` is
  intended to do.
* `Pipeline`\ s should be placed in a directory called ``pipelines`` at the top
  level of a package.
* Instrument packages should provide `Pipeline`\ s that override standard
  `Pipeline`\ s and are specifically configured for that instrument (if
  applicable).
