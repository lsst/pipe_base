.. _pipe_base_pipeline_graphs:

.. py:currentmodule:: lsst.pipe.base.pipeline_graph

############################
Working with Pipeline Graphs
############################

Pipeline objects are written as YAML documents, but once they are fully configured, they are conceptually directed acyclic graphs (DAGs).
In code, this graph version of a pipeline is represented by the `PipelineGraph` class.
`PipelineGraph` objects are usually constructed by calling the `.Pipeline.to_graph` method::

  from lsst.daf.butler import Butler
  from lsst.pipe.base import Pipeline

  butler = Butler("/some/repo")
  pipeline = Pipeline.from_uri("my_pipeline.yaml")
  graph = pipeline.to_graph(registry=butler.registry)

The ``registry`` argument here is optional, but without it the graph will be incomplete ("unresolved") and the pipeline will not be checked for correctness until the `~PipelineGraph.resolve` method is called.
Resolving a graph compares all of the task connections (which are edges in the graph) that reference each dataset type to each other and to the registry's definition of that dataset to determine a common graph-wide definition.
A definition in the registry always takes precedence, followed by the output connection that produces the dataset type (if there is one).
When a pipeline graph is used to register dataset types in a data repository, it is this common definition in the dataset type node that is registered.
Edge dataset type descriptions represent storage class overrides for a task, or specify that the task only wants a component.

Simple Graph Inspection
-----------------------

The basic structure of the graph can be explored via the `~PipelineGraph.tasks` and `~PipelineGraph.dataset_types` mapping attributes.
These are keyed by task label and *parent* (never component) dataset type name, and they have `TaskNode` and `DatasetTypeNode` objects as values, respectively.
A resolved pipeline graph is always sorted, which means iterations over these mappings will be in topological order.
`TaskNode` objects have an `~TaskNode.init` attribute that holds a `TaskInitNode` instance - these resemble `TaskNode` objects and have edges to dataset types as well, but these edges represent the "init input" and "init output" connections of those tasks.

Task and dataset type node objects have attributes holding all of their edges, but to get neighboring nodes, you have to go back to the graph object::

  task_node = graph.tasks["task_a"]
  for edge in task.inputs.values():
      dataset_type_node = graph.dataset_types[edge.parent_dataset_type_name]
      print(f"{task_node.label} takes {dataset_type_node.name} as an input.")

There are also convenience methods on `PipelineGraph` to get the edges or neighbors of a node:

  - `~PipelineGraph.producing_edge_of`: an alternative to `DatasetTypeNode.producing_edge`
  - `~PipelineGraph.consuming_edges_of`: an alternative to `DatasetTypeNode.consuming_edges`
  - `~PipelineGraph.producer_of`: a shortcut for getting the task that write a dataset type
  - `~PipelineGraph.consumers_of`: a shortcut for getting the tasks that read a dataset type
  - `~PipelineGraph.inputs_of`: a shortcut for getting the dataset types that a task reads
  - `~PipelineGraph.outputs_of`: a shortcut for getting the dataset types that a task writes

Pipeline graphs also hold the `~PipelineGraph.description` and `~PipelineGraph.data_id` (usually just an instrument value) of the pipeline used to construct them, as well as the same mapping of labeled task subsets (`~PipelineGraph.task_subsets`).

Modifying PipelineGraphs
------------------------

Usually the tasks in a pipeline are set before a `PipelineGraph` is ever constructed.
In some cases it may be more convenient to add tasks to an existing `PipelineGraph`, either because a related graph is being created from an existing one, or because a (rare) task needs to be configured in a way that depends on the content or structure of the rest of the graph.
`PipelineGraph` provides a number of mutation methods:

- `~PipelineGraph.add_task` adds a brand new task from a `.PipelineTask` type object and its configuration;
- `~PipelineGraph.add_task_nodes` adds one or more tasks from a different `PipelineGraph` instance;
- `~PipelineGraph.reconfigure_tasks` replaces the configuration of an existing task with new configuration (note that this is typically less convenient than adding config *overrides* to a `Pipeline` object, because all configuration in a `PipelineGraph` must be validated and frozen);
- `~PipelineGraph.remove_task_nodes` removes existing tasks;
- `~PipelineGraph.add_task_subset` and `~PipelineGraph.remove_task_subset` modify the mapping of labeled task subsets (which can also be modified in-place).

**The most important thing to remember when modifying `PipelineGraph` objects is that modifications typically reset some or all of the graph to an unresolved state.**

The reference documentation for these methods describes exactly what guarantees they make about existing resolutions in detail, and what operations are still supported on unresolved or partially-resolved graphs, but it is easiest to just ensure `resolve` is called after any modifications are complete.

`PipelineGraph` mutator methods provide strong exception safety (the graph is left unchanged when an exception is raised and caught by calling code) unless the exception type raised is `PipelineGraphExceptionSafetyError`.

Exporting to NetworkX
---------------------

NetworkX is a powerful Python library for graph manipulation, and in addition to being used in the implementation, `PipelineGraph` provides methods to create various native NetworkX graph objects.
The node attributes of these graphs provide much of the same information as the `TaskNode` and `DatasetTypeNode` objects (see the documentation for those objects for details).

The export methods include:

 - `~PipelineGraph.make_xgraph` exports all nodes, including task nodes, dataset type nodes, and task init nodes, and the edges between them.
   This is a `networkx.MultiDiGraph` because there can be (albeit) rarely multiple edges (representing different connections) between a dataset type and a task.
   The edges of this graph have attributes as well as the nodes.
 - `~PipelineGraph.make_bipartite_graph` exports just task nodes and dataset type nodes and the edges between them (or, if ``init=True``, just task init nodes and the dataset type nodes and edges between them).
   A "bipartite" graph is one in which there are two kinds of nodes and edges only connect one type to the other.
   This is also a `networkx.MultiDiGraph`, and its edges also have attributes.
 - `~PipelineGraph.make_task_graph` exports just task (or task init) nodes; it is one "bipartite projection" of the full graph.
   This is a `networkx.DiGraph`, because all dataset types that connect a pair of tasks are rolled into one edge, and edges have no state.
 - `~PipelineGraph.make_dataset_type_graph` exports just dataset type nodes; it is one "bipartite projection" of the full graph.
   This is a `networkx.DiGraph`, because all tasks that connect a pair of dataset types are rolled into one edge, and edges have no state.

.. _pipeline-graph-subset-expressions:

Pipeline graph subset expressions
---------------------------------

The `PipelineGraph.select` and `PipelineGraph.select_tasks` methods utilize a boolean expression language to select a subset of the tasks in a `PipelineGraph`.
The language uses familiar set operators for union (``|``), intersection (``&``), and set-inversion (``~``), with the operands any of the following:

- a task label
- a task subset label
- a dataset type name (resolves to the label of the producing task, or an empty set for overall inputs; may not be an init-output)
- an ancestor or descendant search, starting from a task label or dataset type name (see below)
- a nested expression.

Parentheses may be used for grouping.

Task labels, task subset labels, and dataset type names all appear as regular unquoted strings.
In cases where dataset type name is the same as a task or task subset label, a prefix can be added to disambiguate: ``T:`` for task, ``D:`` for dataset type, and ``S:`` for task subset.

An ancestor or descendant search uses ``<``, ``<=``, ``>``, and ``>=`` as *unary* operators, with the operands being task labels or dataset type names (which may be qualified with ``T:`` or ``D:``, respectively, as described above).
For tasks these searches are straightforward:

- ``<`` and ``<=`` select all tasks whose outputs are consumed by the operand task, recursively, with the operand task itself included only for ``<=``.

- ``>`` and ``>=`` select all tasks that consume the outputs of the operand task, rescursively, with the operand task itself included only for ``>=``.

Because the expressions are logically set operations on tasks, ancestor and descendant searches on dataset types work differently and are not quite symmetric:

- ``<`` and ``<=`` act like an ancestor search on the task that produces the operand dataset type.
  For overall inputs they yield empty sets.
  Init-outputs are not permitted.

- ``>`` and ``>=`` act like a union of descendant searches on all tasks that consume the operand dataset type.
  This includes tasks that consume the operand dataset type as an init-input (this is the only context in which init-output dataset types can appear in expressions).
  For ``>=`` only, the task that produces the operand dataset type is also included, but in this case it is an error for the operand to be an init-output.

Note that these ancestor and descendant searches are not the only useful way to define the subset of a pipeline that is "before" or "after" a task; the ancestors ``<a`` of a task ``a`` are those that *must* be run before ``a``, while the inverse of the descendants ``~>=a`` are the tasks that *can* be run before ``a``.
Similarly, the descendants ``>a`` of ``a`` are the tasks that can only be run after ``a``, while the inverse of the ancestors ``~<=a`` are all tasks that can be run after ``a``.

Examples
^^^^^^^^

All tasks in subset ``s`` except task ``b``:

.. code-block:: text

  s & ~b

All tasks in either subset ``r`` or subset ``s`` that would need to be re-run to pick up a change in the behavior of task ``a``:

.. code-block:: text

  (r | s) & >=a

All tasks in subset ``s`` that need to be run to accept failures in task ``c`` as unrecoverable, after a previous run left some quanta of those tasks blocked:

.. code-block:: text

  s & >a

All tasks needed to produce dataset type ``d`` or dataset type ``e``:

.. code-block:: text

  <d | <e

All tasks except task ``a`` that can be run without producing dataset type ``f``:

.. code-block:: text

  ~a & ~>=f


Formal grammar
^^^^^^^^^^^^^^

.. code-block:: bnf

  <expression> ::= ~ <expression>
                 | <expression> | <expression>
                 | <expression> & <expression>
                 | (<expression>)
                 | S:<subset-label>
                 | <subset-label>
                 | < <node>
                 | <= <node>
                 | > <node>
                 | <= <node>
                 | <node>

  <node> ::= T:<task-label>
           | <task-label>
           | D:<dataset-type-name>
           | <dataset-type-name>

Whitespace is ignored, but is not permitted before or after the ``:`` in qualified identifiers.

The operator precedence in the absence of parenthesis is ``~``, ``&``, ``|`` ( highest to lowest).
