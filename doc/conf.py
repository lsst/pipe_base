from documenteer.conf.pipelinespkg import *  # noqa: F403, import *

project = "pipe_base"
html_theme_options["logotext"] = project  # noqa: F405, unknown name
html_title = project
html_short_title = project
doxylink = {}
exclude_patterns = ["changes/*"]

intersphinx_mapping["networkx"] = ("https://networkx.org/documentation/stable/", None)  # noqa: F405
intersphinx_mapping["lsst"] = ("https://pipelines.lsst.io/v/weekly/", None)  # noqa: F405
intersphinx_mapping["pydantic"] = ("https://docs.pydantic.dev/latest/", None)  # noqa: F405

# As a temporary hack until we move to documenteer 2 delete scipy
# (since it no longer works)
try:
    del intersphinx_mapping["scipy"]  # noqa: F405
except KeyError:
    pass

nitpick_ignore = [
    ("py:mod", "lsst.meas.algorithms.starSelectorRegistry"),
    ("py:mod", "lsst.meas.algorithms.psfDeterminerRegistry"),
]
