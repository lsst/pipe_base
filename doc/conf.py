from documenteer.conf.pipelinespkg import *  # noqa: F403, import *

project = "pipe_base"
html_theme_options["logotext"] = project  # noqa: F405, unknown name
html_title = project
html_short_title = project
doxylink = {}
exclude_patterns = ["changes/*"]

intersphinx_mapping["networkx"] = ("https://networkx.org/documentation/stable/", None)  # noqa: F405
intersphinx_mapping["lsst"] = ("https://pipelines.lsst.io/v/weekly/", None)  # noqa: F405
