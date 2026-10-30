"""Allow running the resource usage analyzer as a module.

Use ``python -m lsst.pipe.base._resource_usage_analyzer``.
"""

from __future__ import annotations

from lsst.pipe.base._resource_usage_analyzer.cli import main

main()
