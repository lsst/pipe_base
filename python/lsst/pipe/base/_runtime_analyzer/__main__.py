"""Allow running the runtime analyzer as a module.

Use ``python -m lsst.pipe.base._runtime_analyzer``.
"""

from __future__ import annotations

from lsst.pipe.base._runtime_analyzer.cli import main

main()
