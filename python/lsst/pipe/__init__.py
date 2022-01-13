import pkgutil

try:
    import lsstimport
except ImportError:
    # Not generally needed.
    pass

__path__ = pkgutil.extend_path(__path__, __name__)
