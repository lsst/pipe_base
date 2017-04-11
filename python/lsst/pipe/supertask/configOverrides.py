"""Module which defines ConfigOverrides class and related methods.
"""
from builtins import object


class ConfigOverrides(object):
    """Defines a set of overrides to be applied to a task config.

    Overrides for task configuration need to be applied by activator when
    creating task instances. This class represents an ordered set of such
    overrides which activator receives from some source (e.g. command line
    or some other configuration).

    Methods
    ----------
    addFileOverride(filename)
        Add overrides from a specified file.
    addValueOverride(field, value)
        Add override for a specific field.
    applyTo(config)
        Apply all overrides to a `config` instance.

    Notes
    -----
    Serialization support for this class may be needed, will add later if
    necessary.
    """

    def __init__(self):
        self._overrides = []

    def addFileOverride(self, filename):
        """Add overrides from a specified file.

        Parameters
        ----------
        filename : str
            Path to the override file.
        """
        self._overrides += [('file', filename)]

    def addValueOverride(self, field, value):
        """Add override for a specific field.

        This method is not very type-safe as it is designed to support
        use cases where input is given as string, e.g. command line
        activators. If `value` has a string type and setting of the field
        fails with `TypeError` the we'll attempt `eval()` the value and
        set the field with that value instead.

        Parameters
        ----------
        field : str
            Fully-qualified field name.
        value :
            Value to be given to a filed.
        """
        self._overrides += [('value', (field, value))]

    def applyTo(self, config):
        """Apply all overrides to a task configuration object.

        Parameters
        ----------
        config : `pex.Config`

        Raises
        ------
        `Exception` is raised if operations on configuration object fail.
        """
        for otype, override in self._overrides:
            if otype == 'file':
                config.load(override)
            elif otype == 'value':
                field, value = override
                field = field.split('.')
                # find object with attribute to set, throws if we name is wrong
                obj = config
                for attr in field[:-1]:
                    obj = getattr(obj, attr)
                try:
                    setattr(obj, field[-1], value)
                except TypeError:
                    if not isinstance(value, str):
                        raise
                    # this can throw
                    value = eval(value, {})
                    setattr(obj, field[-1], value)
