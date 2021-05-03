__all__ = ["getTaskDict", "showTaskHierarchy"]

import lsst.pex.config as pexConfig


def getTaskDict(config, taskDict=None, baseName=""):
    """Get a dictionary of task info for all subtasks in a config

    Parameters
    ----------
    config : `lsst.pex.config.Config`
        Configuration to process.
    taskDict : `dict`, optional
        Users should not specify this argument. Supports recursion.
        If provided, taskDict is updated in place, else a new `dict`
        is started.
    baseName : `str`, optional
        Users should not specify this argument. It is only used for
        recursion: if a non-empty string then a period is appended
        and the result is used as a prefix for additional entries
        in taskDict; otherwise no prefix is used.

    Returns
    -------
    taskDict : `dict`
        Keys are config field names, values are task names.

    Notes
    -----
    This function is designed to be called recursively.
    The user should call with only a config (leaving taskDict and baseName
    at their default values).
    """
    if taskDict is None:
        taskDict = dict()
    for fieldName, field in config.items():
        if hasattr(field, "value") and hasattr(field, "target"):
            subConfig = field.value
            if isinstance(subConfig, pexConfig.Config):
                subBaseName = f"{baseName}.{fieldName}" if baseName else fieldName
                try:
                    taskName = f"{field.target.__module__}.{field.target.__name__}"
                except Exception:
                    taskName = repr(field.target)
                taskDict[subBaseName] = taskName
                getTaskDict(config=subConfig, taskDict=taskDict, baseName=subBaseName)
    return taskDict


def showTaskHierarchy(config):
    """Print task hierarchy to stdout.

    Parameters
    ----------
    config : `lsst.pex.config.Config`
        Configuration to process.
    """
    print("Subtasks:")
    taskDict = getTaskDict(config=config)

    fieldNameList = sorted(taskDict.keys())
    for fieldName in fieldNameList:
        taskName = taskDict[fieldName]
        print(f"{fieldName}: {taskName}")
