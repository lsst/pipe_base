"""
Test2 Task
"""
from __future__ import absolute_import, division, print_function
from lsst.pipe.base.task import Task
from lsst.pipe.base.super_task import SuperTask
import lsst.pipe.base.super_task as stask
from lsst.pipe.base.struct import Struct
import lsst.pex.config as pexConfig


class Test2Config(pexConfig.Config):
    """
    Config
    """
    maxval = pexConfig.Field(
        dtype=int,
        doc="Max value",
        default=22,
    )

@stask.wrapclass(stask.wraprun)
class Test2Task(SuperTask):
    """
    Task
    """
    ConfigClass = Test2Config  # ConfigClass = pexConfig.Config
    _default_name = 'test2'

    def __init__(self, *args, **kwargs):
        super(Test2Task, self).__init__(*args, **kwargs)  # # P3 would be super().__init__()

    def execute(self, dataRef):
        return self.run()

    def pre_run(self):
        #check for inputs
        #missing = []
        #needs = ['val1']
        #try:
        #    input_keys = self.input.getDict().keys()
        #except:
        #    input_keys = []
        #for key in needs:
        #    if key not in input_keys:
        #        missing.append(key)
        #if len(missing) > 0 :
        #    print('Missing these: ',missing)
        #    raise RuntimeError("Missing inputs for %s" % self.name)
        #else:
        #    print('good to go')
        pass

    def run(self):
        """
        Run method
        :return:
        """
        print('I am running %s Using %s activator' % (self.name, self.activator))

        myvalue = 2.5

        self.output= Struct(
            val2=myvalue,
            str2='value 2')
        return self.output

    def __str__(self):
        return str(self.__class__.__name__)+' named : '+self.name

