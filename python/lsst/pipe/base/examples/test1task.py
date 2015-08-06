"""
Test1 Task
"""
from __future__ import absolute_import, division, print_function
from lsst.pipe.base.super_task import SuperTask
from lsst.pipe.base.struct import Struct
import lsst.pipe.base.super_task as stask
import lsst.pex.config as pexConfig


class Test1Config(pexConfig.Config):
    """
    Config
    """
    do_print = pexConfig.Field(
        dtype=bool,
        doc="Display info",
        default=False,
    )

@stask.wrapclass(stask.wraprun)
class Test1Task(SuperTask):
    """
    Task
    """
    ConfigClass = Test1Config  # ConfigClass = pexConfig.Config
    _default_name = 'Test1'

    def __init__(self, *args, **kwargs):
        super(Test1Task, self).__init__(*args, **kwargs)  # # P3 would be super().__init__()


    def execute(self, dataRef):
        return self.run()


    def pre_run(self):
        print("Custom pre run commands at %s" % self.name)

    def run(self):
        """
        Run method
        :return:
        """
        print('I am running %s Using %s activator' % (self.name, self.activator))
        if self.config.do_print:
            print("Displaying Info...")

        self.output = Struct(
            val1=10.,
            str1='test')

        return self.output

    def __str__(self):
        return str(self.__class__.__name__)+' named : '+self.name

