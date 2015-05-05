'''
Created on 2015-4-9

@author: SmokeAngle0421
'''
from twisted.python.log import theLogPublisher


class log():
    @staticmethod
    def msg(*message, **kw):
        theLogPublisher.msg(system='Application',*message, **kw)