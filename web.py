'''
Created on 2015-4-9

@author: SmokeAngle0421
'''
from twisted.web import resource
from scrapy.utils.jsonrpc import json
from datetime import datetime

class webRoot(resource.Resource) :
    """"""
    #----------------------------------------------------------------------
    def __init__(self, app, taskService):
        """Constructor"""
        resource.Resource.__init__(self)
        self.putChild('TaskManage', TaskManage(self))
        self.app = app
        self.taskService = taskService


class TaskManage(resource.Resource):
    #----------------------------------------------------------------------
    def __init__(self, root):
        """"""
        resource.Resource.__init__(self)
        self.root = root

    def render_GET(self, request):
        action = request.args['action'][0]
        LiPid = request.args['iPid']
        result = {}
        if action == 'stopTask':
            result['action'] = 'stopTask'
            result['data'] = {}
            for iPid in LiPid:
                ret = self.root.taskService.stopTask(iPid)
                result['data']['iPid_%s' % iPid] = ret
        elif action == 'stateTask':
            result['action'] = 'stateTask'
            result['data'] = {}
            for iPid in LiPid:
                ret = self.root.taskService.stateTask(iPid)
                result['data']['iPid_%s' % iPid] = ret
        return json.dumps(result,cls=DateEncoder)
    #JSONEncoder().encode({"foo": ["bar", "baz"]})

    def render_POST(self, request):
        return 'render_POST'

    def render_DELETE(self, request):
        return 'render_DELETE'

    def render_PUT(self, request):
        LiPid = request.args['iPid']
        for iPid in LiPid:
            print iPid
            print self.root.taskService.startTask(iPid)
        return 'render_PUT'

class DateEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.__str__()
        return json.JSONEncoder.default(self, o)