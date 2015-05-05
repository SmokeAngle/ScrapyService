from twisted.application import internet,service
from twisted.web import server
from web import  webRoot
from services import  TaskService

PORT=8090
application = service.Application('testApp')


_taskService = TaskService()
_taskService.setServiceParent(application)

_webService = internet.TCPServer(PORT, server.Site(webRoot(app=application, taskService=_taskService)))
_webService.setServiceParent(application)