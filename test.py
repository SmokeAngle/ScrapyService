'''
Created on 2015-4-11

@author: SmokeAngle0421
'''


def my_generator():
    print 'starting up'
    yield 1
    print "workin'"
    yield 2
    print "still workin'"
    yield 311
    print 'done'

for n in my_generator():
    print n