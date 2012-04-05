#!/usr/bin/python

import beanstalkc
import memcache
import time
import operator

QUEUE_TO_WATCH = "msg_for_worker"
QUEUE_TO_USE = "msg_for_client"
QUEUE_TO_IGNORE = "default"

while True:                             # Indefinitely wait for jobs to do

    print "Connecting to job server..."
    beanstalk = beanstalkc.Connection(host='10.4.12.63', port=11300)

    beanstalk.watch(QUEUE_TO_WATCH)     # Reserve jobs from this queue
    beanstalk.ignore(QUEUE_TO_IGNORE)   # Ignore this queue for reservation

    print "Waiting for jobs..."

    job = beanstalk.reserve()           # Blocks until a job is available

    print "Reserved a job.."

    [ strI, strJ, stringRow, stringColumn ] = job.body.split( '\n' )
    I = int( strI )
    J = int( strJ )
    row = map( int, stringRow.split() )
    column = map( int, stringColumn.split() )

    n = len( row )

    start_time = time.time()

    print 'Computing Cij..'
    try:
        result = reduce( operator.add, map( operator.mul, row, column ) )
    except Exception, e:
        print e
        print "Job failed.", n
        job.release()

    mc = memcache.Client(['10.4.12.63:11211'], debug=0)
    print 'Wrote result to',  str(I+J)
    mc.set( str( I + J ), result )
    beanstalk.use(QUEUE_TO_USE)
    beanstalk.put( strI + strJ )

    job.delete()                        # Tell the job server you're done
    beanstalk.close()

    print "Finished job in", str(time.time() - start_time), "seconds"
    print
