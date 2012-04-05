#!/usr/bin/python

import beanstalkc
import memcache

QUEUE_TO_WATCH = "msg_for_client"
QUEUE_TO_USE = "msg_for_worker"
QUEUE_TO_IGNORE = "default"

# We want to multiply matrix 1 and matrix2
matrix1 = [ [1, 2, 3], [4, 5, 6], [7, 8, 9] ]
matrix2 = [ [1, 0, 0], [0, 1, 0], [0, 0, 1] ]

print "Connecting to beanstalkd server.."
beanstalk = beanstalkc.Connection( '10.4.12.63', 11300 )

beanstalk.use( QUEUE_TO_USE )
beanstalk.ignore( QUEUE_TO_IGNORE )
beanstalk.watch( QUEUE_TO_WATCH )

n = len( matrix1 )

mc = memcache.Client(['10.4.12.63:11211'], debug=0)
matrix3 = [ [] * n ]

print 'Sending jobs to jobs queue..'
for i in range(n):
    for j in range(n):
        print 'Sending a row and column to job queue'
        stringRow = ' '.join( map( str, matrix1[ i ] ) )
        stringColumn = ' '.join( map( str, [ row[j] for row in matrix2 ] ) )
        # Separating the row and column with a new line
        beanstalk.put( str(n*i) + '\n' + str(j) + '\n' + stringRow + '\n' + stringColumn )

print 'Sent all jobs to queue.'

# Each of the jobs returned would contain confirmation if
# Cij was completed
for i in range( n**2 ):
    job = beanstalk.reserve()
    print 'Confirmation: Worker finished computing C' + job.body

print 'All workers finished.'
print 'Extracting result from memory..'

print 'Got the result, Here it is: '
for i in range(n):
    for j in range(n):
        print ( mc.get( str( i*n + j ) ) ),
    print

