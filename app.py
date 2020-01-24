# -*- coding: utf-8 -*-
#Temi Owoaje
#EQ Works , rate-limiter solution

from flask import Flask, jsonify, request
from functools import wraps     
from redis import Redis     
import sqlalchemy
import time

#Objective
"""
implement a rate limiter on all api endpoints
-  track access using ip adresses, and route (so i can track all users, not just authenticated users)
-  for now store this information in dictionary.
    -could not get dictionary to work, using redis for easier storage.
- if too many request,return '429 error', 429
"""

# web app
app = Flask(__name__)
redis = Redis()   #initiate a redis instance

# database engine
sql_uri = 'postgresql://readonly:w2UIO@#bg532!@work-samples-db.cx4wctygygyq.us-east-1.rds.amazonaws.com:5432/work_samples'
engine = sqlalchemy.create_engine(sql_uri)

#----------------------------- Rate-limiter helper functions--------------------------------#

#This is method is used to get the api route that was currently
#accessed by a specific address
def getRoute():
    rule = request.url_rule
    return rule.rule    #returns the route as a string


"""
This method takes in a redis instance as a paramter, and sets the instance to pipeline
instruction, so i dont have to remember to do it later. Returns the redis instance
"""
def initializeRedis(red):
    multiinstruc = red.pipeline(transaction = True)     #all instructions wil either be executed
                                                        # or returned to previous state (in case of an error)
    return multiinstruc
    
"""
This is a boolean method, that takes in 3 parameters, a redis connection, a key prefix
and limit list. This method will be used to check if a limit is reached on a certain api endpoit
by a specifc user. 
"""
def reached_limit(connection, key_prefix, limit):
    for dur, limit in limit:    #iterate over tuple in limit
        resetTracker = int ((time.time()) // dur) * dur + dur   #get the time to reset request limit duration
        container = ':%i : %i'%(dur, resetTracker)      #bucket to append to key_prefix, track duration and time left
        
        key = key_prefix + str(container)

        connection.incr(key)    #increment the key as required
        connection.expireat(key , resetTracker) #so the database doesn't hold a whole bunch of unused keys, 
                                                # expire the keys after a certain time period

        if connection.execute() [0] >= limit:   #check if the limit has been reached with the increment counter 
            return True
    
    return False

"""
This method returns a message along wit a 429 error, once the request limit has been reached.

"""
def over_limit_message():
    message = 'Too many request, wait and try again: '
    return message + '429 error', 429

"""
Decorator fucntion to wrap around mylimiter decorator, takes in a list of tuples that represent duration
times and request per durations. limits = [(seconds, # of request)]
"""
def mylimiter(f, limits = [(1,10), (60, 10), (3600, 300)]):
    @wraps(f)
    def rate_limiter(*args, **kwargs):
        who = request.remote_addr   #get the current user ip
        route = getRoute()      #get the route the user accessed, using above getRoute method
        connection = initializeRedis(redis) #setup the redis connection pipline 
        key_prefix = 'limiter/%s/%s/' % (route, who)    #generate the key_prefix using who and route
        if reached_limit(connection, key_prefix, limits):   #call on the reached_limit method passing in the appropriate parameters
                                                            #to see if limit has been reached
            return  over_limit_message()
        return f(*args, **kwargs)
    return rate_limiter


#------------------------------API ENDPOINTS-----------------------------#

@app.route('/')
@mylimiter
def index():
    
    return 'Welcome to EQ Works 😎'


@app.route('/events/hourly')
@mylimiter
def events_hourly():
    return queryHelper('''
        SELECT date, hour, events
        FROM public.hourly_events
        ORDER BY date, hour
        LIMIT 168;
    ''')


@app.route('/events/daily')
@mylimiter
def events_daily():
    return queryHelper('''
        SELECT date, SUM(events) AS events
        FROM public.hourly_events
        GROUP BY date
        ORDER BY date
        LIMIT 7;
    ''')


@app.route('/stats/hourly')
@mylimiter
def stats_hourly():
    return queryHelper('''
        SELECT date, hour, impressions, clicks, revenue
        FROM public.hourly_stats
        ORDER BY date, hour
        LIMIT 168;
    ''')


@app.route('/stats/daily')
@mylimiter
def stats_daily():
    return queryHelper('''
        SELECT date,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(revenue) AS revenue
        FROM public.hourly_stats
        GROUP BY date
        ORDER BY date
        LIMIT 7;
    ''')

@app.route('/poi')
@mylimiter
def poi():
    return queryHelper('''
        SELECT *
        FROM public.poi;
    ''')

def queryHelper(query):
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return jsonify([dict(row.items()) for row in result])

#------------------------MAIN METHOD--------------------#
if __name__ == '__main__':
    app.run(debug =True)
