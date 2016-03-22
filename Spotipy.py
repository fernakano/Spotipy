import csv #csv to process csv input
import sqlite3 #sqlite to help query data easier after preprocessing

#defined session in ms
session_time = 600000

#Local support variables
sessiondict = {'session_id': 0} 
counter = 0

# This can be tuned to work without using this sqlite and working with internal lists and dictionaries.
# Considering the provided data i decided that the simplest tool for the task was to write my analysis using any SQL db.
# Initiate sqlitedb in memory, without persisting
db = sqlite3.connect(':memory:')
dbc = db.cursor()

#Create Tables
dbc.execute('''CREATE TABLE usage (ms_played int, context text, track_id text, product text, end_timestamp number,   user_id text)''')
dbc.execute('''CREATE TABLE users (gender text, age_range text, country text, acct_age_weeks int, user_id text)''')
dbc.execute('''CREATE TABLE sessions (user_id text, session_id int,session_start int, session_end int, PRIMARY KEY (user_id, session_id))''')
db.commit()

#Support Functions
def getUserInfo(userlist,uid):
    return filter(lambda user: user['user_id'] == uid, userlist)[0]

def getUserIdInList(userlist,uid):
    for i, row in enumerate(userlist):
        if row['user_id'] == uid:
            return i

#Process Data for easier query
with open('user_data_sample.csv', 'r') as fusers:
    rusers = csv.reader(fusers)
    next(rusers)
    for row in rusers:
        #Load users data
        db.execute('''INSERT INTO users VALUES (?,?,?,?,?)''', row)
    db.commit()


with open('end_song_sample.csv', 'r') as fusage:
    rusage = csv.DictReader(fusage)
    #Sort List for later session breaks
    sortedusage = list(sorted(rusage, key = lambda x: (x['user_id'], x['end_timestamp'])))
    for i, row in enumerate(sortedusage):
        #Load usage date
        db.execute('''INSERT INTO usage (ms_played,context,track_id,product,end_timestamp,user_id) VALUES (:ms_played,:context,:track_id,:product,:end_timestamp,:user_id)''', row)
        # if counter > 10:
            # break
        if i+1 >= len(sortedusage): break
        #Initialize Session
        if sessiondict['session_id'] == 0: 
            sessiondict = {
                'user_id' : row['user_id'],
                'session_id' : 1,
                'session_start': row['end_timestamp'],
                'session_end': row['end_timestamp']
            }

        #Calculate session using timestamp if its a contiguous user.
        if (sortedusage[i+1]['user_id'] == row['user_id']):
            #will compare next timestamo with current and check session time with configured defined session 
            if (float(sortedusage[i+1]['end_timestamp']) - float(row['end_timestamp'])) >= session_time:   
                #save current session in DB (this could be optimized running maybe with only internal lists before sending)
                db.execute('''INSERT OR REPLACE INTO sessions (user_id,session_id,session_start,session_end) VALUES (:user_id,:session_id,:session_start,:session_end)''', sessiondict)
                #Update session time and session counter per new session in user.
                sessiondict['session_start'] = sortedusage[i+1]['end_timestamp']
                sessiondict['session_id'] += 1
            else:
                #Update session end time if session is continued
                sessiondict['session_end'] = row['end_timestamp']
        else:
            sessiondict['session_id'] = 0
            counter += 1
    db.commit()

#Run Queries
# Warm-up (please do this first):
# Determine whether male and female listeners are significantly different in their overall listening (in terms of the count of track listens, or in terms of the total time spent listening)
dbc.execute('''SELECT users.gender, count(usage.track_id), count(DISTINCT usage.track_id), sum(ms_played)
                FROM usage
                INNER JOIN users ON (users.user_id = usage.user_id)
                GROUP BY users.gender
                ''')

pieGender = {
    'activities' : [],
    'totaltime' : [],
    'labels' : []
}

print (','.join(['Gender','Activities','Unique Tracks','Total Time Played']))
for row in dbc:
    print (','.join(str(bit) for bit in row))
    pieGender['labels'].extend(str(row[0]))
    pieGender['activities'].extend(str(row[1]))
    pieGender['totaltime'].extend(str(row[3]))

# Analysis suggestion 1: 
# Break the user listening into sessions (exactly what is a listening session is up to you to define)
# Look for correlations between user demographic features (or their behavior) and their overall listening, or their average session lengths
# Analysis suggestion 2:
# Find a clustering of user categories that delineates some interesting or useful behavior traits (or show that no clustering makes sense)


# Session time was calculated based on configured session_time_threshold variable on beggining of application
# Initially set for 600000ms, which means that if the user stop playing music for 600000ms or more, it will start a new Playing session for that user.

# Find Overall Listen information for each active User Per Countries that contain data matching a profile
# Total Seconds of music playing  
# Average User Session time, based on configured session_time_threshold variable on beggining of application
# Number of Unique Tracks
# Number of Total Play Activities 
dbc.execute('''SELECT  users.country, 
                        SUM(usage.ms_played/1000),
                        AVG((julianday(datetime(sessions.session_end,'unixepoch'))- julianday(datetime(sessions.session_start,'unixepoch')))* 86400.0),
                        COUNT(DISTINCT usage.track_id), 
                        COUNT(1)
                FROM usage
                INNER JOIN sessions ON (usage.user_id = sessions.user_id)
                INNER JOIN users ON (usage.user_id = users.user_id)                
                GROUP BY users.country
                ORDER BY users.country''')
print ','.join(['Country','Total Seconds Played','Average Session Time','Unique Tracks','Total Activities'])
for row in dbc:
    print ','.join(str(bit) for bit in row)    

# Find Overall Listen information for each active User Per Age Band that contain data matching a profile
# Total Seconds of music playing  
# Average User Session time
# Number of Unique Tracks
# Number of Total Play Activities 
dbc.execute('''SELECT  users.age_range, 
                        SUM(usage.ms_played/1000),
                        AVG((julianday(datetime(sessions.session_end,'unixepoch'))- julianday(datetime(sessions.session_start,'unixepoch')))* 86400.0),
                        COUNT(DISTINCT usage.track_id), 
                        COUNT(1)
                FROM usage
                INNER JOIN sessions ON (usage.user_id = sessions.user_id)
                INNER JOIN users ON (usage.user_id = users.user_id)                
                GROUP BY users.age_range
                ORDER BY users.age_range''')
print ','.join(['Age Range','Total Seconds Played','Average Session Time','Unique Tracks','Total Activities'])
for row in dbc:
    print ','.join(str(bit) for bit in row)    

# Find Overall Listen information for each active User, Grouping by Date, Country, Gender, age_range
# This Grouping Set will give a rich Data Mart to be analyzed on Analytics tools like Tableau or to be preprocessed with D3 to add some cool HTML charting visualizations.
# This will also output a much bigger Summary dataset than the other outputs.
# Total Seconds of music playing  
# Average User Session time
# Number of Unique Tracks
# Number of Total Play Activities 
dbc.execute('''SELECT DATE(end_timestamp,'unixepoch'), 
                        users.country,
                        users.gender,
                        users.age_range,
                        usage.product,
                        COUNT(1), 
                        COUNT(DISTINCT usage.track_id),
                        SUM(ms_played), 
                        COUNT(DISTINCT users.user_id),
                        AVG((julianday(datetime(sessions.session_end,'unixepoch'))- julianday(datetime(sessions.session_start,'unixepoch')))* 86400.0)
                FROM usage
                INNER JOIN users ON (users.user_id = usage.user_id)
                INNER JOIN sessions ON (usage.user_id = sessions.user_id)                
                GROUP BY usage.product,users.country, users.gender,users.age_range,DATE(end_timestamp,'unixepoch')
                ORDER BY DATE(end_timestamp,'unixepoch'), users.country
                ''')
print ','.join(['Date','Country','Gender','Age Range','Product','Activities','Unique Tracks','Total ms Played','Unique Subscribers','Averate Session Time'])
for row in dbc:
    print ','.join(str(bit) for bit in row)
