#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script ensures all AO and FirstF channels are joined by PAXminer and it also ensures that the required log and plot directories exist.
'''

from slack_sdk import WebClient
import pandas as pd
import matplotlib

from db_connection_manager import DBConnectionManager

matplotlib.use('Agg')
import os
import sys

db = sys.argv[1]
mydb = DBConnectionManager('../config/credentials.ini').connect(db)

region = sys.argv[3]

# Set Slack token
key = sys.argv[2]
slack = WebClient(token=key)
firstf = sys.argv[4] #designated 1st-f channel for the region


print("Setting up PAXminer environment for region " + region)
print("Joining FirstF channel")
try:
    slack.conversations_join(channel=firstf)
    print('Joined FirstF channel ' + firstf)
except:
    print('Could not join firstf')

try:
    with mydb.cursor() as cursor:
        sql = "SELECT channel_id, ao FROM aos WHERE backblast = 1 and archived = 0"
        cursor.execute(sql)
        aos = cursor.fetchall()
        aos_df = pd.DataFrame(aos, columns={'channel_id','ao'})
finally:
    print('Pulling all AO channels... Stand by...')

# Join each AO channel
print("Ensuring PAXminer is a member of all AO channels...")
for index, row in aos_df.iterrows():
    ao = row['ao']
    channel_id = row['channel_id']
    print('Joining AO ' + ao)
    try:
        slack.conversations_join(channel=channel_id)
    except:
        print('An Error Occurred in Joining ' + ao + " " + channel_id)

#Make sure log and plot directories are created
plotdir ='plots/' + db
logdir = 'logs/' + db
parent_dir = "./"

# Plot Path
plotpath = os.path.join(parent_dir, plotdir)

# Log Path
logpath = os.path.join(parent_dir, logdir)

# Create the directories
print("Creating required log and plot directories for region " + region)
try:
    os.mkdir(plotpath)
    print("Directory '%s' created" % plotpath)
except OSError as ploterror:
    print(ploterror)
try:
    os.mkdir(logpath)
    print("Directory '%s' created" % logpath)
except OSError as logerror:
    print(logerror)

print('End of preparations')