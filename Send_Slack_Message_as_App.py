#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script allows for manually pushing messages from the PAXminer app or the Slackblast app to all AO channels for messaging purposes.
'''

from slack_sdk import WebClient
import pandas as pd
import pymysql.cursors
import configparser
import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import configparser
import sys

# Configure AWS credentials
config = configparser.ConfigParser();
config.read('../config/credentials.ini');
host = config['aws']['host']
port = int(config['aws']['port'])
user = config['aws']['user']
password = config['aws']['password']
#db = sys.argv[1]
db = 'f3stl'
#region = sys.argv[3]
region = 'STL'

# Set Slack token
#key = sys.argv[2]
key = 'enter app key here'
slack = WebClient(token=key)

#Define AWS Database connection criteria
mydb = pymysql.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    db=db,
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor)

try:
    with mydb.cursor() as cursor:
        sql = "SELECT channel_id, ao FROM aos WHERE backblast = 1"
        cursor.execute(sql)
        aos = cursor.fetchall()
        aos_df = pd.DataFrame(aos, columns={'channel_id','ao'})
finally:
    print('Pulling all AO channels... Stand by...')

#File to upload to Slack with message:
filename = "./Slackblast_screen.jpg"

# Push messages to each channel
for index, row in aos_df.iterrows():
    ao = row['ao']
    channel_id = row['channel_id']
    print('Sending message to AO ' + ao)
    try:
        slack.conversations_join(channel=channel_id)
        #slack.chat_postMessage(channel=ao, text="Hey " + ao + "! I'm a new tool you can use to help make writing preblasts and backblasts easier! By using simple slash commands, you can fill out a form for pre- and backblasts and I will post them to the channel you select in the appropriate format! No more copy/paste issues to have to deal with.\n\nTo post a preblast, just type */preblast* - and I'll pop up a form for you to fill out. \nTo post a backblast, just type */backblast* for a different form. \n\nMake sure to select the right channel. Beatdown pre and backblasts go to the AO channel, QSource pre and backblasts goes to #qsource, blackops goes to #blackops. Contact <@U015U6WFWP5> for questions or problems. Note - this is optional, you can still post your pre and backblasts the old copy & paste way as well. \n\nNote: there is one caveat. If you post using me, your messages aren't editable once posted... so make sure you're happy with it!")
        slack.files_upload(channels=ao, initial_comment="Hey " + ao + "! I'm a new tool you can use to help make writing preblasts and backblasts easier! By using simple slash commands, you can fill out a form for pre- and backblasts and I will post them to the channel you select in the appropriate format (so <@U0187M4NWG4> won't have to come after you!). No more copy/paste issues to have to deal with.\n\nTo post a preblast, just type */preblast* - and I'll pop up a form for you to fill out. \nTo post a backblast, just type */backblast* for a different form. \n\nMake sure to select the right channel. Beatdown pre and backblasts go to the AO channel, QSource pre and backblasts goes to #qsource, blackops goes to #blackops. Contact <@U015U6WFWP5> for questions or problems. This is optional, you can still post your pre and backblasts the old copy & paste way as well. \n\nNote: there is an important caveat: If you post using me, your messages aren't editable once posted... so make sure you're happy with it!", file=filename)
    except:
        print('An Error Occurred in Sending ' + ao + " " + channel_id)
    finally:
                print('Message Sent')
print('End of messages')