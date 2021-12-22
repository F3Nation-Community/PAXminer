#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script queries Slack for User, Channel, and Conversation (channel) history and then parses all conversations to find Backblasts.
All Backblasts are then parsed to collect the PAX that attend any given workout and puts those attendance records into the AWS database for recordkeeping.
'''

import warnings
#from slacker import Slacker
from slack_sdk import WebClient
from datetime import datetime, timedelta
import pandas as pd
import pytz
import re
import pymysql.cursors
import configparser
import sys
import logging
import dateparser

# Configure AWS credentials
config = configparser.ConfigParser();
config.read('../config/credentials.ini');
host = config['aws']['host']
port = int(config['aws']['port'])
user = config['aws']['user']
password = config['aws']['password']
db = sys.argv[1]

# Set Slack token
key = sys.argv[2]
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

# Set epoch and yesterday's timestamp for datetime calculations
epoch = datetime(1970, 1, 1)
yesterday = datetime.now() - timedelta(days = 1)
oldest = yesterday.timestamp()
today = datetime.now()
cutoff_date = today - timedelta(days = 14) # This tells PAXminer to go back up to N days for message age
cutoff_date = cutoff_date.strftime('%Y-%m-%d')
date_time = today.strftime("%m/%d/%Y, %H:%M:%S")

# Set up logging
logging.basicConfig(filename='../logs/PAXminer.log',
                            filemode = 'a',
                            format='%(asctime)s %(levelname)-8s %(message)s',
                            datefmt = '%Y-%m-%d %H:%M:%S',
                            level = logging.INFO)
logging.info("Running PAXminer for " + db)

# Send a message to #general channel to make sure script is working :)
#slack.chat.post_message('#general', 'Don't mind me, I'm debugging PAXminer again!')

# Make users Data Frame
column_names = ['user_id', 'user_name', 'real_name']
users_df = pd.DataFrame(columns = column_names)
users_df.loc[len(users_df.index)] = ['APP', 'BackblastApp', 'BackblastApp']
data = ''
while True:
    users_response = slack.users_list(limit=1000, cursor=data)
    response_metadata = users_response.get('response_metadata', {})
    next_cursor = response_metadata.get('next_cursor')
    users = users_response.data['members']
    users_df_tmp = pd.json_normalize(users)
    users_df_tmp = users_df_tmp[['id', 'profile.display_name', 'profile.real_name']]
    users_df_tmp = users_df_tmp.rename(columns={'id' : 'user_id', 'profile.display_name' : 'user_name', 'profile.real_name' : 'real_name'})
    users_df = users_df.append(users_df_tmp, ignore_index=True)
    if next_cursor:
        # Keep going from next offset.
        #print('next_cursor =' + next_cursor)
        data = next_cursor
    else:
        break
for index, row in users_df.iterrows():
    un_tmp = row['user_name']
    rn_tmp = row['real_name']
    if un_tmp == "" :
        row['user_name'] = rn_tmp

'''
# Get channel list from Slack (note - this has been replaced with a channel list from the AWS database)
channels_response = slack.conversations.list()
channels = channels_response.body['channels']
channels_df = pd.json_normalize(channels)
channels_df = channels_df[['id', 'name', 'created', 'is_archived']]
channels_df = channels_df.rename(columns={'id' : 'channel_id', 'name' : 'channel_name', 'created' : 'channel_created', 'is_archived' : 'archived'})
'''

# Retrieve Channel List from AWS database (backblast = 1 denotes which channels to search for backblasts)
try:
    with mydb.cursor() as cursor:
        sql = "SELECT channel_id, ao FROM aos WHERE backblast = 1 and archived = 0"
        cursor.execute(sql)
        channels = cursor.fetchall()
        channels_df = pd.DataFrame(channels, columns={'channel_id', 'ao'})
finally:
    print('Finding all PAX that attended recent workouts - stand by.')

# Get all channel conversation
messages_df = pd.DataFrame([]) # creates an empty dataframe to append to
for id in channels_df['channel_id']:
    data = ''
    pages = 1
    while True:
        try:
            #print("Checking channel " + id) # <-- Use this if debugging any slack channels throwing errors
            response = slack.conversations_history(channel=id, cursor=data)
            response_metadata = response.get('response_metadata', {})
            next_cursor = response_metadata.get('next_cursor')
            messages = response.data['messages']
            temp_df = pd.json_normalize(messages)
            temp_df = temp_df[['user', 'type', 'text', 'ts']]
            temp_df["user"] = temp_df["user"].fillna("APP")
            temp_df = temp_df.rename(columns={'user' : 'user_id', 'type' : 'message_type', 'ts' : 'timestamp'})
            temp_df["channel_id"] = id
            messages_df = messages_df.append(temp_df, ignore_index=True)
        except:
            print("Error: Unable to access Slack channel:", id, "in region:",db)
            logging.warning("Error: Unable to access Slack channel %s in region %s", id, db)
        if next_cursor:
            # Keep going from next offset.
            #print('Next Page Cursor:', next_cursor)
            data = next_cursor
            if pages == 1: ##Total number of pages to return from Slack
                break
            pages = pages + 1
        else:
            #print('Finished finding PAX... parsing...')
            break
# Calculate Date and Time columns
msg_date = []
msg_time = []
for ts in messages_df['timestamp']:
        seconds_since_epoch = float(ts)
        dt = epoch + timedelta(seconds=seconds_since_epoch)
        dt = dt.replace(tzinfo=pytz.utc)
        dt = dt.astimezone(pytz.timezone('America/Chicago'))
        msg_date.append(dt.strftime('%Y-%m-%d'))
        msg_time.append(dt.strftime('%H:%M:%S'))
messages_df['msg_date'] = msg_date
messages_df['time'] = msg_time

# Merge the data frames into 1 joined DF
f3_df = pd.merge(messages_df, users_df)
f3_df = pd.merge(f3_df,channels_df)
f3_df = f3_df[['timestamp', 'msg_date', 'time', 'channel_id', 'ao', 'user_id', 'user_name', 'real_name', 'text']]

# Now find only backblast messages (either "Backblast" or "Back Blast") - note .casefold() denotes case insensitivity - and pull out the PAX user ID's identified within
# This pattern finds username links followed by commas: pat = r'(?<=\\xa0).+?(?=,)'
pat = r'(?<=\<).+?(?=>)' # This pattern finds username links within brackets <>
pax_attendance_df = pd.DataFrame([])
warnings.filterwarnings("ignore", category=DeprecationWarning) #This prevents displaying the Deprecation Warning that is present for the RegEx lookahead function used below

def list_pax():
    #find Q info
    qline = re.findall(r'(?<=\n)\*?V?Qs?\*?:.+?(?=\n)', str(text_tmp),
                       re.MULTILINE)  # This is regex looking for \nQ: with or without an * before Q
    qids = re.findall(pat, str(qline), re.MULTILINE)
    qids = [re.sub(r'@', '', i) for i in qids]
    if qids:
        qid = qids[0]
    else:
        qid = 'NA'
    if len(qids) > 1:
        coqid = qids[1]
    else:
        coqid = 'NA'
    #paxline = [line for line in text_tmp.split('\n') if 'pax'.casefold() in line.casefold()]
    paxline = re.findall(r'(?<=\n)\*?(?i)PAX\*?:\*?.+?(?=\n)', str(text_tmp), re.MULTILINE) #This is a case insensitive regex looking for \nPAX with or without an * before PAX
    #print(paxline)
    pax = re.findall(pat, str(paxline), re.MULTILINE)
    pax = [re.sub(r'@','', i) for i in pax]
    if pax:
        global pax_attendance_df
        #print(pax)
        df = pd.DataFrame(pax)
        df.columns =['user_id']
        df['ao'] = ao_tmp
        # Find the Date:
        dateline = re.findall(r'(?<=\n)Date:.+?(?=\n)', str(text_tmp), re.IGNORECASE)
        msg_date = row['msg_date']
        if dateline:
            # print("First dateline: " + dateline)
            dateline = re.sub("Date:\s?", '', str(dateline), flags=re.I)
            # print("Removed Date: " + dateline)
            dateline = dateparser.parse(
                dateline)  # dateparser is a flexible date module that can understand many different date formats
            # print("Parsed:")
            # print(dateline)
            if dateline is None:
                date_tmp = '2099-12-31'  # sets a date many years in the future just to catch this error later (needs to be a future date)
            else:
                date_tmp = str(datetime.strftime(dateline, '%Y-%m-%d'))
        else:
            date_tmp = msg_date
        df['bd_date'] = date_tmp
        df['msg_date'] = msg_date
        df['q_user_id'] = qid
        pax_attendance_df = pax_attendance_df.append(df)
# Iterate through the new f3_df dataframe, pull out the channel_name, date, and text line from Slack. Process the text line to find the Pax list
for index, row in f3_df.iterrows():
    ao_tmp = row['channel_id']
    text_tmp = row['text']
    text_tmp = re.sub('_\\xa0', ' ', str(text_tmp))
    text_tmp = re.sub('\\xa0', ' ', str(text_tmp))
    text_tmp = re.sub('_\*', '', str(text_tmp))
    text_tmp = re.sub('\*_', '', str(text_tmp))
    text_tmp = re.sub('\*', '', str(text_tmp))
    if re.findall('^Slackblast', text_tmp, re.IGNORECASE | re.MULTILINE):
        list_pax()
    elif re.findall('^\*Backblast', text_tmp, re.IGNORECASE | re.MULTILINE):
        list_pax()
    elif re.findall('^\*Back blast', text_tmp, re.IGNORECASE | re.MULTILINE):
        list_pax()
    elif re.findall('^Back blast', text_tmp, re.IGNORECASE | re.MULTILINE):
        list_pax()
    elif re.findall('^Backblast', text_tmp, re.IGNORECASE | re.MULTILINE):
        list_pax()
    elif re.findall('^Slack blast', text_tmp, re.IGNORECASE | re.MULTILINE):
        list_pax()
    elif re.findall('^\*Slackblast', text_tmp, re.IGNORECASE | re.MULTILINE):
        bd_info()
    elif re.findall('^\*Slack blast', text_tmp, re.IGNORECASE | re.MULTILINE):
        list_pax()
    elif re.findall('^\*Sackblast', text_tmp, re.IGNORECASE | re.MULTILINE):
        list_pax()
    elif re.findall('^Sackblast', text_tmp, re.IGNORECASE | re.MULTILINE):
        list_pax()
    elif re.findall('^Slackbast', text_tmp, re.IGNORECASE | re.MULTILINE):
        list_pax()
    elif re.findall('^\*Slackbast', text_tmp, re.IGNORECASE | re.MULTILINE):
        list_pax()
    elif re.findall('^Sackdraft', text_tmp, re.IGNORECASE | re.MULTILINE):
        list_pax()
    elif re.findall('^\*Sackdraft', text_tmp, re.IGNORECASE | re.MULTILINE):
        list_pax()
    text_tmp = re.sub('\*', '', text_tmp, re.MULTILINE)

# Now connect to the AWS database and insert some rows!
inserts = 0
try:
    with mydb.cursor() as cursor:
        for index, row in pax_attendance_df.iterrows():
            sql = "INSERT IGNORE INTO bd_attendance (user_id, ao_id, date, q_user_id) VALUES (%s, %s, %s, %s)"
            user_id_tmp = row['user_id']
            msg_date = row['msg_date']
            ao_tmp = row['ao']
            date_tmp = row['bd_date']
            q_user_id = row['q_user_id']
            val = (user_id_tmp, ao_tmp, date_tmp, q_user_id)
            if msg_date > cutoff_date:
                if date_tmp == '2099-12-31':
                    print('Backblast error on Date - AO:', ao_tmp, 'Date:', date_tmp, 'Posted By:', user_id_tmp)
                else:
                    if q_user_id != 'NA':
                        cursor.execute(sql, val)
                        mydb.commit()
                        if cursor.rowcount > 0:
                            print(cursor.rowcount, "record inserted for", user_id_tmp, "at", ao_tmp, "on", date_tmp, "with Q =", q_user_id)
                            inserts = inserts + 1
finally:
    mydb.close()
logging.info("PAXminer complete: Inserted %s new PAX attendance records for region %s", inserts, db)
#try:
#    slack.chat_postMessage(channel='paxminer_logs', text=date_time + " PAXminer run complete for " + db)
#except:
#    pass
print('Finished. You may go back to your day!')