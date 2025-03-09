#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script queries Slack for all PAX Users and their respective beatdown attendance. It then generates bar graphs
on attendance for each member and sends it to them in a private Slack message.
'''

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import pandas as pd
import pymysql.cursors
import configparser
import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import sys
import os
import logging
# This handler does retries when HTTP status 429 is returned
from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler

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
# Enable rate limited error retries
rate_limit_handler = RateLimitErrorRetryHandler(max_retry_count=5)
slack.retry_handlers.append(rate_limit_handler)

#Define AWS Database connection criteria
mydb = pymysql.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    db=db,
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor)

#Get Current Year, Month Number and Name
d = datetime.datetime.now()
d = d - datetime.timedelta(days=15)
thismonth = d.strftime("%m")
thismonthname = d.strftime("%b")
thismonthnamelong = d.strftime("%B")
yearnum = d.strftime("%Y")

# Set up logging
logging.basicConfig(filename='../logs/' + db + '/PAXcharter_error.log',
                            filemode = 'a',
                            format='%(asctime)s %(levelname)-8s %(message)s',
                            datefmt = '%Y-%m-%d %H:%M:%S',
                            level = logging.INFO)
logging.info("Running PAXminer for " + db)

print('Looking for all Slack Users for ' + db + '. Stand by...')

# Make users Data Frame
column_names = ['user_id', 'user_name', 'real_name']
users_df = pd.DataFrame(columns = column_names)
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

print('Now pulling all of those users beatdown attendance records... Stand by...')

## Send Slack Message ( v1 )
def send_slack_message(channel, message, file):
    return slack.files_upload(
        channels=channel,
        initial_comment=message,
        file=file
    )

## Send Slack Message ( v2 )
def send_slack_message_v2(user_id, message, file):
    # V2 File Upload requires a conversation to be opened 
    response = slack.conversations_open(users=user_id)
    channel = response["channel"]["id"]

    return slack.files_upload_v2(
        channel=channel,
        initial_comment=message,
        file=file
    )

def log_message_sent_error(user_id_tmp, db, pax):
    print(f"Error initiating conversation: {e.response['error']}")
    os.system("echo Error: " + user_id_tmp + " >>" + "../logs/" + db + "/PAXcharter.log")
    logging.warning("Slack Error - Message not sent:", pax, user_id_tmp)
    print("Slack error on " + pax + " " + user_id_tmp)

def success_message_sent(user_id_tmp, pax, db):
    os.system("echo " + user_id_tmp + " " + pax + " >>" + "../logs/" + db + "/PAXcharter.log")
    
# Query AWS by user ID for attendance history
#users_df = users_df.iloc[:10] # THIS LINE IS FOR TESTING PURPOSES, THIS FORCES ONLY n USER ROWS TO BE SENT THROUGH THE PIPE
total_graphs = 0 # Sets a counter for the total number of graphs made (users with posting data)
pause_on = [ 50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000 ]

region_method = "v2"
for user_id in users_df['user_id']:
    try:
        attendance_tmp_df = pd.DataFrame([])  # creates an empty dataframe to append to
        with mydb.cursor() as cursor:
                sql = "SELECT * FROM attendance_view WHERE PAX = (SELECT user_name FROM users WHERE user_id = %s) AND YEAR(Date) = %s ORDER BY Date"
                user_id_tmp = user_id
                val = (user_id_tmp, yearnum)
                cursor.execute(sql, val)
                attendance_tmp = cursor.fetchall()
                attendance_tmp_df = pd.DataFrame(attendance_tmp)
                month = []
                day = []
                year = []
                count = attendance_tmp_df.shape[0]
                #if user_id_tmp == 'U0187M4NWG4': #Use this to send a graph to only 1 specific PAX
                if count > 0: # This sends a graph to ALL PAX who have attended at least 1 beatdown
                    for Date in attendance_tmp_df['Date']:
                    #for index, row in attendance_tmp_df.iterrows():
                        datee = datetime.datetime.strptime(Date, "%Y-%m-%d")
                        month.append(datee.strftime("%B"))
                        day.append(datee.day)
                        year.append(datee.year)
                    pax = attendance_tmp_df.iloc[0]['PAX']
                    attendance_tmp_df['Month'] = month
                    attendance_tmp_df['Day'] = day
                    attendance_tmp_df['Year'] = year
                    attendance_tmp_df.sort_values(by=['Date'], inplace=True)
                    attendance_tmp_df.groupby(['Month', 'AO'], sort=False).size().unstack().plot(kind='bar',stacked=True)

                    ax = attendance_tmp_df.groupby(['Month', 'AO'], sort=False).size().unstack().plot(kind='bar', stacked=True)
                    total_count_for_year = attendance_tmp_df.shape[0]

                    # Calculate total count for the last month
                    last_month_start = datetime.date(int(yearnum), int(thismonth), 1)
                    attendance_last_month_df = attendance_tmp_df[attendance_tmp_df['Date'] >= str(last_month_start)]
                    total_count_last_month = attendance_last_month_df.shape[0]

                    if total_count_last_month > 0:
                        # Add the total count as text on the chart
                        ax.text(0.95, 0.95, f"Total: {total_count_for_year}", transform=ax.transAxes, 
                                fontsize=12, verticalalignment='top', horizontalalignment='right')
                        
                        plt.title('Number of posts by '+ pax + ' by AO/Month for ' + yearnum)
                        plt.legend(loc = 'center left', bbox_to_anchor=(1, 0.5), frameon = False)
                        plt.ioff()
                        plt.savefig('../plots/' + db + '/' + user_id_tmp + "_" + thismonthname + yearnum + '.jpg', bbox_inches='tight') #save the figure to a file
                        
                        message = 'Hey ' + pax + "! Here is your monthly posting summary for " + yearnum + ". \nPush yourself, get those bars higher every month! SYITG!"
                        file = '../plots/' + db + '/' + user_id_tmp + "_" + thismonthname + yearnum + '.jpg'

                        print('PAX posting graph created for user', pax, 'Sending to Slack now... hang tight!')
                        
                        # The current method v2, and legacy method, can both be invoked here depending on the region_method variable.
                        # Most regions still use the legacy method, but will need to migrate to v2 by Spring 2025. 
                        # The main difference is that v2 requires an additional conversation scope.
                        # New regions will all use v2.
                        # user_id_override = "U06GDMGJKNE"
                        if region_method == "v2":
                            try:
                                response = send_slack_message_v2(user_id_tmp, message, file)

                                success_message_sent(user_id_tmp, pax, db)
                            except Exception as e:
                                # If the error is missing scope, then 
                                if e.response['error'] == 'missing_scope':
                                    print("Error: The app is missing required scopes. Please add the 'im:write' scope.")
                                    region_method = "v1"
                                else:
                                    log_message_sent_error(user_id_tmp, db, pax)
                                    raise e

                        if region_method != "v2":
                            try:
                                channel = user_id_tmp
                                response = send_slack_message(channel, message, file)
                                
                                success_message_sent(user_id_tmp, pax, db)
                            except:
                                log_message_sent_error(user_id_tmp, db, pax)
                                raise e
                    else:
                        print(pax + ' skipped')
    except Exception as e:
            print(e)
            print("An exception occurred for User ID " + user_id)
    finally:
        plt.close('all') #Note - this was added after the December 2020 processing, make sure this works
print('Total graphs made:', total_graphs)
mydb.close()