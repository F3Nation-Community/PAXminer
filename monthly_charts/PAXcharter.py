#!/usr/bin/env python3

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
import hashlib
import numpy as np
import calendar
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


def hash_email(email):
    if isinstance(email, str):
        return hashlib.md5(email.encode('utf-8')).hexdigest()
    else:
        return None  # Or return a default value if needed

# Make users Data Frame
column_names = ['user_id', 'user_name', 'real_name', 'email']
users_df = pd.DataFrame(columns = column_names)
data = ''
while True:
    users_response = slack.users_list(limit=1000, cursor=data)
    response_metadata = users_response.get('response_metadata', {})
    next_cursor = response_metadata.get('next_cursor')
    users = users_response.data['members']
    users_df_tmp = pd.json_normalize(users)
    users_df_tmp = users_df_tmp[['id', 'profile.display_name', 'profile.real_name', 'profile.email']]
    users_df_tmp = users_df_tmp.rename(columns={'id' : 'user_id', 'profile.display_name' : 'user_name', 'profile.real_name' : 'real_name', 'profile.email' : 'email'})
    users_df = users_df.append(users_df_tmp, ignore_index=True)

    # Apply the hash function to the email column
    users_df['email'] = users_df['email'].apply(hash_email)
    if next_cursor:
        # Keep going from next offset.
        #print('next_cursor =' + next_cursor)
        data = next_cursor
    else:
        break
for index, row in users_df.iterrows():
    un_tmp = row['user_name']
    rn_tmp = row['real_name']
    row['email']
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
pause_on = [ 50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000 ]

def savePlot(aggregated_data, total_count_for_year, title, file_path):
    # Plot the stacked bar chart
    ax = aggregated_data.plot(kind='bar', stacked=True)
    # Add the total count as text on the chart
    ax.text(0.95, 0.95, f"Total: {total_count_for_year}", transform=ax.transAxes, 
            fontsize=12, verticalalignment='top', horizontalalignment='right')
    plt.title(title)
    plt.legend(loc = 'center left', bbox_to_anchor=(1, 0.5), frameon = False)
    plt.ioff()
    plt.savefig(file_path, bbox_inches='tight') #save the figure to a file
    plt.close()

def execute_user_chart(attendance_tmp, user_id_tmp, db, pax, d, thismonthname, yearnum, rm):
    if attendance_tmp and len(attendance_tmp) > 0:
        attendance_tmp_df = pd.DataFrame(attendance_tmp)
        thismonth = d.strftime("%m")
        send_chart = attendance_tmp_df[(attendance_tmp_df['table_schema'] == db) & (attendance_tmp_df['Month'] == int(thismonth))].shape[0]
        if send_chart > 0: # This sends a graph to ALL PAX who have attended at least 1 beatdown
            rgion_method = rm
            # Modify the 'AO' column based on the condition where 'table_schema' is not equal to 'db'
            attendance_tmp_df['AO'] = np.where(attendance_tmp_df['table_schema'] != db, "DR: " + attendance_tmp_df['region'], attendance_tmp_df['AO'])

            attendance_tmp_df.sort_values(by=['Month'], inplace=True)
            attendance_tmp_df['Month'] = attendance_tmp_df['Month'].map(lambda x: calendar.month_abbr[x])
            
            # Group by 'Month' and 'AO', and aggregate the 'cnt' column by summing it
            aggregated_data = attendance_tmp_df.groupby(['Month', 'AO'], sort=False)['cnt'].sum().unstack()

            # Calculate the total count for the year from the 'cnt' column
            total_count_for_year = attendance_tmp_df['cnt'].sum()

            file_path = '../plots/' + db + '/' + user_id_tmp + "_" + thismonthname + yearnum + '.jpg'
            
            savePlot(aggregated_data, total_count_for_year, 'Number of posts by '+ pax + ' by AO/Month for ' + yearnum, file_path)
            
            message = 'Hey ' + pax + "! Here is your monthly posting summary for " + yearnum + "! SYITG!"

            # user_id_tmp = 'U03QFC2S2LX'
            print('PAX posting graph created for user', pax, 'Sending to Slack now... hang tight!')
            
            # The current method v2, and legacy method, can both be invoked here depending on the region_method variable.
            # Most regions still use the legacy method, but will need to migrate to v2 by Spring 2025. 
            # The main difference is that v2 requires an additional conversation scope.
            # New regions will all use v2.
            # user_id_override = "U06GDMGJKNE"
            if rgion_method == "v2":
                try:
                    response = send_slack_message_v2(user_id_tmp, message, file_path)

                    success_message_sent(user_id_tmp, pax, db)
                except Exception as e:
                    # If the error is missing scope, then 
                    if e.response['error'] == 'missing_scope':
                        print("Error: The app is missing required scopes. Please add the 'im:write' scope.")
                        rgion_method = "v1"
                    else:
                        log_message_sent_error(user_id_tmp, db, pax)
                        raise e

            if rgion_method != "v2":
                try:
                    channel = user_id_tmp
                    response = send_slack_message(channel, message, file_path)
                    
                    success_message_sent(user_id_tmp, pax, db)
                except Exception as e:
                    log_message_sent_error(user_id_tmp, db, pax)
                    raise e
            
            return True, rgion_method
        else:
            print(pax + ' skipped')
            return False, region_method


total_graphs = 0
region_method = "v2"
for _, row in users_df.iterrows():
    user_id = row['user_id']
    email = row['email']
    pax = row['user_name']
    try:
        attendance_tmp_df = pd.DataFrame([])  # creates an empty dataframe to append to
        with mydb.cursor() as cursor:
            sql = "SELECT table_schema, region, year as Year, month as Month, ao as AO, email_hash, cnt FROM f3stlcity.user_monthly_aggregates WHERE email_hash=%s AND year = %s"
            user_id_tmp = user_id
            val = (email, yearnum)
            cursor.execute(sql, val)
            attendance_tmp = cursor.fetchall()
            if attendance_tmp :
                graph_or_not, rm = execute_user_chart(attendance_tmp, user_id_tmp, db, pax, d, thismonthname, yearnum, region_method)
                region_method = rm

                if graph_or_not:
                    total_graphs = total_graphs + 1
            else:
                print("No attendance this year", user_id)
    except Exception as e:
            print(e)
            print("An exception occurred for User ID " + user_id)
    finally:
        plt.close('all') #Note - this was added after the December 2020 processing, make sure this works
print('Total graphs made:', total_graphs)
mydb.close()