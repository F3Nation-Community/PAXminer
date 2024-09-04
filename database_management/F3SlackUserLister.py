#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script queries Slack for all PAX Users and inserts User IDs/names into the AWS database for recordkeeping.
Updates existing user records if changes have been made. Uses parameterized inputs for multiple region updates.

Usage: F3SlackUserLister.py [db_name] [slack_token]

'''

import pandas as pd
import pymysql.cursors
from slack_sdk import WebClient
import time
import os
import logging


def user_lookback():
        SECONDS_PER_DAY = 86400
        LOOKBACK_DAYS = 7
        LOOKBACK_SECONDS = SECONDS_PER_DAY * LOOKBACK_DAYS
        current_ts = time.time()
        cutoff_ts = current_ts - LOOKBACK_SECONDS
        return cutoff_ts


def database_slack_user_update(region_db, key):
    logging.basicConfig(format=f'%(asctime)s [{region_db}] %(levelname)-8s %(message)s',
                            datefmt = '%Y-%m-%d %H:%M:%S',
                            level = logging.INFO)
    logging.info("Database_slack_user_update")

    host = os.environ['host']
    port = 3306
    user = os.environ['user']
    password = os.environ['password']

    # Set Slack token
    slack = WebClient(token=key)

    #Define AWS Database connection criteria
    mydb = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        db=region_db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)

    logging.info('Looking for any new or updated F3 Slack Users. Stand by...')

    # Make users Data Frame
    data = ''
    while True:
        cutoff_ts = user_lookback()
        users_response = slack.users_list(limit=1000, cursor=data)
        response_metadata = users_response.get('response_metadata', {})
        next_cursor = response_metadata.get('next_cursor')
        users = users_response.data['members']
        users_df = pd.json_normalize(users)
        users_df = users_df[['id', 'profile.display_name', 'profile.real_name', 'profile.phone', 'profile.email', 'is_bot', 'updated']]
        users_df = users_df.rename(columns={'id' : 'user_id', 'profile.display_name' : 'user_name', 'profile.real_name' : 'real_name', 'profile.phone' : 'phone', 'profile.email' : 'email', 'is_bot': 'app'})
        # Update any null user_names with the real_name values
        users_df['email'].fillna("None", inplace=True)

        # Now connect to the AWS database and insert some rows!
        try:
            with mydb.cursor() as cursor:
                for index, row in users_df[users_df['updated'] > cutoff_ts].iterrows():
                    sql = "INSERT INTO users (user_id, user_name, real_name, phone, email, app) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE user_name=%s, real_name=%s, phone=%s, email=%s, app=%s"
                    user_id_tmp = row['user_id']
                    user_name_tmp = row['user_name']
                    real_name_tmp = row['real_name']

                    if(user_name_tmp == ""):
                        user_name_tmp = real_name_tmp
                        
                    phone_tmp = row['phone']
                    email_tmp = row['email']
                    app_tmp = row['app']
                    val = (user_id_tmp, user_name_tmp, real_name_tmp, phone_tmp, email_tmp, app_tmp, user_name_tmp, real_name_tmp, phone_tmp, email_tmp, app_tmp)
                    cursor.execute(sql, val)
                    mydb.commit()
                    result = cursor.rowcount
                    if result == 1:
                        logging.info("Record inserted for user: " + user_name_tmp)
                    #     try:
                    #         slack.chat_postMessage(channel='paxminer_logs', text=" - New PAX record created for " + user_name_tmp)
                    #     except:
                    #         pass
                    elif result == 2:
                        logging.info("Record updated for user: " + user_name_tmp)
                    #     try:
                    #         slack.chat_postMessage(channel='paxminer_logs', text=" - PAX record updated for " + user_name_tmp)
                    #     except:
                    #         pass

        finally:
            pass
        if next_cursor:
            # Keep going from next offset.
            #logging.info('next_cursor =' + next_cursor)
            data = next_cursor
        else:
            #logging.info('End of Loop')# All done!
            mydb.close()
            break
    logging.info('Finished - users are up to date.')