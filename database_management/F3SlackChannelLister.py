#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script queries Slack for Channels and inserts channel IDs/names into the AWS database for recordkeeping.
The Channels data table is used by PAXminer to query only AO channels for backblasts. Uses parameterized inputs for
multiple region updates.

Usage: F3SlackChannelLister.py [db_name] [slack_token]
'''

import pandas as pd
from slack_sdk import WebClient
import logging

def database_slack_channel_update(region_db, key, mydb):
    logging.info("Database_slack_user_update")

    # Configure AWS credentials
    slack = WebClient(token=key)
    # Get channel list
    channels_response = slack.conversations_list(limit=999)
    channels = channels_response.data['channels']
    channels_df = pd.json_normalize(channels)
    channels_df = channels_df[['id', 'name', 'created', 'is_archived']]
    channels_df = channels_df.rename(columns={'id' : 'channel_id', 'name' : 'ao', 'created' : 'channel_created', 'is_archived' : 'archived'})

    # Now connect to the AWS database and insert some rows!
    logging.info('Updating Slack channel list / AOs for region...' + region_db)
    try:
        with mydb.cursor() as cursor:
            for index, row in channels_df.iterrows():
                sql = "INSERT INTO aos (ao, channel_id, channel_created, archived) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE ao=%s, archived=%s"
                channel_name_tmp = row['ao']
                channel_id_tmp = row['channel_id']
                channel_created_tmp = row['channel_created']
                archived_tmp = row['archived']
                val = (channel_name_tmp, channel_id_tmp, channel_created_tmp, archived_tmp, channel_name_tmp, archived_tmp)
        
                cursor.execute(sql, val)
                mydb.commit()
                if cursor.rowcount == 1:
                    logging.info(channel_name_tmp +  "record inserted.")
                    try:
                        slack.chat_postMessage(channel='paxminer_logs', text=" - Slack channel created for " + channel_name_tmp)
                    except:
                        pass
                elif cursor.rowcount == 2:
                    logging.info(channel_name_tmp + " record updated.")
                    try:
                        slack.chat_postMessage(channel='paxminer_logs', text=" - Slack channel updated for " + channel_name_tmp)
                    except:
                        pass
        with mydb.cursor() as cursor3:
            sql3 = "UPDATE aos SET backblast = 0 where backblast IS NULL"
            cursor3.execute(sql3)
            mydb.commit()
            
    finally:
        mydb.close()