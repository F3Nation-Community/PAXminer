#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script queries the AWS F3(region) database for attendance records. It then generates leaderboard bar graphs
for each AO for the current month and YTD on total attendance.
The graph then is sent to each AO in a Slack message.
'''

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import time
import pandas as pd
import pymysql.cursors
import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import configparser
import sys
# This handler does retries when HTTP status 429 is returned
from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler


def generate_leaderboards(mydb, slack, region, db):

    #Graph Counter Reset
    total_graphs = 0 # Sets a counter for the total number of graphs made (users with posting data)

    #Get Current Year, Month Number and Name
    d = datetime.datetime.now()
    d = d - datetime.timedelta(days=7)
    thismonth = d.strftime("%m")
    thismonthname = d.strftime("%b")
    thismonthnamelong = d.strftime("%B")
    yearnum = d.strftime("%Y")

    try:
        with mydb.cursor() as cursor:
            sql = "SELECT ao, channel_id FROM aos WHERE backblast = 1 and archived = 0"
            cursor.execute(sql)
            aos = cursor.fetchall()
            aos_df = pd.DataFrame(aos, columns=['ao', 'channel_id'])
    finally:
        print('Now pulling all beatdown records... Stand by...')

    total_graphs = 0 # Sets a counter for the total number of graphs made (users with posting data)

    # Query AWS by for beatdown history
    for index, row in aos_df.iterrows():
        ao = row['ao']
        channel_id = row['channel_id']
        month = []
        day = []
        year = []
        try:
            with mydb.cursor() as cursor:
                sql = "select PAX, count(Date) as Posts\
                from attendance_view \
                where MONTH(Date) = %s \
                AND YEAR(Date) = %s \
                AND ao= %s \
                group by PAX \
                order by count(Date) desc\
                limit 20"
                val = (thismonth, yearnum, ao)
                cursor.execute(sql, val)
                posts = cursor.fetchall()
                posts_df = pd.DataFrame(posts, columns=['PAX', 'Posts'])
        finally:
            print('Now pulling all posting records for', ao, '... Stand by...')

        if not posts_df.empty:
            ax = posts_df.plot.bar(x='PAX', color={"Posts": "orange"})
            plt.title("Monthly Leaderboard - " + thismonthnamelong + ", " + yearnum)
            plt.xlabel("")
            plt.ylabel("# Posts for " + thismonthname + ", " + yearnum)
            plt.savefig('../plots/' + db + '/PAX_Leaderboard_' + ao + thismonthname + yearnum + '.jpg', bbox_inches='tight')  # save the figure to a file
            print('Monthly Leaderboard Graph created for AO', ao, 'Sending to Slack now... hang tight!')
            max_attempts = 5
            for attempt in range(max_attempts):
                try:
                    response = slack.files_upload_v2(channel=channel_id, initial_comment='Hey ' + ao + "! Here are the posting leaderboards for " + thismonthnamelong + ", " + yearnum + " as well as for Year to Date (includes all beatdowns, rucks, Qsource, etc.) with the top 20 posters! T-CLAPS to these HIMs.", file='../plots/' + db + '/PAX_Leaderboard_' + ao + thismonthname + yearnum + '.jpg')
                    total_graphs = total_graphs + 1
                    break #exit the loop if upload is successful
                except SlackApiError as e:
                    if e.response.status_code == 429:
                        delay = int(e.response.headers['Retry-After'])
                        print(f"Rate limited. Retrying in {delay} seconds")
                        time.sleep(delay)
                    else:
                        # other errors
                        raise e

        try:
            with mydb.cursor() as cursor:
                sql = "select PAX, count(Date) as Posts\
                from attendance_view \
                WHERE YEAR(Date) = %s \
                AND ao = %s \
                group by PAX \
                order by count(Date) desc\
                limit 20"
                val = (yearnum, ao)
                cursor.execute(sql, val)
                posts = cursor.fetchall()
                posts_df = pd.DataFrame(posts, columns=['PAX', 'Posts'])
        finally:
            print('Now pulling all posting records for', ao, '... Stand by...')
        if not posts_df.empty:
            ax = posts_df.plot.bar(x='PAX', color={"Posts": "green"})
            plt.title("Year to Date Leaderboard - " + yearnum)
            plt.xlabel("")
            plt.ylabel("# Posts for " + yearnum + " - Year To Date")
            plt.savefig('../plots/' + db + '/PAX_Leaderboard_YTD_' + ao + yearnum + '.jpg', bbox_inches='tight')  # save the figure to a file
            print('YTD Leaderboard Graph created for region', region, 'Sending to Slack now... hang tight!')
            max_attempts = 5
            for attempt in range(max_attempts):
                try:
                    slack.files_upload_v2(file='../plots/' + db + '/PAX_Leaderboard_YTD_' + ao + yearnum + '.jpg', channel=channel_id)
                    total_graphs = total_graphs + 1
                    break # exit the loop if upload is successful
                except SlackApiError as e:
                    if e.response.status_code == 429:
                        delay = int(e.response.headers['Retry-After'])
                        print(f"Rate limited. Retrying in {delay} seconds")
                        time.sleep(delay)
                    else:
                        # other errors
                        raise e
    # After all AOs have been processed, print the total number of graphs made
    print('Total graphs made:', total_graphs)


if __name__ == '__main__':
    # Configure AWS credentials
    config = configparser.ConfigParser()
    config.read('../config/credentials.ini')
    host = config['aws']['host']
    port = int(config['aws']['port'])
    user = config['aws']['user']
    password = config['aws']['password']
    # db = config['aws']['db']
    db = sys.argv[1]
    region = sys.argv[3]
    # region = 'STL'

    # Set Slack token
    key = sys.argv[2]
    slack = WebClient(token=key)
    firstf = sys.argv[4]  # designated 1st-f channel for the region
    # Enable rate limited error retries
    rate_limit_handler = RateLimitErrorRetryHandler(max_retry_count=7)
    slack.retry_handlers.append(rate_limit_handler)

    # Define AWS Database connection criteria
    mydb = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        db=db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)

    generate_leaderboards(mydb, slack, region, firstf, db)