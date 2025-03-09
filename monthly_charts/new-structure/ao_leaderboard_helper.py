import pandas as pd
import pymysql.cursors
import os
import logging

import pandas as pd
import time
import pymysql.cursors
import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler
# This handler does retries when HTTP status 429 is returned
# from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler

def create_directory(db, subdirectory):
    # print(('hostname'))
    if os.environ.get('cloud_run') :
        base_directory = f'/mnt/images_volume/plots/{db}'
    else :
        base_directory = f'../plots/{db}'

    # List of subdirectories to ensure exist
    subdirectories = [base_directory, f'{base_directory}/{subdirectory}']

    # Create all necessary directories
    for directory in subdirectories:
        os.makedirs(directory, exist_ok=True)
    
    return base_directory

def region_run(mydb, db, slack_key, send_charts=False):
    slack = WebClient(token=slack_key)
    rate_limit_handler = RateLimitErrorRetryHandler(max_retry_count=7)
    slack.retry_handlers.append(rate_limit_handler)
    
    base_directory = create_directory(db, 'ao_charts')

    #Graph Counter Reset
    total_graphs = 0 # Sets a counter for the total number of graphs made (users with posting data)

    #Get Current Year, Month Number and Name
    d = datetime.datetime.now()
    d = d - datetime.timedelta(days=9)
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
        logging.info('Now pulling all beatdown records... Stand by...')

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
                sql = """
                select PAX, count(1) as Posts FROM (
                    select
                        `bd`.`date` AS `Date`,
                        `ao`.`ao` AS `AO`,
                        `u`.`user_name` AS `PAX`
                    from
                        (((`bd_attendance` `bd`
                    left join `aos` `ao` on
                        ((`bd`.`ao_id` = `ao`.`channel_id`)))
                    left join `users` `u` on
                        ((`bd`.`user_id` = `u`.`user_id`))))
                    where `u`.app != 1
                    order by
                        `bd`.`date` desc,
                        `ao`.`ao`
                ) a
                where 
                MONTH(Date) = %s
                AND YEAR(Date) = %s
                AND ao= %s
                group by PAX
                order by count(1) desc
                limit 20
                """
                val = (thismonth, yearnum, ao)
                cursor.execute(sql, val)
                posts = cursor.fetchall()
                posts_df = pd.DataFrame(posts, columns=['PAX', 'Posts'])
        finally:
            logging.info(f'Now pulling all posting records for {ao} ... Stand by...')

        if not posts_df.empty:
            ax = posts_df.plot.bar(x='PAX', color={"Posts": "orange"})
            plt.title("Monthly Leaderboard - " + thismonthnamelong + ", " + yearnum)
            plt.xlabel("")
            plt.ylabel("# Posts for " + thismonthname + ", " + yearnum)
            plt.savefig(f'{base_directory}/ao_charts/PAX_Leaderboard_{ao}{thismonthname}{yearnum}.jpg', bbox_inches='tight')
            plt.close()
            logging.info(f'Monthly Leaderboard Graph created for AO {ao} Sending to Slack now... hang tight!')
            
            if send_charts:
                max_attempts = 5
                for attempt in range(max_attempts):
                    try:
                        response = slack.files_upload_v2(channel=channel_id, initial_comment='Hey ' + ao + "! Here are the posting leaderboards for " + thismonthnamelong + ", " + yearnum + " as well as for Year to Date with the top 20 posters! T-CLAPS to these HIMs.", file=f'{base_directory}/PAX_Leaderboard_{ao}{thismonthname}{yearnum}.jpg')
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
                sql = sql = """
                select PAX, count(1) as Posts FROM (
                    select
                        `bd`.`date` AS `Date`,
                        `ao`.`ao` AS `AO`,
                        `u`.`user_name` AS `PAX`
                    from
                        (((`bd_attendance` `bd`
                    left join `aos` `ao` on
                        ((`bd`.`ao_id` = `ao`.`channel_id`)))
                    left join `users` `u` on
                        ((`bd`.`user_id` = `u`.`user_id`))))
                    where `u`.app != 1
                    order by
                        `bd`.`date` desc,
                        `ao`.`ao`
                ) a
                where 
                YEAR(Date) = %s
                AND ao= %s
                group by PAX
                order by count(1) desc
                limit 20
                """
                val = (yearnum, ao)
                cursor.execute(sql, val)
                posts = cursor.fetchall()
                posts_df = pd.DataFrame(posts, columns=['PAX', 'Posts'])
        finally:
            logging.info(f'Now pulling all posting records for {ao} Stand by...')
        if not posts_df.empty:
            ax = posts_df.plot.bar(x='PAX', color={"Posts": "green"})
            plt.title("Year to Date Leaderboard - " + yearnum)
            plt.xlabel("")
            plt.ylabel("# Posts for " + yearnum + " - Year To Date")
            plt.savefig(f'{base_directory}/ao_charts/PAX_Leaderboard_YTD_{ao}{yearnum}.jpg', bbox_inches='tight')  # save the figure to a file
            plt.close()
            logging.info(f'YTD Leaderboard Graph created for region {db} ... Sending to Slack now... hang tight!')
            if send_charts:
                max_attempts = 5
                for attempt in range(max_attempts):
                    try:
                        slack.files_upload_v2(file=f'{base_directory}/PAX_Leaderboard_YTD_{ao}{yearnum}.jpg', channel=channel_id)
                        total_graphs = total_graphs + 1
                        break # exit the loop if upload is successful
                    except SlackApiError as e:
                        if e.response.status_code == 429:
                            delay = int(e.response.headers['Retry-After'])
                            logging.info(f"Rate limited. Retrying in {delay} seconds")
                            time.sleep(delay)
                        else:
                            # other errors
                            raise e
        # After all AOs have been processed, logging.info the total number of graphs made
        logging.info(f'Total graphs made: {total_graphs}')


def ao_leaderboard_charts():
    logging.basicConfig(format=f'%(asctime)s %(levelname)-8s %(message)s',
                            datefmt = '%Y-%m-%d %H:%M:%S',
                            level = logging.INFO)
    
    host = os.environ['host']
    port = 3306
    user = os.environ['user']
    user = os.environ.get('custom_region')
    password = os.environ['password']
    db = "paxminer"

    #Define AWS Database connection criteria
    mydb1 = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        db=db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)

    # Get list of regions and Slack tokens for PAXminer execution
    try:
        with mydb1.cursor() as cursor:
            sql = "SELECT * FROM paxminer.regions where firstf_channel IS NOT NULL AND send_ao_leaderboard = 1"
            cursor.execute(sql)
            regions = cursor.fetchall()
            regions_df = pd.DataFrame(regions)
    finally:
        logging.info('Getting list of regions that use PAXminer...')
        cursor.close()
    
    count = 0
    for index, row in regions_df.iterrows():
        region = row['region']
        key = row['slack_token']
        db = row['schema_name']

            #Define AWS Database connection criteria
        region_db = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

        logging.info(f'Processing statistics for region {region}')
        count = count + 1
        if count < 5:
            region_run(region_db, db, key, False)
        logging.info('----------------- End of Region Update -----------------\n')