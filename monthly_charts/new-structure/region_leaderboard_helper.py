from slack_sdk import WebClient
from ao_leaderboard_helper import create_directory
import pandas as pd
import pymysql.cursors
import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import logging
import os
# This handler does retries when HTTP status 429 is returned
from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler

def region_leaderboard_charts():
    host = os.environ['host']
    port = 3306
    user = os.environ['user']
    password = os.environ['password']
    schema_name = "paxminer"

    #Define AWS Database connection criteria
    mydb1 = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        db=schema_name,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)

    # Get list of regions and Slack tokens for PAXminer execution
    try:
        logging.info('Getting list of regions that use PAXminer...')
        with mydb1.cursor() as cursor:
            sql = "SELECT * FROM paxminer.regions where firstf_channel IS NOT NULL AND send_region_leaderboard = 1"
            cursor.execute(sql)
            regions = cursor.fetchall()
            regions_df = pd.DataFrame(regions)
    finally:
        cursor.close()
    
    count = 0
    for index, row in regions_df.iterrows():
        region = row['region']
        key = row['slack_token']
        db = row['schema_name']
        firstf = row['firstf_channel']

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
        # if count < 5:
        region_leaderboard_run(region, region_db, key, firstf, row['schema_name'])
        logging.info('----------------- End of Region Update -----------------\n')

def region_leaderboard_run(region_name, region_db, key, firstf, schema_name):
    total_graphs = 0
    slack = WebClient(token=key)
    # Enable rate limited error retries
    rate_limit_handler = RateLimitErrorRetryHandler(max_retry_count=5)
    slack.retry_handlers.append(rate_limit_handler)

    base_directory = create_directory(schema_name, 'region_charts')

    #Get Current Year, Month Number and Name
    d = datetime.datetime.now()
    d = d - datetime.timedelta(days=7)
    thismonth = d.strftime("%m")
    thismonthname = d.strftime("%b")
    thismonthnamelong = d.strftime("%B")
    yearnum = d.strftime("%Y")

    try:
        print(f'Now pulling all posting records for {region_name}... Stand by...')
        with region_db.cursor() as cursor:
            sql = """
            select PAX, count(distinct AO) as UniqueAOs, count(1) as Posts FROM (
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
            where MONTH(Date) = %s
            AND YEAR(Date) = %s
            group by PAX
            order by count(1) desc
            limit 20
            """
            val = (thismonth, yearnum)
            cursor.execute(sql, val)
            posts = cursor.fetchall()
            posts_df = pd.DataFrame(posts, columns=['PAX', 'UniqueAOs', 'Posts'])
    except Exception as e: 
        logging.error(e)

    if not posts_df.empty:
        print(f'Sending Region Leaderboard Graph for {region_name} ... Stand by...')
        ax = posts_df.plot.bar(x='PAX', color={'UniqueAOs' : "blue", "Posts" : "orange"})
        plt.title("Monthly Leaderboard - " + thismonthnamelong + ", " + yearnum)
        plt.xlabel("")
        plt.ylabel("# Posts for " + thismonthname + ", " + yearnum)
        plt.savefig(f'{base_directory}/PAX_Leaderboard_' + region_name + thismonthname + yearnum + '.jpg', bbox_inches='tight')  # save the figure to a file
        print('Monthly Leaderboard Graph created for region_name ', region_name, 'Sending to Slack now... hang tight!')
        plt.close()
        # slack.files_upload_v2(channel=firstf, initial_comment='Hey ' + region_name + "! Check out the current posting leaderboards for " + thismonthnamelong + ", " + yearnum + " as well as for Year to Date (includes all beatdowns, rucks, Qsource, etc.). Here are the top 20 posters! T-CLAPS to these HIMs.", file=f'{base_directory}/{schema_name}/region_charts/PAX_Leaderboard_' + region_name + thismonthname + yearnum + '.jpg', )
        total_graphs = total_graphs + 1
    print(f'Total graphs made: {total_graphs}')

    try:
        with region_db.cursor() as cursor:
            sql = """
            select PAX, count(distinct AO) as UniqueAOs, count(1) as Posts FROM (
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
            where YEAR(Date) = %s
            group by PAX
            order by count(1) desc
            limit 20
            """
            val = (yearnum)
            cursor.execute(sql, val)
            posts = cursor.fetchall()
            posts_df = pd.DataFrame(posts, columns=['PAX', 'UniqueAOs', 'Posts'])
    finally:
        print(f'Now pulling all posting records for {region_name}... Stand by...')

    if not posts_df.empty:
        ax = posts_df.plot.bar(x='PAX', color={'UniqueAOs' : "purple", "Posts" : "green"})
        plt.title("Year to Date Leaderboard - " + yearnum)
        plt.xlabel("")
        plt.ylabel("# Posts for " + yearnum + " - Year To Date")
        plt.savefig(f'{base_directory}/region_charts/PAX_Leaderboard_YTD_' + region_name + yearnum + '.jpg', bbox_inches='tight')  # save the figure to a file
        print('YTD Leaderboard Graph created for region_name', region_name, 'Sending to Slack now... hang tight!')
        plt.close()
        # slack.files_upload_v2(file=f'{base_directory}/region_charts/PAX_Leaderboard_YTD_' + region_name + yearnum + '.jpg', channel=firstf)
        total_graphs = total_graphs + 1
    logging.info(f'Total graphs made: {total_graphs}')