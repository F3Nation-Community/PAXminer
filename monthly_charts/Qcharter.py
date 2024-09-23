#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script queries the AWS F3(region) database for all beatdown records. It then generates bar graphs
on Q's for each AO and sends it to the AO channel in a Slack message.
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
#db = config['aws']['db']
db = sys.argv[1]
region = sys.argv[3]

# Set Slack token
key = sys.argv[2]
slack = WebClient(token=key)
firstf = sys.argv[4] #designated 1st-f channel for the region

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
# for ao in aos_df['ao']:
for index, row in aos_df.iterrows():
    ao = row['ao']
    channel_id = row['channel_id']    
    month = []
    day = []
    year = []
    with mydb.cursor() as cursor:
        sql = """
        select
            `B`.`bd_date` AS `Date`,
            `a`.`ao` AS `AO`,
            `U1`.`user_name` AS `Q`,
            `U1`.`app` AS `Q_Is_App`,
            `U2`.`user_name` AS `CoQ`,
            `B`.`pax_count` AS `pax_count`,
            `B`.`fngs` AS `fngs`,
            `B`.`fng_count` AS `fng_count`
        from
            (((`beatdowns` `B`
        left join `users` `U1` on
            ((`U1`.`user_id` = `B`.`q_user_id`)))
        left join `users` `U2` on
            ((`U2`.`user_id` = `B`.`coq_user_id`)))
        left join `aos` `a` on
            ((`a`.`channel_id` = `B`.`ao_id`)))
        WHERE `a`.`ao` = %s AND YEAR(`bd_date`) = %s AND MONTH(`bd_date`) = %s and `U1`.`app` != 1
        order by
            `B`.`bd_date`,
            `a`.`ao`
        """
        
        val = (ao, yearnum, thismonth)
        cursor.execute(sql, val)
        bd_tmp = cursor.fetchall()
        bd_tmp_df = pd.DataFrame(bd_tmp)
        if not bd_tmp_df.empty:
            for Date in bd_tmp_df['Date']:
                datee = datetime.datetime.strptime(str(Date), "%Y-%m-%d")
                month.append(datee.strftime("%B"))
                day.append(datee.day)
                year.append(datee.year)
            bd_tmp_df['Month'] = month
            bd_tmp_df['Day'] = day
            bd_tmp_df['Year'] = year
            month_order = ["January", "February", "March", "April", "May", "June", "July", "August", "September",
                           "October", "November", "December"]
            try:
                melted_df = pd.melt(bd_tmp_df, id_vars=['Month'], value_vars=['Q', 'CoQ'], var_name='Role', value_name='TempQ')
                melted_df = melted_df.dropna()
                # Rename 'TempQ' to 'Q'
                melted_df = melted_df.rename(columns={'TempQ': 'Q'})
                melted_df.groupby(['Q', 'Month']).size().unstack().sort_values(['Q'], ascending=True).plot(kind='bar')
                plt.title('Number of Qs by individual at ' + ao + ' for ' + thismonthnamelong + ', ' + yearnum)
                #plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), frameon=False)
                plt.legend('')
                plt.ioff()
                #ax = bd_tmp_df.plot.bar(x='Q', color={"ao": "orange"})
                #plt.title("Q Counts - " + ao + " " + thismonthnamelong + ", " + yearnum)
                #plt.xlabel("")
                #plt.ylabel("# Q Counts for " + thismonthname + ", " + yearnum)
                plt.savefig('../plots/' + db + '/Q_Counts_' + ao + "_" + thismonthname + yearnum + '.jpg', bbox_inches='tight')  # save the figure to a file
                print('Q Graph created for AO', ao, 'Sending to Slack now... hang tight!')
                # ao_override = "C07FFAG02LS"
                slack.files_upload_v2(channel=channel_id, initial_comment='Hey ' + ao + '! Here is a look at who has been stepping up to Q at this AO. Is your name on this list? Remember Core Principle #4 - F3 is peer led in a rotating fashion. Exercise your leadership muscles. Sign up to Q!', file='../plots/' + db + '/Q_Counts_' + ao + "_" + thismonthname + yearnum + '.jpg', title="Test upload")
                total_graphs = total_graphs + 1
                plt.close()
            except Exception as e:
                print(e)
                print('An Error Occurred in Sending')
            finally:
                print('Message Sent')
print('Total AO graphs made:', total_graphs)

try:
    total_graphs = 0
    month = []
    day = []
    year = []
    with mydb.cursor() as cursor:
        sql = """
        select
            `B`.`bd_date` AS `Date`,
            `a`.`ao` AS `AO`,
            `U1`.`user_name` AS `Q`,
            `U1`.`app` AS `Q_Is_App`,
            `U2`.`user_name` AS `CoQ`,
            `B`.`pax_count` AS `pax_count`,
            `B`.`fngs` AS `fngs`,
            `B`.`fng_count` AS `fng_count`
        from
            (((`beatdowns` `B`
        left join `users` `U1` on
            ((`U1`.`user_id` = `B`.`q_user_id`)))
        left join `users` `U2` on
            ((`U2`.`user_id` = `B`.`coq_user_id`)))
        left join `aos` `a` on
            ((`a`.`channel_id` = `B`.`ao_id`)))
        WHERE YEAR(`bd_date`) = %s AND MONTH(`bd_date`) = %s and `U1`.`app` != 1
        order by
            `B`.`bd_date`,
            `a`.`ao`
        """
        val = (yearnum, thismonth)
        cursor.execute(sql, val)
        bd_tmp2 = cursor.fetchall()
        bd_tmp_df2 = pd.DataFrame(bd_tmp2)
        if not bd_tmp_df2.empty:
            for Date in bd_tmp_df2['Date']:
                datee = datetime.datetime.strptime(str(Date), "%Y-%m-%d")
                month.append(datee.strftime("%B"))
                day.append(datee.day)
                year.append(datee.year)
            bd_tmp_df2['Month'] = month
            bd_tmp_df2['Day'] = day
            bd_tmp_df2['Year'] = year
            melted_df = pd.melt(bd_tmp_df2, id_vars=['AO'], value_vars=['Q', 'CoQ'], var_name='Role', value_name='TempQ')
            melted_df = melted_df.dropna()
            
            # Rename 'TempQ' to 'Q'
            melted_df = melted_df.rename(columns={'TempQ': 'Q'})
            melted_df.groupby(['Q', 'AO']).size().unstack().plot(kind='bar', stacked = True, figsize=(25,4))
            #bd_tmp_df2.groupby(['Q'],['AO']).sum().size().plot(kind='bar', stacked=True, sort_columns=False, figsize=(8,4))
            plt.title('Number of Qs by individual across all AOs for ' + thismonthnamelong + ', ' + yearnum)
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), frameon=False)
            plt.ioff()
            plt.savefig('../plots/' + db + '/Q_Counts_' + db + "_" + thismonthname + yearnum + '.jpg',
                        bbox_inches='tight')  # save the figure to a file
            print('Q Graph created for ', region, 'Sending to Slack now... hang tight!')
            # firstf_override = "C07FFAG02LS"
            slack.conversations_join(channel=firstf)
            slack.files_upload_v2(channel=firstf, initial_comment='Hey ' + region + '! Here is a look at who has been stepping up to Q across all AOs for the month. Is your name on this list? Remember Core Principle #4 - F3 is peer led in a rotating fashion. Exercise your leadership muscles. Sign up to Q!', file='../plots/' + db + '/Q_Counts_' + db + "_" + thismonthname + yearnum + '.jpg')
            total_graphs = total_graphs + 1
finally:
    print('Total Q summary graphs made:', total_graphs)