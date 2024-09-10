#!/Users/schaecher/.pyenv/versions/3.7.3/bin/python3.7
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script queries the AWS F3(region) database for all beatdown records. It then generates bar graphs
on Q's for each AO and sends it to the AO channel in a Slack message.
'''

from slacker import Slacker
import pandas as pd
import configparser
import datetime
import matplotlib

from db_connection_manager import DBConnectionManager

matplotlib.use('Agg')
import matplotlib.pyplot as plt

# Configure Slack credentials
config_file_name = '/Users/schaecher/PycharmProjects/config/credentials.ini'
config = configparser.ConfigParser()
config.read(config_file_name)

mydb = DBConnectionManager(config_file_name).connect()

# Set Slack tokens
key = config['slack']['prod_key']
slack = Slacker(key)


try:
    with mydb.cursor() as cursor:
        sql = "SELECT ao FROM aos WHERE backblast = 1"
        cursor.execute(sql)
        aos = cursor.fetchall()
        aos_df = pd.DataFrame(aos, columns={'ao'})
finally:
    print('Now pulling all beatdown records... Stand by...')

total_graphs = 0 # Sets a counter for the total number of graphs made (users with posting data)

# Query AWS by for beatdown history
try:
    for ao in aos_df['ao']:
        month = []
        day = []
        year = []
        with mydb.cursor() as cursor:
            sql = "SELECT * FROM beatdown_info WHERE ao = %s AND MONTH(Date) IN (10, 11) ORDER BY Date"
            val = (ao)
            cursor.execute(sql, val)
            bd_tmp = cursor.fetchall()
            bd_tmp_df = pd.DataFrame(bd_tmp, columns={'Date', 'AO', 'Q', 'CoQ', 'pax_count', 'fngs', 'fng_count'})
            if not bd_tmp_df.empty:
                for Date in bd_tmp_df['Date']:
                    datee = datetime.datetime.strptime(str(Date), "%Y-%m-%d")
                    month.append(datee.strftime("%B"))
                    day.append(datee.day)
                    year.append(datee.year)
                bd_tmp_df['Month'] = month
                bd_tmp_df['Day'] = day
                bd_tmp_df['Year'] = year
                #bd_tmp_df = bd_tmp_df[bd_tmp_df.Month == 'November'] # Keep only October data until I can better figure out how to facet the charts by month
                bd_tmp_df.groupby(['fng_count', 'Month']).size().unstack().plot(kind='bar')
                plt.title('Number of FNGs at ' + ao + ' by Month')
                plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), frameon=False)
                plt.ioff()
                plt.savefig('/Users/schaecher/PycharmProjects/PAXminer/plots/FNG_Counts_' + ao + '.jpg', bbox_inches='tight')  # save the figure to a file
                print('Graph created for AO', ao, 'Sending to Slack now... hang tight!')
            #slack.chat.post_message(ao, 'Hello ' + ao + '! Here is a look at who has Qd at this AO by month. Is your name on this list?')
            #slack.files.upload('/Users/schaecher/PycharmProjects/PAXminer/plots/Q_Counts_' + ao + '.jpg', channels=ao)
            total_graphs = total_graphs + 1
finally:
    print('Total graphs made:', total_graphs)