#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script executes the daily PAXminer export to tab delimited files for all F3 regions using PAXminer.
'''

from slacker import Slacker
import pandas as pd
import os

from db_connection_manager import DBConnectionManager

# Set the working directory to the directory of the script
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

db = 'paxminer'
mydb1 = DBConnectionManager('../config/credentials.ini').connect(db)

# Get list of regions and Slack tokens for PAXminer execution
try:
    with mydb1.cursor() as cursor:
        sql = "SELECT * FROM paxminer.regions WHERE active = 1"
        cursor.execute(sql)
        regions = cursor.fetchall()
        regions_df = pd.DataFrame(regions, columns={'region', 'slack_token', 'schema_name'})
finally:
    print('Getting list of regions for export...')

for index, row in regions_df.iterrows():
    region = row['region']
    key = row['slack_token']
    db = row['schema_name']
    print('Exporting data for region ' + region)
    os.system("./DelimFileWriter.py " + db + " " + key)
    print('----------------- End of Region Export -----------------\n')