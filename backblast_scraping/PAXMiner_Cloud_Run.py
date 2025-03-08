#!/usr/bin/env python3
import pandas as pd
import os
import sys
from PAX_BD_Miner import run_pax_bd_miner, create_database_connection

# Set RegEx range for which regions will be queried. Command line input parameter 1 should be a regex range (e.g. A-M) which will search for all regions starting with A through M.
region_regex = sys.argv[1]

paxminer_db = None

# Get list of regions and Slack tokens for PAXminer execution
try:
    host = os.environ['host']
    port = 3306
    user = os.environ['user']
    password = os.environ['password']
    db = "paxminer"
    #Define AWS Database connection criteria
    paxminer_db = create_database_connection(host, port, user, password, db)

    with paxminer_db.cursor() as cursor:
        sql = "SELECT * from paxminer.regions WHERE active = 1 AND scrape_backblasts = 1"
        cursor.execute(sql)
        regions = cursor.fetchall()
        regions_df = pd.DataFrame(regions, columns=['region', 'slack_token', 'schema_name'])
finally:
    print('Getting list of regions that use PAXminer...')
    if paxminer_db:
        paxminer_db.close()
    
for index, row in regions_df.iterrows():
    region = row['region']
    key = row['slack_token']
    db = row['schema_name']
    print(f'Executing user updates for region {region}')

    try:
        run_pax_bd_miner(host, port, user, password, db, key)
    except Exception as e:
        print(f'Error in PAXminer execution for region {region}')
        print(e)
    finally:
        print(f'-------- PAXMiner Coud Run Complete {region}-------------')