import pandas as pd

from F3SlackUserLister import database_slack_user_update
from F3SlackChannelLister import database_slack_channel_update
import logging

from db_connection_manager import DBConnectionManager


def database_management_update():
    logging.basicConfig(format=f'%(asctime)s %(levelname)-8s %(message)s',
                            datefmt = '%Y-%m-%d %H:%M:%S',
                            level = logging.INFO)

    db_connection_manager = DBConnectionManager()
    db = "paxminer"
    mydb1 = db_connection_manager.connect(db)

    # Get list of regions and Slack tokens for PAXminer execution
    try:
        with mydb1.cursor() as cursor:
            sql = "SELECT * FROM paxminer.regions where active = 1"
            cursor.execute(sql)
            regions = cursor.fetchall()
            regions_df = pd.DataFrame(regions, columns={'region', 'slack_token', 'schema_name'})
    finally:
        logging.info('Getting list of regions that use PAXminer...')

    for index, row in regions_df.iterrows():
        region = row['region']
        key = row['slack_token']
        region_db = row['schema_name']
        
        logging.info('Executing user updates for region ' + region)
        db_connection = db_connection_manager.connect(region_db)
        try :
            database_slack_user_update(region_db, key, False, db_connection)
        except Exception as e:
            logging.error("An error occured updating the users for region " + region_db)
            logging.error(e)

        try :
            database_slack_channel_update(region_db, key, db_connection)
        except Exception as e:
            logging.error("An error occured updating the channels for region " + region_db)
            logging.error(e)
        
        logging.info('----------------- End of Region Update -----------------\n')