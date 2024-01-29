import unittest

from os import sys, path
sys.path.append('../')

from BD_Update_Utils import determine_db_action, find_match, retrievePreviousBackblasts, DbAction
import pandas as pd
import pymysql.cursors
import configparser
import time

# Optional class to use as a part of testing some functionalities and packages.
# Requires setting up a test config
class TestComparer(unittest.TestCase):
    # Configure AWS credentials
    SECONDS_PER_DAY = 86400
    current_ts = time.time()
    lookback_days = 7
    lookback_seconds = SECONDS_PER_DAY * lookback_days
    cutoff_ts = current_ts - lookback_seconds
    config = configparser.ConfigParser()
    config.read('../config/credentials_test.ini')
    host = config['aws']['host']
    port = int(config['aws']['port'])
    user = config['aws']['user']
    password = config['aws']['password']
    testdb = config['aws']['db']
    #Define AWS Database connection criteria
    mydb = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        db=testdb,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)

    curr_bd = retrievePreviousBackblasts(mydb, cutoff_ts)

    NEW_SLACK_MESSAGE = {
        "timestamp": 1704218963.541859,
        "ts_edited": 1704235588.000000,
        "ao_id": "C04807X50N4"
    }
    

    NEW_SLACK_MESSAGE_EDITED_NONE = {
        "timestamp": 1704218963.541859,
        "ao_id": "C04807X50N4"
    }

    HISTORICAL_UPDATE = {
        "timestamp": 1704218963.541859,
        "ts_edited": 1677222222.000000,
        "ao_id": "C04807X50N4"
    }

    HISTORICAL_MATCH = {
        "timestamp": 1704218963.541859,
        "ts_edited": 1704235588.000000,
        "ao_id": "C04807X50N4"
    }

    def test_determine_db_action_no_historical(self):
        self.assertEqual(determine_db_action(self.NEW_SLACK_MESSAGE, None), DbAction.INSERT, "Should be insert when no historical record is found")
 
    def test_determine_db_action_historical_update(self):
        self.assertEqual(determine_db_action(self.NEW_SLACK_MESSAGE, self.HISTORICAL_UPDATE), DbAction.UPDATE, "Should be update when the new ts_edited is greated")

    def test_determine_db_action_no_ts_edited(self):
        self.assertEqual(determine_db_action(self.NEW_SLACK_MESSAGE_EDITED_NONE, self.HISTORICAL_MATCH), DbAction.IGNORE, "Should be ignore if there is a match found, but ts_edited is null ( meaning it is not greater)")

    def test_determine_db_action_historical_match(self):
        self.assertEqual(determine_db_action(self.NEW_SLACK_MESSAGE, self.HISTORICAL_MATCH), DbAction.IGNORE, "Should be ignore when the ts_edited matches")

    def test_find_match_when_exists(self):
        match = find_match(self.NEW_SLACK_MESSAGE, self.curr_bd)
        self.assertIsNotNone(match, "Match should be returned")

if __name__ == '__main__':
    unittest.main()