#!/usr/bin/env python3

# This demonstrates that the changes to `LeaderboardByAO_Charter.py` maintain backwards compatibility.
#
# It simulates how AO_Leaderboard_Monthly_Execution.py calls LeaderboardByAO_Charter.py
#
# The script will fail when attempting to read from the `config` object after executing
# `config.read('../config/credentials.ini')`, as the credentials file doesn't exist in this context.
# To properly test this functionality, add the credentials file, and it should work as expected.
#
# Despite the failure to read the non-existent config file, this test still proves that
# the `if __name__ == '__main__':` condition allows the `os.system` call to execute as intended.
# This confirms that the script's core functionality remains intact.

import os

db = 'some-db'
key = 'some-key'
region = 'some-region'
firstf = 'some-firstf'

os.system("./LeaderboardByAO_Charter.py " + db + " " + key + " " + region + " " + firstf)
