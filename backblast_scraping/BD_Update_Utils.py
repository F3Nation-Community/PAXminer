from enum import Enum

# Set of potential database actions to take on a new beatdown record.
DbAction = Enum('DbAction', ['INSERT', 'UPDATE', 'IGNORE', 'DELETE'])
TS_EDITED_NULL_VAL = "NA"

# Takes a beatdown retrieved from a slack message and a list of previous beatdowns.
# Compares timestamp of slack message to return a match if the beatdowns match.
def find_match(new_beatdown, previous_beatdowns):
    first_or_default = next((x for x in previous_beatdowns if ( str(x["timestamp"]) == str(new_beatdown["timestamp"]))), None)
    return first_or_default

# From the database, reads backblasts based on a timestamp filter.
def retrievePreviousBackblasts(dbConn, timestampFilter):
    with dbConn.cursor() as cursor:
        sql12 = f"SELECT ao_id, timestamp, ts_edited FROM beatdowns WHERE timestamp >= {timestampFilter}"
        cursor.execute(sql12)
        return cursor.fetchall()

# Compares a current slack message to a historical database record to determine the database action.
# When their is no historical record, we insert the new beatdown.
# When their is a historical record, but no updated timestamp we ignore.
# If there is a historical record AND the edited timestamp in the new record is larger than the edited timestamp in the old record. We update.
def determine_db_action(new_record, historical_record):
    if historical_record is None:
        return DbAction.INSERT
    elif "ts_edited" not in new_record:
        return DbAction.IGNORE
    elif new_record.get("ts_edited") == TS_EDITED_NULL_VAL:
        return DbAction.IGNORE
    elif new_record.get("ts_edited") != TS_EDITED_NULL_VAL and historical_record["ts_edited"] == TS_EDITED_NULL_VAL:
        return DbAction.UPDATE
    elif new_record.get("ts_edited") and (float(new_record["ts_edited"]) > float(historical_record["ts_edited"])):
        return DbAction.UPDATE
    else:
        return DbAction.IGNORE
    