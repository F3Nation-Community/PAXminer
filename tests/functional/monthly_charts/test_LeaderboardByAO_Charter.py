import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import cv2
from skimage.metrics import structural_similarity

from monthly_charts.LeaderboardByAO_Charter import generate_leaderboards
from tests.functional.sqlite_adapter import SQLiteAdapter

DB_NAME = 'leaderboard_by_ao_charter'
GOLDEN_IMAGES_PATH = Path(__file__).parent.parent / f"plots/golden_images/{DB_NAME}"
NEW_IMAGES_PATH = Path(__file__).parent.parent / f"plots/{DB_NAME}"

GOLDEN_IMAGE_PREFIXES = [
    "PAX_Leaderboard_AO1",
    "PAX_Leaderboard_AO2",
    "PAX_Leaderboard_YTD_AO1",
    "PAX_Leaderboard_YTD_AO2"
]


@pytest.fixture()
def setup_test_data(cursor, db_connection):
    # WARNING: If you change the test data you must also update the pre-generated golden images.

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS aos (
        ao TEXT PRIMARY KEY,
        channel_id TEXT,
        backblast INTEGER,
        archived INTEGER
    )
    """)

    cursor.execute("""
    INSERT INTO aos (ao, channel_id, backblast, archived) VALUES 
    ('AO1', 'channel1', 1, 0),
    ('AO2', 'channel2', 1, 0),
    ('AO3', 'channel3', 1, 0)
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS attendance_view (
        PAX TEXT,
        Date TEXT,
        ao TEXT
    )
    """)

    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().month
    cursor.execute(f"""
    INSERT INTO attendance_view (PAX, Date, ao) VALUES 
    ('PAX1', '{current_year}-{current_month:02d}-01', 'AO1'),
    ('PAX1', '{current_year}-{current_month:02d}-02', 'AO1'),
    ('PAX2', '{current_year}-{current_month:02d}-01', 'AO1'),
    ('PAX3', '{current_year}-{current_month:02d}-01', 'AO1'),
    ('PAX1', '{current_year}-{current_month:02d}-01', 'AO2'),
    ('PAX2', '{current_year}-{current_month:02d}-01', 'AO2'),
    ('PAX1', '{current_year}-01-01', 'AO1'),
    ('PAX2', '{current_year}-02-01', 'AO1'),
    ('PAX3', '{current_year}-03-01', 'AO1'),
    ('PAX1', '{current_year}-04-01', 'AO2'),
    ('PAX2', '{current_year}-05-01', 'AO2')
    """)

    db_connection.commit()


def test_generate_leaderboards(db_connection, setup_test_data):
    # given
    delete_test_images(NEW_IMAGES_PATH)

    db_adapter = SQLiteAdapter(db_connection)

    # Mock the Slack WebClient to prevent actual Slack messages
    mock_slack = MagicMock()

    # when
    generate_leaderboards(db_adapter, mock_slack, 'Geneva', DB_NAME)

    # then
    assert mock_slack.files_upload_v2.call_count == 4  # 2 graphs for each of the 2 active AOs

    for prefix in GOLDEN_IMAGE_PREFIXES:

        # Find the golden image for this prefix
        golden_image_path = next(GOLDEN_IMAGES_PATH.glob(f"{prefix}*.jpg"))
        assert golden_image_path, f"No golden image found for prefix {prefix}"

        # Find the corresponding new image
        new_image_path = next(NEW_IMAGES_PATH.glob(f"{prefix}*.jpg"))
        assert new_image_path, f"No new image found for prefix {prefix}"

        # Compare images
        golden_image = cv2.imread(golden_image_path, cv2.IMREAD_GRAYSCALE)
        new_image = cv2.imread(new_image_path, cv2.IMREAD_GRAYSCALE)

        assert golden_image.shape == new_image.shape, "Images must have the same dimensions"

        (similarity_score, _) = structural_similarity(golden_image, new_image, full=True)

        assert similarity_score > 0.95, f"Generated image does not match golden image. Similarity Score: {similarity_score}"


def delete_test_images(directory):
    for file in directory.glob('PAX*'):
        try:
            file.unlink()
        except Exception as e:
            print(f"Failed to delete {file}: {e}")
