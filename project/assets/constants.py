import os
RAW_DATA_PATH = "dagster_project/data/raw"


API_TEST_FILES  = os.path.join("data", "raw", "test_final.json")


DBT_DIRECTORY = os.path.abspath(os.path.join(__file__, "..", "..", "..", "analytics"))

FILE_PATH_SAVE = os.path.abspath(os.path.join(__file__, "..", "..", "..", "output","output.png"))
