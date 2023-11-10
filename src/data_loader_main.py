import pandas as pd
import log_config
import data_loader

from tqdm import tqdm
from clean_data import create_query
from constants import LOGGER_NAME
from directories import DATA_DIR, GENERAL_DIR

logger = log_config.setup_logger(LOGGER_NAME)


def get_data():
    try:
        countries_df = pd.read_csv(f"{GENERAL_DIR}/country_query.csv")
        logger.info("Loaded countries query from csv file")
    except FileNotFoundError:
        logger.info("Couldn't find countries query csv file")
        logger.info("Creating countries query csv file...")
        create_query()
        countries_df = pd.read_csv(f"{GENERAL_DIR}/country_query.csv")
        logger.info("Loaded countries query from csv file")

    for country in tqdm(countries_df["country_name"].unique()):
        country_df = countries_df[countries_df["country_name"] == country]
        query = country_df["query"].tolist()[0]
        data_loader.RedditDumpLoader(DATA_DIR, country, query).get_submissions()


if __name__ == "__main__":
    get_data()
