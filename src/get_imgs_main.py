import os
import log_config
import pandas as pd

from get_imgs import GetImage
from directories import IMG_DIR, GENERAL_DIR
from constants import LOGGER_NAME

logger = log_config.setup_logger(LOGGER_NAME)


def download_images():
    logger.info("Downloading images...")

    countries_df = pd.read_csv(f"{GENERAL_DIR}/country_query.csv")
    countries = countries_df["country_name"].unique()

    for i, country in enumerate(countries):
        logger.info("+--------------------------------------+")
        logger.info(f"{i+1}.{country}")
        logger.info("+--------------------------------------+")
        image_dir = os.path.join(IMG_DIR, country)
        get_image = GetImage(country)
        get_image.download_img_e_update_table(image_dir)


if __name__ == "__main__":
    download_images()
