import os
import requests
import logging
import pandas as pd
from PIL import Image
from io import BytesIO

from tqdm import tqdm
from psycopg2 import sql
from db_config import Database
from constants import LOGGER_NAME
from directories import TMP_DIR

logger = logging.getLogger(LOGGER_NAME)


class GetImage:
    def __init__(self, country: str):
        self.country = country
        self.db = Database()

    def get_country_table(self):
        query = sql.SQL("SELECT id, image_url FROM public.{}").format(
            sql.Identifier(self.country)
        )
        self.db.cursor.execute(query)
        return self.db.cursor.fetchall()

    def download_img_e_update_table(self, img_dir):
        if not os.path.exists(img_dir):
            os.makedirs(img_dir)

        country_table = self.get_country_table()
        country_table = [
            (img_id, img_url)
            for img_id, img_url in country_table
            if img_url is not None
        ]

        image_names = []
        rate_limited_requests = []

        for img_id, img_url in tqdm(country_table):
            if os.path.exists(os.path.join(img_dir, f"{img_id}.jpg")):
                logger.info(f"Skipping {img_id}: Image already downloaded")
                image_names.append({"id": img_id, "image_name": f"{img_id}.jpg"})
                continue

            file_name, is_downloaded = self.download_image(img_url, img_id, img_dir)
            if is_downloaded:
                image_names.append({"id": img_id, "image_name": file_name})
            elif is_downloaded is None:
                rate_limited_requests.append({"id": img_id, "image_url": img_url})
            else:
                logger.info(f"Failed to download image for {img_id}")

        image_names_df = pd.DataFrame(image_names)
        self.db.update_data(
            schema_name="public",
            table_name=self.country,
            chunk=image_names_df,
            key_column="id",
        )

        # Save the rate limited requests as a CSV file
        if rate_limited_requests:
            rate_limited_df = pd.DataFrame(rate_limited_requests)
            rate_limited_csv_path = os.path.join(
                TMP_DIR, f"rate_limited_{self.country}.csv"
            )
            rate_limited_df.to_csv(rate_limited_csv_path, index=False)
            logger.info(f"Rate limited requests saved to {rate_limited_csv_path}")

    def download_image(self, img_url, img_id, img_dir):
        try:
            if any(
                img_url.lower().endswith(ext)
                for ext in [".jpg", ".jpeg", ".png", ".gif"]
            ):
                response = requests.get(img_url, timeout=10)
                response.raise_for_status()

                # convert image to JPEG
                img = Image.open(BytesIO(response.content))
                file_path = os.path.join(img_dir, f"{img_id}.jpg")
                img.convert("RGB").save(file_path, "JPEG")
                return f"{img_id}.jpg", True

        except requests.HTTPError as http_err:
            if response.status_code == 429:
                logger.error(f"Rate limit hit for {img_id}: {http_err}")
                return None, None  # None, None signifies a 429 status code
            logger.error(f"HTTP error occurred for {img_id}: {http_err}")
        except Exception as err:
            logger.error(f"Other error occurred for {img_id}: {err}")
        return None, False
