import os
import re
import pandas as pd
import logging
from datetime import datetime
import zstandard
import json

from db_config import Database
from constants import LOGGER_NAME, CHUNK_SIZE, DB_COLUMNS

logger = logging.getLogger(LOGGER_NAME)


class RedditDumpLoader:
    def __init__(self, file_path: str, country: str, query: str):
        self.file_path = file_path
        self.country = country
        # query sample: "Rome, Roma, Milan, Milano, Naples, Napoli, Turin, Turino, Palermo"
        self.query = [q.lower().strip() for q in query.split(",")]
        self.file_names = [
            file for file in os.listdir(self.file_path) if file.endswith(".zst")
        ]

        self.db = Database()
        self.db.create_table(
            schema_name="public", table_name=self.country, columns=DB_COLUMNS
        )

        # log the setting
        logger.info(50 * "=")
        logger.info(f"Country: {self.country}")
        logger.info(f"Query: {self.query}")
        logger.info(f"Found {len(self.file_names)} files in {self.file_path}")
        logger.info(50 * "*")

    def get_submissions(self):
        fields = ["title", "selftext"]

        for file_name in self.file_names:
            total_submissions = 0
            matched_submissions = 0
            matched = False
            submission_to_insert = []
            logger.info(
                f"Searching for {self.query} in {fields} in {file_name.split('.')[0]}"
            )
            for line, _ in self.load_data(os.path.join(self.file_path, file_name)):
                total_submissions += 1

                try:
                    matched_query = None
                    obj = json.loads(line)
                    created = datetime.fromtimestamp(int(obj["created_utc"])).date()

                    if not obj.get("id") or not obj.get("subreddit"):
                        continue

                    for field in fields:
                        for q in self.query:
                            if re.search(
                                r"\b" + re.escape(q) + r"\b",
                                obj.get(field, ""),
                                re.IGNORECASE,
                            ):
                                matched = True
                                matched_submissions += 1
                                matched_query = q
                                break
                        if matched:
                            break

                    if matched:
                        matched = False
                        score = int(obj.get("score", 0))
                        num_comments = int(obj.get("num_comments", 0))

                        submission_to_insert.append(
                            {
                                "id": obj["id"],
                                "created": created,
                                "author": obj.get("author", ""),
                                "title": obj.get("title", ""),
                                "body": obj.get("selftext", ""),
                                "score": score,
                                "num_comments": num_comments,
                                "subreddit": obj["subreddit"],
                                "subreddit_id": obj["subreddit_id"],
                                "image_url": obj.get("url", ""),
                                "image_name": None,
                                "matched_query": matched_query,
                            }
                        )

                        if len(submission_to_insert) >= CHUNK_SIZE:
                            self.insert_data_chunk_to_db(submission_to_insert)
                            submission_to_insert = []
                except Exception as e:
                    logger.error(f"Error processing submission: {e}")
                    continue

            if len(submission_to_insert) > 0:
                self.insert_data_chunk_to_db(submission_to_insert)

            logger.info(f"Found {matched_submissions} matched submissions")
            logger.info(f"Processed {total_submissions} submissions in total")
            logger.info(50 * "*")

    def insert_data_chunk_to_db(self, data):
        """Insert a chunk of submissions to the database."""
        df = pd.DataFrame(data)

        try:
            schema_name = "public"
            table_name = self.country
            self.db.insert_data(
                schema_name=schema_name, table_name=table_name, chunk=df
            )
        except Exception as e:
            logger.error(f"Error inserting submissions chunk to database: {e}")

    # Borrowed from https://github.com/Watchful1/PushshiftDumps
    def read_and_decode(
        self, reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0
    ):
        chunk = reader.read(chunk_size)
        bytes_read += chunk_size
        if previous_chunk is not None:
            chunk = previous_chunk + chunk
        try:
            return chunk.decode()
        except UnicodeDecodeError:
            if bytes_read > max_window_size:
                raise UnicodeError(
                    f"Unable to decode frame after reading {bytes_read:,} bytes"
                )
            logger.info(
                f"Decoding error with {bytes_read:,} bytes, reading another chunk"
            )
            return self.read_and_decode(
                reader, chunk_size, max_window_size, chunk, bytes_read
            )

    def load_data(self, file_name):
        with open(file_name, "rb") as file_handle:
            buffer = ""
            reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(
                file_handle
            )
            while True:
                chunk = self.read_and_decode(reader, 2**27, (2**29) * 2)

                if not chunk:
                    break
                lines = (buffer + chunk).split("\n")

                for line in lines[:-1]:
                    yield line.strip(), file_handle.tell()

                buffer = lines[-1]

            reader.close()
