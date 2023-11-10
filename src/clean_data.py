import pandas as pd

import log_config
from constants import LOGGER_NAME
from directories import GENERAL_DIR

logger = log_config.setup_logger(LOGGER_NAME)


def create_query():
    country_city_df = pd.read_csv(f"{GENERAL_DIR}/country_city_list.csv")
    country_city_df = country_city_df[["Name", "Local Name", "Country"]]
    country_city_df.columns = ["city_name", "city_local_name", "country_name"]
    country_city_df.drop_duplicates(inplace=True)
    country_city_df.dropna(subset=["country_name", "city_name"], inplace=True)
    logger.info(f"Number of countries: {len(country_city_df['country_name'].unique())}")

    countries = []
    for country in country_city_df["country_name"].unique():
        country_df = country_city_df[country_city_df["country_name"] == country].copy()
        cities = country_df["city_name"].apply(str).tolist()

        # local names were tricky
        local_names = []
        for local_names_str in country_df["city_local_name"].dropna():
            local_names.extend(
                [
                    name.strip()
                    for name in str(local_names_str).split(",")
                    if name.strip()
                ]
            )

        combined_query = ", ".join(sorted(set(cities + local_names), key=str))

        country_dict = {"country_name": country, "query": combined_query}
        countries.append(country_dict)

    countries_df = pd.DataFrame(countries)
    countries_df.to_csv(f"{GENERAL_DIR}/country_query.csv", index=False)
    logger.info("Saved countries query to csv file")
