import os
import pandas as pd

from directories import IMG_DIR, GENERAL_DIR


def count_img_per_country():
    countries = os.listdir(IMG_DIR)
    country_img_count = []
    for country in countries:
        country_dir = os.path.join(IMG_DIR, country)
        imgs = os.listdir(country_dir)
        country_img_count.append({"country": country, "img_count": len(imgs)})

    country_img_count_df = pd.DataFrame(country_img_count)
    country_img_count_df = country_img_count_df.sort_values(
        by=["img_count"], ascending=False
    )
    country_img_count_df.to_csv(f"{GENERAL_DIR}/country_img_count.csv", index=False)


if __name__ == "__main__":
    count_img_per_country()
