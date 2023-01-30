from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import os


class Fidilio:

    def __init__(self):
        self.url = "https://fidilio.com/"
        self.coffeeshops_url = "https://fidilio.com/coffeeshops/"
        self.bas_path_directory = os.getcwd()
        self.output_path_directory = ""

        self.headers = {
            "accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-US,en;q=0.9,fa;q=0.8",
            "cache-control": "max-age=0",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"}

        self.create_output_directory()

    def all_actions_for_cities(self):
        # get city data
        df_from_web = self.get_cities_info()

        # read city data from csv
        df_from_csv = self.read_city_data_from_csv()

        # compare data
        df = pd.merge(left=df_from_web,
                      right=df_from_csv,
                      on=["URL", "City", "City_EN", "updated_at"],
                      how="outer")

        df.sort_values(by=["updated_at", "URL"], inplace=True, ascending=False)
        df.drop_duplicates(subset=["URL"], keep="first", inplace=True)
        df.sort_values(by=["URL"], inplace=True, ascending=True)

        # save city data
        self.save_city_data(df)

    def get_cities_info(self):
        response = requests.get(self.coffeeshops_url, headers=self.headers)
        soup = BeautifulSoup(response.text, "html.parser")
        city_bas_tag = soup.find("select", {"id": "cityClass"})
        city_tags = city_bas_tag.find_all("option")
        cities_ls = []
        for city_tag in city_tags:
            if city_tag.get("label") is not None:
                city_data = {"City": city_tag.get("label"), "City_EN": city_tag.get("value")}
                cities_ls.append(city_data)
        df = pd.DataFrame(cities_ls)
        df["URL"] = self.coffeeshops_url + "in/" + df["City_EN"] + "/" + df["City"]
        df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df = df[["URL",
                 "City",
                 "City_EN",
                 "updated_at"]]
        return df

    def create_directory(self, directory_name):
        path = os.path.join(self.bas_path_directory, directory_name)
        if not os.path.exists(path):
            os.mkdir(path)
        return path

    def create_output_directory(self):
        path = self.create_directory("output")
        self.output_path_directory = path

    def save_city_data(self, df):
        df.to_csv(os.path.join(self.output_path_directory, "01_cities.csv"), index=False, encoding="utf-8-sig")

    def read_city_data_from_csv(self):
        df = pd.read_csv(os.path.join(self.output_path_directory, "01_cities.csv"))
        return df



