import math
import random
import time
import urllib.parse
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import os
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

from colorama import Fore
from tqdm import tqdm


class Fidilio:

    def __init__(self):
        self.url = "https://fidilio.com/"
        self.coffeeshops_url = "https://fidilio.com/coffeeshops/"
        self.bas_path_directory = os.getcwd()
        self.output_path_directory = ""
        self.count_of_url_requests = 300
        self.sleep_time_between_requests = 3

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
        if len(df_from_csv) == 0:
            df = df_from_web
        else:
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
        df = pd.DataFrame()
        try:
            df = pd.read_csv(os.path.join(self.output_path_directory, "01_cities.csv"))
            return df
        except Exception as e:
            return df

    def all_actions_for_coffeeshops_info(self):
        # get city data and get list of city urls
        df_city = self.read_city_data_from_csv()
        city_urls_list = df_city["URL"].tolist()

        # get coffeeshops data
        last_page_dict = self.get_coffeeshops_last_page(city_urls_list)

        # create all coffeeshops_info urls
        coffeeshops_info_urls_list = []
        for key, value in last_page_dict.items():
            for i in range(0, value + 1):
                url = key + "/?p=" + str(i)
                coffeeshops_info_urls_list.append(url)
        # get coffeeshops_info data
        df = self.get_coffeeshops_info(coffeeshops_info_urls_list)

    def get_coffeeshops_last_page(self, city_urls_list):
        infos_list = self.request_url(city_urls_list)
        res_dict = {}
        for info in tqdm(infos_list, desc="get_coffeeshops_last_page"):
            link = info[0]
            soup = BeautifulSoup(info[1], "html.parser")
            last_page = self.parse_last_page(inp_soup=soup)
            res_dict[link] = last_page

        return res_dict

    def request_url(self,
                    inp_list: list,
                    status_code_list=[200, ]) -> list:
        result = []
        out = []
        max_count = math.ceil((len(inp_list) * 1.5) / 100)
        if max_count < 3:
            max_count = 3

        def load_url(url, timeout):
            ans = requests.get(url, timeout=timeout, headers=self.headers)
            ans.encoding = ans.apparent_encoding
            return ans

        count = 1
        count_first_inp_list = len(inp_list)
        count_last_inp_list = len(inp_list)
        pbar = tqdm(total=count_first_inp_list, desc="Requesting to urls")
        while (len(inp_list)) and (count <= max_count):
            inp_dict = dict()
            for this_url in inp_list:
                name = this_url.split("/")[-2]
                inp_dict[name] = this_url

            random.shuffle(inp_list)

            req_ls = list()
            if len(inp_list) > self.count_of_url_requests:
                req_ls = inp_list[0:self.count_of_url_requests].copy()
            else:
                req_ls = inp_list.copy()

            with ThreadPoolExecutor(max_workers=len(req_ls)) as executor:
                future_to_url = (executor.submit(load_url, url, 30) for url in req_ls)
                time.sleep(self.sleep_time_between_requests)
                for future in as_completed(future_to_url):
                    try:
                        data = future.result()
                        out.append(data)
                    except Exception as exc:
                        # print(Fore.YELLOW + str(type(exc)))
                        continue

            for item in out:
                if item.status_code in status_code_list:
                    this_name = item.url.split("/")[-3]
                    if this_name in inp_dict:
                        result.append((inp_dict[this_name], item.text))
                        inp_list.remove(inp_dict[this_name])
            count += 1
            pbar.update(count_last_inp_list - len(inp_list))
            count_last_inp_list = len(inp_list)

        # pbar.close()
        if (count > max_count) and (len(inp_list)):
            for link in inp_list:
                print(Fore.YELLOW + "Error: {}".format(link))
                continue
        else:
            pbar.close()
            return result
        pbar.close()
        return result

    @staticmethod
    def parse_last_page(inp_soup):
        last_page = -1
        container_tag = inp_soup.find("div", {"id": "container"})
        pages_tags = container_tag.find_all("a")
        pages_tags.reverse()
        for page_tag in pages_tags:
            if page_tag.getText() == "اخری":
                last_page_url = page_tag.get("href")
                last_page = int(last_page_url.split("p=")[-1].strip())
        return last_page

    def get_coffeeshops_info(self, coffeeshops_info_urls_list) -> pd.DataFrame:
        # coffeeshops_info_urls_list = ["https://fidilio.com/coffeeshops/in/tehran/تهران/?p=1"]
        infos_list = self.request_coffeeshops_info_url(coffeeshops_info_urls_list)
        df = pd.DataFrame()
        for info in tqdm(infos_list, desc="Parsing coffeeshops info"):
            link = info[0]
            print(link)
            soup = BeautifulSoup(info[1], "html.parser")
            df = self.parse_coffeeshops_info(inp_soup=soup)
        return df

    def request_coffeeshops_info_url(self,
                                     inp_list: list,
                                     status_code_list=[200, ]) -> list:
        result = []
        out = []
        max_count = math.ceil((len(inp_list) * 1.5) / 100)
        if max_count < 3:
            max_count = 3

        def load_url(url, timeout):
            ans = requests.get(url, timeout=timeout, headers=self.headers)
            ans.encoding = ans.apparent_encoding
            return ans

        count = 1
        count_first_inp_list = len(inp_list)
        count_last_inp_list = len(inp_list)
        pbar = tqdm(total=count_first_inp_list, desc="Requesting to urls")
        while (len(inp_list)) and (count <= max_count):
            inp_dict = dict()
            for this_url in inp_list:
                name = this_url.split("/")[-3]
                page_number = this_url.split("p=")[-1]
                key = name + "_" + page_number
                inp_dict[key] = this_url

            random.shuffle(inp_list)

            req_ls = list()
            if len(inp_list) > self.count_of_url_requests:
                req_ls = inp_list[0:self.count_of_url_requests].copy()
            else:
                req_ls = inp_list.copy()

            with ThreadPoolExecutor(max_workers=len(req_ls)) as executor:
                future_to_url = (executor.submit(load_url, url, 30) for url in req_ls)
                time.sleep(self.sleep_time_between_requests)
                for future in as_completed(future_to_url):
                    try:
                        data = future.result()
                        out.append(data)
                    except Exception as exc:
                        # print(Fore.YELLOW + str(type(exc)))
                        continue

            for item in out:
                if item.status_code in status_code_list:
                    this_name = item.url.split("/")[-3]
                    this_number = item.url.split("p=")[-1]
                    this_key = this_name + "_" + this_number
                    if this_key in inp_dict:
                        result.append((inp_dict[this_key], item.text))
                        inp_list.remove(inp_dict[this_key])
            count += 1
            pbar.update(count_last_inp_list - len(inp_list))
            count_last_inp_list = len(inp_list)

        # pbar.close()
        if (count > max_count) and (len(inp_list)):
            for link in inp_list:
                print(Fore.YELLOW + "Error: {}".format(link))
                continue
        else:
            return result

        return result

    def parse_coffeeshops_info(self, inp_soup):

        all_base_tags = inp_soup.find_all("div", {"class": "restaurant-list-items span-4 th-span-6 mn-span-12"})
        for this_coffeeshop_tag in all_base_tags:
            # title, url
            title_url_tag = this_coffeeshop_tag.find("a", {"class": "restaurant-link"})
            title = title_url_tag.get("title").strip()
            url = title_url_tag.get("href").strip()
            if (url is not None) or (url != ""):
                url = self.url + url
            else:
                url = ""
            print("title: {}".format(title))
            print("url: {}".format(url))

            # image_link
            image_tag = this_coffeeshop_tag.find("img")
            image_link = image_tag.get("src").strip()
            print("image_link: {}".format(image_link))

            # info_title, address, price_class, rate, followers
            info_tag = this_coffeeshop_tag.find("div", {"class": "info"})

            info_title_tag = info_tag.find("div", {"class": "venue-title"})
            info_title = info_title_tag.getText().strip()
            print("info_title: {}".format(info_title))

            address_tag = info_tag.find("div", {"class": "venue-address"})
            address = address_tag.getText().strip()
            print("address: {}".format(address))

            footer_tag = info_tag.find("div", {"class": "foot"})

            # price_class
            price_class_tag = footer_tag.find("span", {"class": "price-class"})
            active_price_class_tags = price_class_tag.find_all("span", {"class": "active"})
            price_class = len(active_price_class_tags)
            print("price_class: {}".format(price_class))

            # rate
            rate_tag = footer_tag.find("span", {"class": "rate"})
            rate_value_tag = rate_tag.find("div", {"class": "rate-it"})
            rate = rate_value_tag.get("data-rateit-value")
            print("rate: {}".format(rate))

            # followers
            followers_tag = footer_tag.find("span", {"class": "followers"})
            follower = followers_tag.getText().strip()
            print("follower: {}".format(follower))

            print("--------------------------------------------------")
