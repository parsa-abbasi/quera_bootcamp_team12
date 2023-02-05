import json
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
import warnings


class Fidilio:

    def __init__(self):
        warnings.filterwarnings("ignore")
        self.url = "https://fidilio.com/"
        self.api_url = "https://fidilio.com/api/map/GetSearchData"
        self.base_property_url = "https://fidilio.com/restaurants/"
        self.base_user_url = "https://fidilio.com/u/"
        self.bas_path_directory = os.getcwd()
        self.output_path_directory = ""
        self.count_of_url_requests = 100
        self.sleep_time_between_requests = 10

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }
        self.create_output_directory()

    def create_directory(self, directory_name):
        path = os.path.join(self.bas_path_directory, directory_name)
        if not os.path.exists(path):
            os.mkdir(path)
        return path

    def create_output_directory(self):
        path = self.create_directory("Files\\output")
        self.output_path_directory = path

    def all_actions_for_get_data_info(self):
        # get lat and lang of cities
        df_lat_lang = pd.read_csv(self.bas_path_directory + "\\Files\\iran_lat_lang.csv")
        df_lat_lang = df_lat_lang[["lat", "lng"]].copy()

        # create data for call api
        df_lat_lang["api_data"] = "Radius=15000&SortBy=1&latitude=" + df_lat_lang["lat"].astype("str") + "&longitude=" + \
                                  df_lat_lang["lng"].astype("str") + \
                                  "&PageSize=1000&PageNumber=0&firstObjectLatEnabled=true&query=&isSpecialList=false"

        # save lat and lang
        df_lat_lang.to_csv(self.output_path_directory + "\\lat_lang.csv", index=False, encoding="utf-8-sig")

        # create list of api_data
        list_api_data = df_lat_lang["api_data"].tolist()

        # get data_info
        df_data_info_web = self.get_data_info(list_api_data)

        # data cleaning
        df_data_info_web.dropna(subset=["results_id"], inplace=True)
        df_data_info_web.drop_duplicates(subset=["results_id"], inplace=True, keep="first")

        # create url for each property and update date
        df_data_info_web["url"] = self.base_property_url + df_data_info_web["results_slug"].astype("str")
        df_data_info_web["update_date"] = str(datetime.now())

        # get previous data in csv file
        try:
            df_data_info_csv = pd.read_csv(self.output_path_directory + "\\data_info.csv")
        except Exception as e:
            df_data_info_csv = pd.DataFrame()

        # concat data
        df_data_info = pd.concat([df_data_info_web, df_data_info_csv], ignore_index=True)
        df_data_info.sort_values(by=["update_date"], inplace=True)
        df_data_info.drop_duplicates(subset=["results_id"], inplace=True, keep="last")

        # save data
        df_data_info.to_csv(self.output_path_directory + "\\data_info.csv", index=False, encoding="utf-8-sig")

    def get_data_info(self, list_api_data) -> pd.DataFrame:
        res_df = pd.DataFrame()
        infos_list = self.request_api(list_api_data)
        for info in tqdm(infos_list, desc="parse data info"):
            link = info[0]
            df = pd.json_normalize(info[1], record_path="results",
                                   meta=["totalResults", "page", "totalPages"],
                                   record_prefix="results_")
            df["data"] = link
            res_df = pd.concat([res_df, df], ignore_index=True)

        return res_df

    def request_api(self,
                    inp_list: list,
                    status_code_list=[200, ]) -> list:
        result = []
        out = []
        max_count = math.ceil((len(inp_list) * 1.5) / self.count_of_url_requests)
        if max_count < 8:
            max_count = 8

        def load_url(inp_data, timeout):
            ans = requests.post(self.api_url, timeout=timeout, headers=self.headers, data=inp_data)
            ans.encoding = ans.apparent_encoding
            return ans

        count = 1
        count_first_inp_list = len(inp_list)
        count_last_inp_list = len(inp_list)
        pbar = tqdm(total=count_first_inp_list, desc="Requesting to urls")
        while (len(inp_list)) and (count <= max_count):

            random.shuffle(inp_list)

            req_ls = list()
            if len(inp_list) > self.count_of_url_requests:
                req_ls = inp_list[0:self.count_of_url_requests].copy()
            else:
                req_ls = inp_list.copy()

            with ThreadPoolExecutor(max_workers=len(req_ls)) as executor:
                future_to_url = (executor.submit(load_url, data, 30) for data in req_ls)
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
                    if item.request.body in inp_list:
                        result.append((item.request.body, item.json()))
                        inp_list.remove(item.request.body)
            count += 1
            pbar.update(count_last_inp_list - len(inp_list))
            count_last_inp_list = len(inp_list)

        if (count > max_count) and (len(inp_list)):
            for link in inp_list:
                print(Fore.YELLOW + "Error: {}".format(link))
                continue
        else:
            pbar.close()
            return result
        pbar.close()
        return result

    def all_actions_for_get_data_details(self):
        # get data info
        df_data_info = pd.read_csv(self.output_path_directory + "\\data_info.csv")

        # create list of url
        list_url = df_data_info["url"].tolist()
        list_url = list(set(list_url))

        # get data details
        df_data_details = self.get_data_details(list_url)

        # concat data
        try:
            df_data_details_on_csv = pd.read_csv(self.output_path_directory + "\\data_details.csv")
        except Exception as e:
            df_data_details_on_csv = pd.DataFrame()

        df_data_details = pd.concat([df_data_details, df_data_details_on_csv], ignore_index=True)

        # clean data
        df_data_details.sort_values(by=["update_date"], inplace=True)
        df_data_details.drop_duplicates(subset=["url"], inplace=True, keep="last")
        df_data_details.reset_index(drop=True, inplace=True)
        print(df_data_details)

        # save data
        df_data_details.to_csv(self.output_path_directory + "\\data_details.csv", index=False, encoding="utf-8-sig")
        df_data_details.to_json(self.output_path_directory + "\\data_details.json", orient="records", force_ascii=False,
                                index=True)

    def get_data_details(self, list_url) -> pd.DataFrame:
        res_df = pd.DataFrame()
        # list_url = list_url[:1500].copy()
        # https://fidilio.com/restaurants/barcoo1
        # list_url = ["https://fidilio.com/restaurants/borito"]
        # list_url = ["https://fidilio.com/restaurants/barcoo1"]
        infos_list = self.request_url(list_url)
        for info in tqdm(infos_list, desc="parse data details"):
            link = info[0]
            soup = BeautifulSoup(info[1], "html.parser")
            df = self.parse_data_details(soup)
            df["url"] = link
            df["update_date"] = str(datetime.now())
            df = df[[
                'url',
                'name',
                'price_rate',
                'badges',
                'followers',
                'menu',
                'social',
                'features',
                'description',
                'reviews_count',
                'reviews_rate',
                'reviews',
                'information',
                'users_url',
                'popular_menu_api',
                "update_date"]].copy()
            res_df = pd.concat([res_df, df])

        return res_df

    def request_url(self,
                    inp_list: list,
                    status_code_list=[200, ]) -> list:
        result = []
        out = []
        max_count = math.ceil((len(inp_list) * 1.5) / self.count_of_url_requests)
        if max_count < 8:
            max_count = 8

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
                name = this_url.split("/")[-1]
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
                    try:
                        this_name = item.url.split("/")[4]
                        if this_name == "500?aspxerrorpath=":
                            continue
                    except Exception as exc:
                        continue
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
            return result

        return result

    def parse_data_details(self, inp_soup) -> pd.DataFrame:
        res_df = pd.DataFrame()

        # rate and price_rate
        rate = -1
        price_rate = -1
        try:
            rate_price_base_tag = inp_soup.find("div", {"class": "venue-rate-box"})

            # rate
            try:
                rate_base_tag = rate_price_base_tag.find("div", {"class": "rate"})
                rate = rate_base_tag.getText().strip()
            except:
                rate = -1

            # price_rate
            try:
                price_rate_base_tag = rate_price_base_tag.find("div", {"class": "price-class"})
                price_rate_tags = price_rate_base_tag.find_all("span", {"class": "active"})
                price_rate = len(price_rate_tags)
            except:
                price_rate = -1
        except:
            total_rate = -1
            total_price_rate = -1
        # print("rate: {}".format(rate))
        # print("price_rate: {}".format(price_rate))

        # name & badge
        name = ""
        badges = []
        try:
            name_badge_base_tag = inp_soup.find("div", {"class": "venue-name-box"})

            # name
            try:
                name_base_tag = name_badge_base_tag.find("h1", {"property": "name"})
                name = name_base_tag.getText().strip()
            except:
                name = ""

            # badges
            try:
                badge_base_tag = name_badge_base_tag.find("span", {"class": "badge-container"})
                badge_tags = badge_base_tag.find_all("span", {"class": "badge"})
                badges = []
                for badge_tag in badge_tags:
                    badges.append(badge_tag.getText().strip())
            except:
                badge_base_tags = []

        except:
            name = ""
            badges = []
        badges = json.dumps(badges, ensure_ascii=False)
        # print("name: {}".format(name))
        # print("badges: {}".format(badges))

        # followers
        followers = ""
        try:
            followers_base_tag = inp_soup.find("span", {"class": "followers-box"})
            followers = followers_base_tag.find("span", {"class": "follow-count"}).getText().strip()
        except:
            followers = ""
        # print("followers: {}".format(followers))

        # menu
        menu = {}
        try:
            menu_base_tag = inp_soup.find("input", {"name": "menuObject"})
            menu = menu_base_tag["value"]
            menu = json.loads(menu)
        except:
            menu = {}
        menu = json.dumps(menu, ensure_ascii=False)
        # print("menu: {}".format(menu))

        # social
        social = {}
        try:
            social_base_tag = inp_soup.find("span", {"class": "social pr-label"})
            social_tags = social_base_tag.find_all("a")
            for social_tag in social_tags:
                url = social_tag["href"]
                source = social_tag.getText().strip()
                social[source] = url
        except:
            social = {}
        social = json.dumps(social, ensure_ascii=False)
        # print("social: {}".format(social))

        # features
        features = []
        try:
            features_base_tag = inp_soup.find("div", {"class": "venue-features-box"})
            features_tags = features_base_tag.find_all("span", {"class": "feature-title"})
            for feature_tag in features_tags:
                features.append(feature_tag.getText().strip())
        except:
            features = []
        features = json.dumps(features, ensure_ascii=False)
        # print("features: {}".format(features))

        # reviews and review_rate and review_count
        reviews_rate = {}
        reviews = {}
        reviews_count = ""
        users_url = []
        try:
            review_bas_tag = inp_soup.find("div", {"class": "panel-container reviews-highlight"})

            # reviews_count
            try:
                reviews_count_base_tag = review_bas_tag.find("hgroup")
                reviews_count_tag = reviews_count_base_tag.find("span", {"class": "reviews-count"})
                reviews_count = reviews_count_tag.getText().strip()
            except:
                reviews_count = ""

            # reviews_rate
            try:
                rate_dict = {}
                rate_overview = {}
                rate_overall = {}

                reviews_rate_base_tag = review_bas_tag.find("div", {"class": "rates-container"})

                # rate dict
                try:
                    rate_list_base_tag = reviews_rate_base_tag.find("ul", {"class": "rates-list"})
                    rate_list_tags = rate_list_base_tag.find_all("li")
                    for rate_list_tag in rate_list_tags:
                        this_type = rate_list_tag.find("span").getText().strip()
                        this_rate = rate_list_tag.find("div", {"class": "rate-it"}).get("data-rateit-value")
                        rate_dict[this_type] = this_rate
                except:
                    rate_dict = {}

                # rate_overview
                try:
                    rate_overview_base_tag = reviews_rate_base_tag.find("div", {"class": "rate-overview-container"})
                    rate_overview_tags = rate_overview_base_tag.find_all("div", {"class": "overview"})
                    for rate_overview_tag in rate_overview_tags:
                        key_value = rate_overview_tag.find_all("span")
                        if len(key_value) == 2:
                            value = key_value[0].getText().strip()
                            key = key_value[1].getText().strip()
                            rate_overview[key] = value
                        else:
                            continue
                except:
                    rate_overview = {}

                # rate_overall
                try:
                    rate_overall = {}
                    rate_overall_base_tag = reviews_rate_base_tag.find("div", {"class": "rate-overal"})
                    this_type = rate_overall_base_tag.find("span", {"class": "red"}).getText().strip()
                    value_tag = rate_overall_base_tag.find("div", {"class": "rate-it"})
                    value = value_tag.get("data-rateit-value")
                    rate_overall[this_type] = value
                except:
                    rate_overall = {}
            except:
                rate_dict = {}
                rate_overview = {}
                rate_overall = {}

            reviews_rate["rate_dict"] = rate_dict
            reviews_rate["rate_overview"] = rate_overview
            reviews_rate["rate_overall"] = rate_overall

            # reviews
            try:
                reviews_base_tag = review_bas_tag.find("div", {"class": "panel-body"})

                # user_reviews
                try:
                    user_reviews_base_tag = reviews_base_tag.find("div", {"class": "tab review-paginate"})
                    script_tags = user_reviews_base_tag.find_all("script")
                    div_tags = user_reviews_base_tag.find_all("div", {"class": "review-box clear-both"})
                    for script_tag, div_tag in zip(script_tags, div_tags):
                        this_json = script_tag.getText().strip()
                        this_json = json.loads(this_json)
                        this_user_url = self.base_user_url + str(this_json["author"]) + "/"
                        users_url.append(this_user_url)

                        idd = div_tag.get("id")

                        # comment-body
                        comment_body_base_tag = div_tag.find("div", {"class": "comment-body"})

                        # comment-head
                        comment_head_base_tag = comment_body_base_tag.find("div", {"class": "comment-head"})
                        user_url = comment_head_base_tag.find("a").get("href")
                        user_name = comment_head_base_tag.find("span", {"itemprop": "author"}).getText().strip()
                        user_grade = comment_head_base_tag.find("div", {"class": "user-grade"}).getText().strip()
                        user_date = comment_head_base_tag.find("div", {"class": "review-date"}).getText().strip()

                        # review-details
                        reviews_details = {}
                        review_details_base_tag = div_tag.find("div", {"class": "review-details"})
                        review_details_tags = review_details_base_tag.find_all("span", {"class": "rate"})
                        for review_details_tag in review_details_tags:
                            title = review_details_tag.find("span", {"class": "rate-title"}).getText().strip()
                            value = review_details_tag.find("div", {"class": "rate-it"}).get(
                                "data-rateit-value").strip()
                            reviews_details[title] = value

                        # comment-foot
                        comment_foot_base_tag = div_tag.find("div", {"class": "comment-foot"})

                        comment_foot_rate_tag = comment_foot_base_tag.find("div", {"class": "ratings"})
                        comment_foot_rate = comment_foot_rate_tag.find("div", {"class": "rate-it"}).get(
                            "data-rateit-value")
                        comment_foot_face_review_tag = comment_foot_base_tag.find("span", {"class": "face-icon"})
                        if comment_foot_face_review_tag is not None:
                            comment_foot_face_review = comment_foot_face_review_tag.get("class")[1]
                        else:
                            comment_foot_face_review = ""

                        # like_count
                        like_count = 0
                        like_count_tag = comment_foot_base_tag.find("a",
                                                                    {"class": "like like-button review-like-button"})
                        like_count = like_count_tag.find("span").getText().strip()
                        like_count = int(like_count)

                        review_dict = {}
                        review_dict["user_url"] = user_url
                        review_dict["json"] = this_json
                        review_dict["user_name"] = user_name
                        review_dict["user_grade"] = user_grade
                        review_dict["user_date"] = user_date
                        review_dict["reviews_details"] = reviews_details
                        review_dict["comment_foot_rate"] = comment_foot_rate
                        review_dict["comment_foot_face_review"] = comment_foot_face_review
                        review_dict["like_count"] = like_count
                        reviews[idd] = review_dict

                except:
                    reviews = {}

            except:
                reviews = {}

        except:
            reviews_count = ""
            reviews_rate = {}
            reviews = {}
        reviews_rate = json.dumps(reviews_rate, ensure_ascii=False)

        reviews = json.dumps(reviews, ensure_ascii=False)
        # print("reviews_count: {}".format(reviews_count))
        # print("reviews_rate: {}".format(reviews_rate))
        # print("reviews: {}".format(reviews))

        # information
        information = {}
        try:
            information_base_tag = inp_soup.find("ul", {"class": "infolist"})
            information_tags = information_base_tag.find_all("li")
            for i, information_tag in enumerate(information_tags):
                try:
                    key = information_tag.find("span").get("property").strip()
                except:
                    key = str(i)
                try:
                    value = information_tag.find("span").getText().strip()
                except:
                    value = ""
                information[key] = value
        except:
            information = {}

        information = json.dumps(information, ensure_ascii=False)
        # print("information: {}".format(information))

        # popular foods
        popular_menu_api = ""
        try:
            popular_foods_base_tag = inp_soup.find("section", {"class": "venue-sidebar"})
            popular_menu_tag = popular_foods_base_tag.find("div", {"class": "menu-panel"})
            popular_menu_api = self.url + popular_menu_tag.get("data-url")
        except:
            popular_menu_api = "[]"

        # print("popular_menu_api: {}".format(popular_menu_api))

        res_dict = {}
        res_dict["price_rate"] = price_rate
        res_dict["name"] = name
        res_dict["badges"] = badges
        res_dict["followers"] = followers
        res_dict["menu"] = menu
        res_dict["social"] = social
        res_dict["features"] = features
        description = ""
        res_dict["description"] = description
        res_dict["reviews_count"] = reviews_count
        res_dict["reviews_rate"] = reviews_rate
        res_dict["reviews"] = reviews
        res_dict["information"] = information
        res_dict["users_url"] = json.dumps(list(set(users_url)), ensure_ascii=False)
        res_dict["popular_menu_api"] = popular_menu_api
        res_df = pd.DataFrame(res_dict, index=[0])
        return res_df
