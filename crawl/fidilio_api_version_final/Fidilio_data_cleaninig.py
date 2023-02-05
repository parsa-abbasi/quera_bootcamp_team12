import pandas as pd
import numpy as np
import os
import json
from fidilio_database_handling import FidilioDatabaseHandling
import warnings
import re
from datetime import datetime, timedelta, date, time as dt_time
from unidecode import unidecode


class FidilioDataCleaner:

    def __init__(self):
        warnings.filterwarnings("ignore")
        self.bas_path_directory = os.getcwd()
        self.output_directory = os.path.join(self.bas_path_directory, 'Files\\output\\')
        self.data_info = self.get_data("data_info")
        self.data_details = self.get_data("data_details")
        self.total_data = pd.merge(left=self.data_info,
                                   right=self.data_details,
                                   left_on='url',
                                   right_on='url',
                                   how='inner',
                                   suffixes=('_info', '_details'))
        self.initial_cleaning()
        self.total_data.to_json(self.output_directory + "total_data.json", orient="records")

        # database handle
        self.db_handle = FidilioDatabaseHandling(schema="fidilio")

        # DataFrames for all tables
        self.temp_df = pd.DataFrame()
        self.df_type = pd.DataFrame()
        self.df_main = pd.DataFrame()

        # print(self.total_data)
        # print(self.total_data.columns)

    def get_data(self, name):
        data = pd.read_csv(self.output_directory + name + ".csv")
        return data

    def initial_cleaning(self):
        self.total_data.drop_duplicates(subset=['results_id'], inplace=True)
        self.total_data.dropna(subset=['results_id'], inplace=True)
        self.total_data.sort_values(by=['update_date_details'], inplace=True)
        self.total_data['update_date_details'] = pd.to_datetime(self.total_data['update_date_details'])
        self.total_data['update_date_info'] = pd.to_datetime(self.total_data['update_date_info'])
        self.total_data['update_date_details'].fillna(value=self.total_data['update_date_details'].max(), inplace=True)
        self.total_data['update_date_info'].fillna(value=self.total_data['update_date_info'].max(), inplace=True)
        self.total_data = self.total_data[['results_id',
                                           'results_name',
                                           'results_slug',
                                           'url',
                                           'results_image',
                                           'results_types',
                                           'results_styles',
                                           'results_city',
                                           'results_latitude',
                                           'results_longitude',
                                           'results_address',
                                           'results_rating',
                                           'results_priceClass',
                                           'update_date_info',
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
                                           'update_date_details']]

    def get_df_type_crawl_file(self):
        res_df = pd.DataFrame()
        type_set = set()
        for index, row in self.total_data.iterrows():
            res = json.loads(row['results_types'])
            for item in res:
                type_set.add(item)

        res_df = pd.DataFrame(list(type_set), columns=['id'])
        res_df["Title"] = ""
        res_df["Title_EN"] = ""

        res_df.loc[res_df['id'] == 1, 'Title'] = "رستوران"
        res_df.loc[res_df['id'] == 1, 'Title_EN'] = "Restaurant"
        res_df.loc[res_df['id'] == 2, 'Title'] = "کافی شاپ"
        res_df.loc[res_df['id'] == 2, 'Title_EN'] = "Cafe Shop"
        res_df.loc[res_df['id'] == 4, 'Title'] = "قنادی"
        res_df.loc[res_df['id'] == 4, 'Title_EN'] = "confectionary"
        res_df.loc[res_df['id'] == 8, 'Title'] = "آبمیوه و بستنی"
        res_df.loc[res_df['id'] == 8, 'Title_EN'] = "ice cream shop"

        return res_df

    def get_df_main_crawl_file(self):
        self.temp_df = pd.DataFrame()

        def create_main_columns(row):
            res_dict = {}
            idd = row['results_id']
            res_dict['id'] = idd

            # try:
            name = row['results_name']
            res_dict['Name'] = name
            # except:
            #     print(row)

            name_en = row['results_slug']
            res_dict['Name_En'] = name_en

            url = row['url']
            res_dict['Url'] = url

            image = row['results_image']
            res_dict['Image'] = image

            city = row['results_city']
            res_dict['City'] = city

            latitude = row['results_latitude']
            res_dict['Latitude'] = float(latitude)

            longitude = row['results_longitude']
            res_dict['Longitude'] = float(longitude)

            address = row['results_address']
            res_dict['Address'] = address

            rating = row['results_rating']
            res_dict['Rating_Overall'] = float(rating)

            price_class = row['results_priceClass']
            res_dict['Price_Class'] = float(price_class)

            followers = row['followers']
            if type(followers) == str:
                followers = followers.replace('دنبال کننده', '').strip()
            res_dict['Followers'] = float(followers)

            social = row['social']
            if type(social) == str:
                social = json.loads(social)
                if len(social) > 0:
                    social = social["اینستاگرام"]
                else:
                    social = np.nan
            res_dict['Social'] = social

            description = row['description']
            if type(description) == str:
                description = description.strip()
            else:
                description = ""
            res_dict['Description'] = description

            reviews_count = row['reviews_count']
            if type(reviews_count) == str:
                reviews_count = reviews_count.replace('تعداد نظرات:', '').strip()
            else:
                reviews_count = np.nan
            res_dict['survey_Quality_Total_Count'] = float(reviews_count)

            reviews_rate = row['reviews_rate']
            survey_details_food_quality = np.nan
            survey_details_service_quality = np.nan
            survey_details_price_quality = np.nan
            survey_details_environment_quality = np.nan
            survey_rate_very_bad_count = np.nan
            survey_rate_bad_count = np.nan
            survey_rate_average_count = np.nan
            survey_rate_good_count = np.nan
            survey_rate_very_good_count = np.nan
            if type(reviews_rate) == str:
                reviews_rate = json.loads(reviews_rate)
                if len(reviews_rate) > 0:
                    if "rate_dict" in reviews_rate:
                        if "کیفیت غذا" in reviews_rate["rate_dict"]:
                            survey_details_food_quality = float(reviews_rate["rate_dict"]["کیفیت غذا"].strip())
                        else:
                            survey_details_food_quality = np.nan
                        if "سرویس" in reviews_rate["rate_dict"]:
                            survey_details_service_quality = float(reviews_rate["rate_dict"]["سرویس"].strip())
                        else:
                            survey_details_service_quality = np.nan
                        if "ارزش به قیمت" in reviews_rate["rate_dict"]:
                            survey_details_price_quality = float(reviews_rate["rate_dict"]["ارزش به قیمت"].strip())
                        else:
                            survey_details_price_quality = np.nan
                        if "دکوراسیون و محیط" in reviews_rate["rate_dict"]:
                            survey_details_environment_quality = float(
                                reviews_rate["rate_dict"]["دکوراسیون و محیط"].strip())
                        else:
                            survey_details_environment_quality = np.nan
                    else:
                        survey_details_food_quality = np.nan
                        survey_details_service_quality = np.nan
                        survey_details_price_quality = np.nan
                        survey_details_environment_quality = np.nan

                    if "rate_overview" in reviews_rate:
                        if "خیلی‌ بد" in reviews_rate["rate_overview"]:
                            survey_rate_very_bad_count = float(reviews_rate["rate_overview"]["خیلی‌ بد"].strip())
                        else:
                            survey_very_bad_count = np.nan
                        if "بد" in reviews_rate["rate_overview"]:
                            survey_rate_bad_count = float(reviews_rate["rate_overview"]["بد"].strip())
                        else:
                            survey_rate_bad_count = np.nan
                        if "متوسط" in reviews_rate["rate_overview"]:
                            survey_rate_average_count = float(reviews_rate["rate_overview"]["متوسط"].strip())
                        else:
                            survey_rate_average_count = np.nan
                        if "خوب" in reviews_rate["rate_overview"]:
                            survey_rate_good_count = float(reviews_rate["rate_overview"]["خوب"].strip())
                        else:
                            survey_rate_good_count = np.nan
                        if "عالی" in reviews_rate["rate_overview"]:
                            survey_rate_very_good_count = float(reviews_rate["rate_overview"]["عالی"].strip())
                        else:
                            survey_rate_very_good_count = np.nan
                    else:
                        survey_rate_very_bad_count = np.nan
                        survey_rate_bad_count = np.nan
                        survey_rate_average_count = np.nan
                        survey_rate_good_count = np.nan
                        survey_rate_very_good_count = np.nan

            res_dict['Survey_Details_Food_Quality'] = survey_details_food_quality
            res_dict['Survey_Details_Service_Quality'] = survey_details_service_quality
            res_dict['Survey_Details_Price_Quality'] = survey_details_price_quality
            res_dict['Survey_Details_Environment_Quality'] = survey_details_environment_quality
            res_dict['Survey_Rate_Very_Bad_Count'] = survey_rate_very_bad_count
            res_dict['Survey_Rate_Bad_Count'] = survey_rate_bad_count
            res_dict['Survey_Rate_Average_Count'] = survey_rate_average_count
            res_dict['Survey_Rate_Good_Count'] = survey_rate_good_count
            res_dict['Survey_Rate_Very_Good_Count'] = survey_rate_very_good_count

            information = row['information']
            telephone = ""
            # opening_hours1 = ""
            # closing_hours1 = ""
            # opening_hours2 = ""
            # closing_hours2 = ""
            if type(information) == str:
                information = json.loads(information)
                if len(information) > 0:
                    # if "address" in information:
                    #     address = information["address"]
                    # else:
                    #     address = ""
                    if "telephone" in information:
                        telephone = str(information["telephone"]).strip()

                    # if type(information) == dict:
                    #     for key, value in information.items():
                    #         if str(value).find("ساعت کار") != -1:
                    #             working_hours = str(value).replace("ساعت کار رستوران", "").strip()
                    #             # print(working_hours)
                    #             hour_list = re.findall(r"(\d+:?\d*)", working_hours)
                    #             if (len(hour_list) % 2) == 1:
                    #                 print(row["results_slug"])
                    #                 print(working_hours)
                    #                 print(hour_list)
                    #                 print(len(hour_list))
                    #             # if len(hour_list) > 6:
                    #             #     print(working_hours)
                    #             #     print(hour_list)
                    #             #     print(len(hour_list))

            else:
                # address = np.nan
                telephone = np.nan

            res_dict['Address'] = address
            res_dict['Telephone'] = telephone

            update_date_1 = row['update_date_details']
            update_date_2 = row['update_date_info']
            update_date = min(update_date_1, update_date_2)
            res_dict['Update_Date'] = update_date

            res_df = pd.DataFrame(res_dict, index=[0])
            self.temp_df = pd.concat([self.temp_df, res_df], axis=0)

        self.total_data.apply(create_main_columns, axis=1)
        self.temp_df.sort_values(by=['Update_Date'], ascending=False, inplace=True)
        self.temp_df.dropna(subset=['id'], inplace=True)
        self.temp_df.drop_duplicates(subset=['id'], inplace=True, keep='last')
        result_df = self.temp_df.copy()
        self.temp_df = pd.DataFrame()
        return result_df

    def get_df_main_type_crawl_file(self):
        self.temp_df = pd.DataFrame()

        def create_main_type_columns(row):
            res_dict = {}
            res_df = pd.DataFrame()
            idd = row['results_id']
            res_dict['id_main'] = idd

            types = json.loads(row['results_types'])
            if type(types) == list:
                if len(types) > 0:
                    for type_ in types:
                        res_dict["id_type"] = type_
                        this_df = pd.DataFrame(res_dict, index=[0])
                        res_df = pd.concat([res_df, this_df], axis=0)

            self.temp_df = pd.concat([self.temp_df, res_df], axis=0)

        self.total_data.apply(create_main_type_columns, axis=1)
        result_df = self.temp_df.copy()
        self.temp_df = pd.DataFrame()
        result_df = result_df.dropna(subset=['id_type'])
        result_df.drop_duplicates(subset=['id_main', 'id_type'], inplace=True, keep='last')
        return result_df

    def get_df_style_crawl_file(self):
        res_df = pd.DataFrame()
        style_set = set()
        for index, row in self.total_data.iterrows():
            if type(row['badges']) == str:
                res = json.loads(row['badges'])
                if type(res) == list:
                    for item in res:
                        style_set.add(item)

        res_df = pd.DataFrame(list(style_set), columns=['Style'])
        res_df.sort_values(by=['Style'], inplace=True)
        return res_df

    def get_df_features_crawl_file(self):
        res_df = pd.DataFrame()
        style_set = set()
        for index, row in self.total_data.iterrows():
            if type(row['features']) == str:
                res = json.loads(row['features'])
                if type(res) == list:
                    for item in res:
                        style_set.add(item)

        res_df = pd.DataFrame(list(style_set), columns=['Feature'])
        res_df.sort_values(by=['Feature'], inplace=True)
        return res_df

    def get_df_main_style_crawl_file(self):
        self.temp_df = pd.DataFrame()

        def create_main_style_columns(row):
            res_dict = {}
            res_df = pd.DataFrame()
            idd = row['results_id']
            res_dict['id_main'] = idd

            if type(row['badges']) == str:
                styles = json.loads(row['badges'])
                if type(styles) == list:
                    if len(styles) > 0:
                        for style in styles:
                            res_dict["Style"] = style
                            this_df = pd.DataFrame(res_dict, index=[0])
                            res_df = pd.concat([res_df, this_df], axis=0)

                self.temp_df = pd.concat([self.temp_df, res_df], axis=0)

        self.total_data.apply(create_main_style_columns, axis=1)
        result_df = self.temp_df.copy()
        self.temp_df = pd.DataFrame()
        result_df = result_df.dropna(subset=['Style'])
        result_df.drop_duplicates(subset=['id_main', 'Style'], inplace=True, keep='last')

        df_style_db = self.db_handle.read_style()

        final_df = pd.merge(left=result_df,
                            right=df_style_db,
                            on='Style',
                            how='inner',
                            suffixes=('_crawl', '_db'))

        final_df.drop(columns=['Style'], inplace=True)
        final_df.rename(columns={'id': 'id_style'}, inplace=True)
        final_df.drop_duplicates(subset=['id_main', 'id_style'], inplace=True, keep='last')
        self.temp_df = pd.DataFrame()
        return final_df

    def get_df_main_feature_crawl_file(self):
        self.temp_df = pd.DataFrame()

        def create_main_feature_columns(row):
            res_dict = {}
            res_df = pd.DataFrame()
            idd = row['results_id']
            res_dict['id_main'] = idd

            if type(row['features']) == str:
                features = json.loads(row['features'])
                if type(features) == list:
                    if len(features) > 0:
                        for feature in features:
                            res_dict["Feature"] = feature
                            this_df = pd.DataFrame(res_dict, index=[0])
                            res_df = pd.concat([res_df, this_df], axis=0)

                self.temp_df = pd.concat([self.temp_df, res_df], axis=0)

        self.total_data.apply(create_main_feature_columns, axis=1)
        result_df = self.temp_df.copy()
        self.temp_df = pd.DataFrame()
        result_df = result_df.dropna(subset=['Feature'])
        result_df.drop_duplicates(subset=['id_main', 'Feature'], inplace=True, keep='last')

        df_feature_db = self.db_handle.read_feature()

        final_df = pd.merge(left=result_df,
                            right=df_feature_db,
                            on='Feature',
                            how='inner',
                            suffixes=('_crawl', '_db'))

        final_df.drop(columns=['Feature'], inplace=True)
        final_df.rename(columns={'id': 'id_Feature'}, inplace=True)
        final_df.drop_duplicates(subset=['id_main', 'id_Feature'], inplace=True, keep='last')
        self.temp_df = pd.DataFrame()
        return final_df

    def get_df_working_time_crawl_file(self):
        self.temp_df = pd.DataFrame()

        def create_working_hours_columns(row):
            res_dict = {}
            res_list = []
            res_df = pd.DataFrame()
            idd = row['results_id']
            res_dict['id_main'] = idd

            if type(row['information']) == str:
                information = json.loads(row['information'])
                if type(information) == dict:
                    first_hour_work_list = []
                    second_hour_work_list = []
                    third_hour_work_list = []
                    for key, value in information.items():
                        if str(value).find("ساعت کار") != -1:
                            hour_work_list = re.findall(r'(\d\d?:?\d?\d?)', str(value))
                            for i, hour in enumerate(hour_work_list):
                                this_hour = unidecode(str(hour))
                                # print(this_hour)
                                this_time = None
                                if str(this_hour).find(":") != -1:
                                    if str(this_hour) == "24:00":
                                        this_time = datetime.strptime("23:59", '%H:%M').time()
                                        first_hour_work_list.append(this_time)
                                    elif str(this_hour) == "24:30":
                                        first_hour_work_list[-1] = datetime.strptime("11:30", '%H:%M').time()
                                        this_time = datetime.strptime("23:59", '%H:%M').time()
                                        first_hour_work_list.append(this_time)
                                    else:
                                        # try:
                                        this_time = datetime.strptime(this_hour, '%H:%M').time()
                                        first_hour_work_list.append(this_time)
                                        # except:
                                        #     this_time = datetime.strptime("23:59", '%H:%M').time()
                                        #     first_hour_work_list.append(this_time)
                                else:
                                    if str(this_hour) == "24":
                                        this_time = datetime.strptime("23:59", '%H:%M').time()
                                        first_hour_work_list.append(this_time)
                                    elif str(this_hour) == "30" or str(this_hour) == "45":
                                        first_hour_work_list[-1] = datetime.combine(date.today(), first_hour_work_list[-1]) + timedelta(minutes=int(this_hour))
                                        first_hour_work_list[-1] = first_hour_work_list[-1].time()
                                    elif re.match(r'\d\d\d\d', str(hour)):
                                        this_time = datetime.strptime(this_hour, '%H%M').time()
                                        first_hour_work_list.append(this_time)
                                    else:
                                        this_time = datetime.strptime(this_hour, '%H').time()
                                        first_hour_work_list.append(this_time)

                                # print(this_time)

                            for i, hour in enumerate(first_hour_work_list):
                                if (hour.minute % 5) != 0:
                                    if ((hour.hour == 23) or (hour.hour == 11)) and (hour.minute == 59):
                                        if len(first_hour_work_list) == 1:
                                            second_hour_work_list.append(dt_time(hour=0))
                                            second_hour_work_list.append(dt_time(hour=23, minute=59))
                                        else:
                                            second_hour_work_list.append(dt_time(hour=23, minute=59))
                                    else:
                                        second_hour_work_list.append(dt_time(hour=hour.hour))
                                        if hour.minute != 24:
                                            second_hour_work_list.append(dt_time(hour=hour.minute))
                                        else:
                                            second_hour_work_list.append(dt_time(hour=23, minute=59))
                                else:
                                    second_hour_work_list.append(hour)

                            if len(second_hour_work_list):
                                for i, hour in enumerate(second_hour_work_list):
                                    if i == 0:
                                        third_hour_work_list.append(hour)
                                    else:
                                        if hour < third_hour_work_list[i - 1] and hour.hour < 12:
                                            third_hour_work_list.append(dt_time(hour=hour.hour + 12, minute=hour.minute))
                                        else:
                                            third_hour_work_list.append(hour)

                            if len(third_hour_work_list):
                                for i, hour in enumerate(third_hour_work_list):
                                    each_dict = {}
                                    if i % 2 == 1:
                                        each_dict["Open"] = third_hour_work_list[i - 1]
                                        each_dict["Close"] = third_hour_work_list[i]
                                        res_list.append(each_dict)
                                res_df = pd.DataFrame(res_list)
                                res_df['id_main'] = idd
                                self.temp_df = pd.concat([self.temp_df, res_df], axis=0, ignore_index=True)

        self.total_data.apply(create_working_hours_columns, axis=1)
        self.temp_df.drop_duplicates(subset=['id_main', 'Open', 'Close'], inplace=True, keep='last')
        result_df = self.temp_df[['id_main', 'Open', 'Close']].copy()
        result_df['Open'] = pd.to_datetime(result_df['Open'], format='%H:%M:%S').dt.time
        result_df['Close'] = pd.to_datetime(result_df['Close'], format='%H:%M:%S').dt.time
        return result_df
