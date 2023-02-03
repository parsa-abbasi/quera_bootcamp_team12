import pandas as pd
import numpy as np
import os
import json


class FidilioDataCleaner:

    def __init__(self):
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
