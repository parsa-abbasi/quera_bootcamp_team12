import requests
from bs4 import BeautifulSoup
from Fidilio_class import Fidilio
from Fidilio_data_cleaninig import FidilioDataCleaner
from fidilio_database_handling import FidilioDatabaseHandling
from urllib.parse import quote_plus

"""fidilio = Fidilio()
fidilio.all_actions_for_get_data_info()
fidilio.all_actions_for_get_data_details()"""

# https://fidilio.com/restaurants/borito/
# password = """g+QyjX%+^e2Qnn$W"""
# username = """user_group12"""
# print(quote_plus(password))
# print(quote_plus(username))
# exit(1)


fidilio_cleaner = FidilioDataCleaner()
db_handle = FidilioDatabaseHandling(schema="fidilio")

# type table
df_type_web = fidilio_cleaner.get_df_type_crawl_file()
df_type = df_type_web.copy()

try:
    df_type.to_sql(name=db_handle.schema + "_type", con=db_handle.food_db.engin, if_exists="append", index=False)
except Exception as e:
    print(e)
    exit(1)

# main table
df_main_web = fidilio_cleaner.get_df_main_crawl_file()
df_main = df_main_web.copy()

try:
    df_main.to_sql(name=db_handle.schema + "_main", con=db_handle.food_db.engin, if_exists="append", index=False)
except Exception as e:
    print(e)
    exit(1)

# main_type table
df_main_type_web = fidilio_cleaner.get_df_main_type_crawl_file()
df_main_type = df_main_type_web.copy()

try:
    df_main_type.to_sql(name=db_handle.schema + "_main_type", con=db_handle.food_db.engin, if_exists="append", index=False)
except Exception as e:
    print(e)
    exit(1)

# styles table
df_style_web = fidilio_cleaner.get_df_style_crawl_file()
df_style = df_style_web.copy()

try:
    df_style.to_sql(name=db_handle.schema + "_style", con=db_handle.food_db.engin, if_exists="append", index=False)
except Exception as e:
    print(e)
    exit(1)

# features table
df_features_web = fidilio_cleaner.get_df_features_crawl_file()
df_features = df_features_web.copy()

try:
    df_features.to_sql(name=db_handle.schema + "_feature", con=db_handle.food_db.engin, if_exists="append", index=False)
except Exception as e:
    print(e)
    exit(1)

# main_style table
df_main_style_web = fidilio_cleaner.get_df_main_style_crawl_file()
df_main_style = df_main_style_web.copy()

try:
    df_main_style.to_sql(name=db_handle.schema + "_main_style", con=db_handle.food_db.engin, if_exists="append", index=False)
except Exception as e:
    print(e)
    exit(1)

# main_feature table
df_main_feature_web = fidilio_cleaner.get_df_main_feature_crawl_file()
df_main_feature = df_main_feature_web.copy()

try:
    df_main_feature.to_sql(name=db_handle.schema + "_main_feature", con=db_handle.food_db.engin, if_exists="append", index=False)
except Exception as e:
    print(e)
    exit(1)

# working_time table
df_working_time_web = fidilio_cleaner.get_df_working_time_crawl_file()
df_working_time = df_working_time_web.copy()

try:
    df_working_time.to_sql(name=db_handle.schema + "_working_time", con=db_handle.food_db.engin, if_exists="append", index=False)
except Exception as e:
    print(e)
    exit(1)

