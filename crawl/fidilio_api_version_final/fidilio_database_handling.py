from fidilio_database_engine import DatabaseFidilioEngine
import mysql.connector
from mysql.connector import Error
import pandas as pd


class FidilioDatabaseHandling:
    def __init__(self, schema):
        self.food_db = DatabaseFidilioEngine()
        self.food_db.get_engine()
        self.schema = schema
        self.create_table_type()
        self.create_table_main()
        self.create_table_main_type()
        self.create_table_styles()
        self.create_table_features()
        self.create_table_main_style()
        self.create_table_main_feature()
        self.create_table_working_time()

    def create_table_type(self):
        sql_query = """CREATE TABLE IF NOT EXISTS `{}`.`{}`  (
  `id` bigint NOT NULL,
  `Title` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `Title_EN` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;""".format(
            self.food_db.database, self.schema + "_type")

        try:
            connection = self.food_db.get_cursor()
            cursor = connection.cursor()
            cursor.execute(sql_query)

        except Error as e:
            print(e)
            exit(1)

    def create_table_main(self):
        sql_query = """CREATE TABLE IF NOT EXISTS `{}`.`{}`  (
  `id` char(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `Name` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `Name_En` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `Url` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `Image` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `City` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `Latitude` double NULL DEFAULT NULL,
  `Longitude` double NULL DEFAULT NULL,
  `Address` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `Rating_Overall` double NULL DEFAULT NULL,
  `Price_Class` double NULL DEFAULT NULL,
  `Followers` double NULL DEFAULT NULL,
  `Social` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `Description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `survey_Quality_Total_Count` double NULL DEFAULT NULL,
  `Survey_Details_Food_Quality` double NULL DEFAULT NULL,
  `Survey_Details_Service_Quality` double NULL DEFAULT NULL,
  `Survey_Details_Price_Quality` double NULL DEFAULT NULL,
  `Survey_Details_Environment_Quality` double NULL DEFAULT NULL,
  `Survey_Rate_Very_Bad_Count` double NULL DEFAULT NULL,
  `Survey_Rate_Bad_Count` double NULL DEFAULT NULL,
  `Survey_Rate_Average_Count` double NULL DEFAULT NULL,
  `Survey_Rate_Good_Count` double NULL DEFAULT NULL,
  `Survey_Rate_Very_Good_Count` double NULL DEFAULT NULL,
  `Telephone` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `Update_Date` datetime NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;""".format(
            self.food_db.database, self.schema + "_main")

        try:
            connection = self.food_db.get_cursor()
            cursor = connection.cursor()
            cursor.execute(sql_query)

        except Error as e:
            print(e)
            exit(1)

    def create_table_main_type(self):
        sql_query = """CREATE TABLE IF NOT EXISTS `{0}`.`{1}`  (
  `row_id` int NOT NULL AUTO_INCREMENT,
  `id_main` char(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `id_type` bigint NOT NULL,
  PRIMARY KEY (`row_id`) USING BTREE,
  INDEX `fk_fidilio_main_tbl_main_type_idx`(`id_main` ASC) USING BTREE,
  INDEX `fk_fidilio_type_tbl_main_type_idx`(`id_type` ASC) USING BTREE,
  CONSTRAINT `fk_fidilio_main_tbl_main_type` FOREIGN KEY (`id_main`) REFERENCES `{0}`.`{2}_main` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_fidilio_type_tbl_main_type` FOREIGN KEY (`id_type`) REFERENCES `{0}`.`{2}_type` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;""".format(
            self.food_db.database, self.schema + "_main_type", self.schema)

        try:
            connection = self.food_db.get_cursor()
            cursor = connection.cursor()
            cursor.execute(sql_query)

        except Error as e:
            print(e)
            exit(1)

    def create_table_styles(self):
        sql_query = """CREATE TABLE IF NOT EXISTS `{}`.`{}`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `Style` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;""".format(
            self.food_db.database, self.schema + "_style")

        try:
            connection = self.food_db.get_cursor()
            cursor = connection.cursor()
            cursor.execute(sql_query)

        except Error as e:
            print(e)
            exit(1)

    def create_table_features(self):
        sql_query = """CREATE TABLE IF NOT EXISTS `{}`.`{}`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `Feature` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;""".format(
            self.food_db.database, self.schema + "_feature")

        try:
            connection = self.food_db.get_cursor()
            cursor = connection.cursor()
            cursor.execute(sql_query)

        except Error as e:
            print(e)
            exit(1)

    def create_table_main_style(self):
        sql_query = """CREATE TABLE IF NOT EXISTS `{0}`.`{1}`  (
  `row_id` bigint NOT NULL AUTO_INCREMENT,
  `id_main` char(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `id_style` bigint NULL DEFAULT NULL,
  PRIMARY KEY (`row_id`) USING BTREE,
  INDEX `fk_fidilio_main_tbl_main_style_idx`(`id_main` ASC) USING BTREE,
  INDEX `fk_fidilio_style_tbl_main_style_idx`(`id_style` ASC) USING BTREE,
  CONSTRAINT `fk_fidilio_main_tbl_main_style` FOREIGN KEY (`id_main`) REFERENCES `{0}`.`{2}_main` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_fidilio_style_tbl_main_style` FOREIGN KEY (`id_style`) REFERENCES `{0}`.`{2}_style` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;""".format(
            self.food_db.database, self.schema + "_main_style", self.schema)

        try:
            connection = self.food_db.get_cursor()
            cursor = connection.cursor()
            cursor.execute(sql_query)

        except Error as e:
            print(e)
            exit(1)

    def create_table_main_feature(self):
        sql_query = """CREATE TABLE IF NOT EXISTS `{0}`.`{1}`  (
  `row_id` int NOT NULL AUTO_INCREMENT,
  `id_main` char(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `id_Feature` bigint NOT NULL,
  PRIMARY KEY (`row_id`) USING BTREE,
  INDEX `fk_fidilio_main_tbl_main_feature_idx`(`id_main` ASC) USING BTREE,
  INDEX `fk_fidilio_feature_tbl_main_feature_idx`(`id_Feature` ASC) USING BTREE,
  CONSTRAINT `fk_fidilio_feature_tbl_main_feature` FOREIGN KEY (`id_Feature`) REFERENCES `{0}`.`{2}_feature` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_fidilio_main_tbl_main_feature` FOREIGN KEY (`id_main`) REFERENCES `{0}`.`{2}_main` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;""".format(
            self.food_db.database, self.schema + "_main_feature", self.schema)

        try:
            connection = self.food_db.get_cursor()
            cursor = connection.cursor()
            cursor.execute(sql_query)

        except Error as e:
            print(e)
            exit(1)

    def create_table_working_time(self):
        sql_query = """CREATE TABLE IF NOT EXISTS `{0}`.`{1}`  (
  `row_id` int NOT NULL AUTO_INCREMENT,
  `id_main` char(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `Open` time NULL DEFAULT NULL,
  `Close` time NULL DEFAULT NULL,
  PRIMARY KEY (`row_id`) USING BTREE,
  INDEX `fk_fidilio_main_tbl_working_time_idx`(`id_main` ASC) USING BTREE,
  CONSTRAINT `fk_fidilio_main_tbl_working_time` FOREIGN KEY (`id_main`) REFERENCES `{0}`.`{2}_main` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;""".format(
            self.food_db.database, self.schema + "_working_time", self.schema)

        try:
            connection = self.food_db.get_cursor()
            cursor = connection.cursor()
            cursor.execute(sql_query)

        except Error as e:
            print(e)
            exit(1)


    def read_data(self, sql_query) -> pd.DataFrame:
        try:
            df = pd.read_sql(sql_query, self.food_db.get_cursor())
            return df
        except Error as e:
            print(e)
            exit(1)

    def read_style(self) -> pd.DataFrame:
        sql_query = "SELECT * FROM " + self.schema + "_style"
        return self.read_data(sql_query)

    def read_feature(self) -> pd.DataFrame:
        sql_query = "SELECT * FROM " + self.schema + "_feature"
        return self.read_data(sql_query)


