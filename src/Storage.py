import cx_Oracle
import config_database as config


class Storage():
    def __init__(self, dsn):
        self.dsn = config.dsn.replace('db', dsn['database']).replace(
            '_instance', dsn['instance'])
        self.connection = None
        self.cursor = None
        self.db_counts = []
        self.db_files = []
        self.db_invalid = []
        self.__connect()

    def __connect(self):
        try:
            self.connection = cx_Oracle.connect(
                config.username,
                config.password,
                self.dsn
            )

            self.cursor = self.connection.cursor()

        except cx_Oracle.Error as error:
            print(error)

    def close_connection(self):
        if self.connection:
            self.connection.close()

    def execute_counts(self, reader, feature):
        sql = f"SELECT COUNT(*) FROM {feature}{reader.epsg} WHERE PLIK = 'WRO_{reader.filename}'"
        try:
            self.db_counts.append([*self.cursor.execute(sql)][0][0])
        except cx_Oracle.Error as error:
            print(error)

    def execute_files(self):
        sql = '''SELECT * FROM (
Select distinct plik from kodgik_u1.punkty_2174 union
Select distinct plik from kodgik_u1.punkty_2176 union
Select distinct plik from kodgik_u1.punkty_2177 union
Select distinct plik from kodgik_u1.punkty_2178 union
Select distinct plik from kodgik_u1.punkty_2179 union
Select distinct plik from kodgik_u1.teksty_2174 union
Select distinct plik from kodgik_u1.teksty_2176 union
Select distinct plik from kodgik_u1.teksty_2177 union
Select distinct plik from kodgik_u1.teksty_2178 union
Select distinct plik from kodgik_u1.teksty_2179 union
Select distinct plik from kodgik_u1.teksty_etykiety_2174 union
Select distinct plik from kodgik_u1.teksty_etykiety_2176 union
Select distinct plik from kodgik_u1.teksty_etykiety_2177 union
Select distinct plik from kodgik_u1.teksty_etykiety_2178 union
Select distinct plik from kodgik_u1.teksty_etykiety_2179 union
Select distinct plik from kodgik_u1.poligony_2174 union
Select distinct plik from kodgik_u1.poligony_2176 union
Select distinct plik from kodgik_u1.poligony_2177 union
Select distinct plik from kodgik_u1.poligony_2178 union
Select distinct plik from kodgik_u1.poligony_2179 union
Select distinct plik from kodgik_u1.blokiexploded_2174 union
Select distinct plik from kodgik_u1.blokiexploded_2176 union
Select distinct plik from kodgik_u1.blokiexploded_2177 union
Select distinct plik from kodgik_u1.blokiexploded_2178 union
Select distinct plik from kodgik_u1.blokiexploded_2179 union
Select distinct plik from kodgik_u1.blokiplus_2174 union
Select distinct plik from kodgik_u1.blokiplus_2176 union
Select distinct plik from kodgik_u1.blokiplus_2177 union
Select distinct plik from kodgik_u1.blokiplus_2178 union
Select distinct plik from kodgik_u1.blokiplus_2179 union
Select distinct plik from kodgik_u1.etykiety2_blokow_2174 union
Select distinct plik from kodgik_u1.etykiety2_blokow_2176 union
Select distinct plik from kodgik_u1.etykiety2_blokow_2177 union
Select distinct plik from kodgik_u1.etykiety2_blokow_2178 union
Select distinct plik from kodgik_u1.etykiety2_blokow_2179 union
Select distinct plik from kodgik_u1.wipeout_2174 union
Select distinct plik from kodgik_u1.wipeout_2176 union
Select distinct plik from kodgik_u1.wipeout_2177 union
Select distinct plik from kodgik_u1.wipeout_2178 union
Select distinct plik from kodgik_u1.wipeout_2179 union
Select distinct plik from kodgik_u1.polilinie_2174 union
Select distinct plik from kodgik_u1.polilinie_2176 union
Select distinct plik from kodgik_u1.polilinie_2177 union
Select distinct plik from kodgik_u1.polilinie_2178 union
Select distinct plik from kodgik_u1.polilinie_2179)'''
        try:
            self.db_files.append([*self.cursor.execute(sql)])
        except cx_Oracle.Error as error:
            print(error)

    def execute_invalid(self):
        sql = ['Select distinct plik from kodgik_u1.punkty_invalid_2174',
               'Select distinct plik from kodgik_u1.punkty_invalid_2176',
               'Select distinct plik from kodgik_u1.punkty_invalid_2177',
               'Select distinct plik from kodgik_u1.punkty_invalid_2178',
               'Select distinct plik from kodgik_u1.punkty_invalid_2179',
               'Select distinct plik from kodgik_u1.teksty_invalid_2174',
               'Select distinct plik from kodgik_u1.teksty_invalid_2176',
               'Select distinct plik from kodgik_u1.teksty_invalid_2177',
               'Select distinct plik from kodgik_u1.teksty_invalid_2178',
               'Select distinct plik from kodgik_u1.teksty_invalid_2179',
               'Select distinct plik from kodgik_u1.teksty_etykiety_invalid_2174',
               'Select distinct plik from kodgik_u1.teksty_etykiety_invalid_2176',
               'Select distinct plik from kodgik_u1.teksty_etykiety_invalid_2177',
               'Select distinct plik from kodgik_u1.teksty_etykiety_invalid_2178',
               'Select distinct plik from kodgik_u1.teksty_etykiety_invalid_2179',
               'Select distinct plik from kodgik_u1.poligony_invalid_2174',
               'Select distinct plik from kodgik_u1.poligony_invalid_2176',
               'Select distinct plik from kodgik_u1.poligony_invalid_2177',
               'Select distinct plik from kodgik_u1.poligony_invalid_2178',
               'Select distinct plik from kodgik_u1.poligony_invalid_2179',
               'Select distinct plik from kodgik_u1.blokiexploded_invalid_2174',
               'Select distinct plik from kodgik_u1.blokiexploded_invalid_2176',
               'Select distinct plik from kodgik_u1.blokiexploded_invalid_2177',
               'Select distinct plik from kodgik_u1.blokiexploded_invalid_2178',
               'Select distinct plik from kodgik_u1.blokiexploded_invalid_2179',
               'Select distinct plik from kodgik_u1.blokiplus_invalid_2174',
               'Select distinct plik from kodgik_u1.blokiplus_invalid_2176',
               'Select distinct plik from kodgik_u1.blokiplus_invalid_2177',
               'Select distinct plik from kodgik_u1.blokiplus_invalid_2178',
               'Select distinct plik from kodgik_u1.blokiplus_invalid_2179',
               'Select distinct plik from kodgik_u1.etykiety2_blokow_invalid_2174',
               'Select distinct plik from kodgik_u1.etykiety2_blokow_2176',
               'Select distinct plik from kodgik_u1.etykiety2_blokow_invalid_2177',
               'Select distinct plik from kodgik_u1.etykiety2_blokow_invalid_2178',
               'Select distinct plik from kodgik_u1.etykiety2_blokow_invalid_2179',
               'Select distinct plik from kodgik_u1.wipeout_invalid_2174',
               'Select distinct plik from kodgik_u1.wipeout_invalid_2176',
               'Select distinct plik from kodgik_u1.wipeout_invalid_2177',
               'Select distinct plik from kodgik_u1.wipeout_invalid_2178',
               'Select distinct plik from kodgik_u1.wipeout_invalid_2179',
               'Select distinct plik from kodgik_u1.polilinie_invalid_2174',
               'Select distinct plik from kodgik_u1.polilinie_invalid_2176',
               'Select distinct plik from kodgik_u1.polilinie_invalid_2177',
               'Select distinct plik from kodgik_u1.polilinie_invalid_2178',
               'Select distinct plik from kodgik_u1.polilinie_invalid_2179']

        for select in sql:
            try:
                self.db_invalid.append([*self.cursor.execute(select)])
            except cx_Oracle.Error as error:
                print(error)

    def clear_counts(self):
        self.db_counts = []

    def clear_files(self):
        self.db_files = []

    def clear_invalid(self):
        self.db_invalid = []
