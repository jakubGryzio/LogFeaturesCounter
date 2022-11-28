import re


class FileReader():

    def __init__(self, filepath):
        self.filepath = filepath
        self.filename = self.__convert_filename()
        self.epsg = ''
        self.log_counts = []
        self.feature_names = []

    def read(self):
        with open(self.filepath, 'r', encoding='utf-8') as f:
            self.lines = f.readlines()
            start_idx = self.__get_start_index(self.lines[len(self.lines) // 2:])

            if not start_idx:
                self.log_counts.append([self.filename, 'FILE WITHOUT OUTPUT', ''])
                return None
            return start_idx

    def parser(self, lines, feature):
        indx = self.__check_feature_in_line(lines, feature)

        if indx:
            line = lines[indx[0]]
            self.epsg = self.__get_epsg(line)
            count = self.__get_count(line)
            self.log_counts.append([self.filename, f'{feature}{self.epsg}', count])
            self.feature_names.append(feature)

    def __get_start_index(self, lines):
        pattern = "Feature output statistics for `ORACLE_SPATIAL' writer using keyword `ORACLE_SPATIAL_1'"
        return [ind for ind, line in enumerate(lines) if pattern in line]

    def __check_feature_in_line(self, lines, feature):
        return [ind for ind, line in enumerate(lines) if f'|STATS |{feature}' in line and f'({feature}' in line]

    def __get_epsg(self, line):
        return re.search(r'_\d{4}', line)[0].split('_')[1]

    def __get_count(self, line):
        return line.split(' ')[-1]

    def __convert_filename(self):
        indx = -22
        return self.filepath[:indx].split('log')[1][1:]
