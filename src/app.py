import itertools
import os
import re
import sys
from datetime import date
from os.path import join

from PyQt5.QtWidgets import QApplication

import logger
from config import features
from ExcelWriter import ExcelWriter
from FileReader import FileReader
from GUI import GUI
from Storage import Storage


class App():
    def __init__(self):
        self.db = None
        self.gui = None
        self.excelWriter = ExcelWriter(
            f'./countsSHH_{date.today()}.xlsx')

        self.main()

    def __compute_logFiles(self):
        for i, file in enumerate(self.fileList):
            filepath = os.path.join(self.gui.logPath, file)
            reader = FileReader(filepath)

            partial_elem = f'{i + 1}/{len(self.fileList)}'

            print(f'{partial_elem} |DONE| {reader.filename}')
            progress = int((i + 1) * 100 / len(self.fileList)) - 10
            self.gui.worker.setWorkerValue(
                progress + 1 if progress <= 100 else 100)

            logger.execution_logger(partial_elem, reader.filename)
            counts = self.__fetch_counts(reader)
            self.excelWriter.write_to_excel(counts)

            self.db.clear_counts()

    def __comapare_files_with_db(self):
        rootList = self.__join_roots(self.gui.filePath)

        fetchedFilesDb = self.__fetch_files()
        filesDatabase = [file[0] for file in list(
            itertools.chain.from_iterable(fetchedFilesDb))]

        fetchedInvalidDb = self.__fetch_invalid()
        invalidDatabase = [file[0] for file in list(
            itertools.chain.from_iterable(fetchedInvalidDb))]

        self.__create_sheets_info(
            self.fileList, filesDatabase, invalidDatabase, 'Files_with_LOGS')
        self.__create_sheets_info(
            rootList, filesDatabase, invalidDatabase, 'Files_IN_Folder')

        self.db.clear_files()
        self.db.clear_invalid()

    def __create_sheets_info(self, fileList, filesDatabase, invalidList, sheetname):
        comparedListFirst, comparedListSecond = self.__returnNotMatches(
            fileList, filesDatabase)
        filesDf = self.excelWriter.createDataFrame(
            fileList, filesDatabase)
        comparedfilesDf = self.excelWriter.createDataFrame(
            comparedListFirst, comparedListSecond)
        self.excelWriter.write_another_sheet(
            filesDf, comparedfilesDf, invalidList, sheetname)

    def __get_fileList(self, path):
        return [file for file in os.listdir(path) if file.endswith('.log')]

    def __fetch_counts(self, readerObj: FileReader):
        start_idx = readerObj.read()
        if start_idx is not None:
            for feature in features:
                readerObj.parser(readerObj.lines[start_idx[0] + 1:], feature)
                if feature in readerObj.feature_names:
                    self.db.execute_counts(readerObj, feature)
        return readerObj.log_counts, self.db.db_counts

    def __fetch_files(self):
        self.db.execute_files()
        return self.db.db_files

    def __fetch_invalid(self):
        self.db.execute_invalid()
        return self.db.db_invalid

    def __join_roots(self, path):
        rootList = []
        for root, _, files in os.walk(path):
            for name in files:
                if name.endswith('.dwg'):
                    fullRoot = join(root, name)
                    endRootFile = self.__create_valid_path(fullRoot)
                    rootList.append(f'WRO_{endRootFile}')
        return rootList

    def __create_valid_path(self, fullRoot):
        endRoot = fullRoot.split("/")
        endRoot = endRoot[-1]
        rootFile = endRoot.split('\\')
        rootFile = rootFile[1:]
        return '--'.join(rootFile)

    def __clearEPGS(self, fileList):
        listWithoutEPSG = []
        for elem in fileList:
            clearedElem = re.sub('(?:^|\W)WRO_EPSG-(?:$|\W)\d{4}..', '', elem)
            listWithoutEPSG.append(clearedElem)
        return listWithoutEPSG

    def __returnNotMatches(self, firstList, secondList):
        firstListToCompare = self.__clearEPGS(firstList)
        secondListToCompare = self.__clearEPGS(secondList)
        comparedListFirst = [elem for idx, elem in enumerate(
            firstList) if firstListToCompare[idx] not in secondListToCompare]
        comparedListSecond = [elem for idx, elem in enumerate(
            secondList) if secondListToCompare[idx] not in firstListToCompare]

        return comparedListFirst, comparedListSecond

    def __init(self):
        self.fileList = self.__get_fileList(self.gui.logPath)
        self.db = Storage({'database': self.gui.database.lower(),
                           'instance': self.gui.instance})

        self.__compute_logFiles()
        self.__comapare_files_with_db()

        self.gui.worker.setWorkerValue(100)

        self.db.close_connection()

    def main(self):
        self.gui = GUI()
        self.gui.execute(self.__init)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    App()
    sys.exit(app.exec_())
