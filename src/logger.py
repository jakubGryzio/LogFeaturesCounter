import logging
from datetime import date

logging.basicConfig(
    filename=f"./countsSHH_{date.today()}.log", level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')


def execution_logger(partial_elem, filename):
    logging.info(
        f'{partial_elem} {filename}')
