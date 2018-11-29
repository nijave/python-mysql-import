import time
# from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import Pool
from collections import deque
import csv
import pymysql

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

WORKER_COUNT = 16
MAX_LINE_LENGTH = 255
INSERT_BATCH_SIZE = 10000
MYSQL_DB_DETAILS = {
    "user": "root",
    "password": "",
    "host": "localhost",
    "database": "parking",
}
TABLE_NAME = "tickets"


class MockFuture(object):
    def __init__(self, value):
        self.value = value

    @staticmethod
    def ready():
        return True

    def get(self, *args, **kwargs):
        logger.info("Returning %s", self.value)
        return self.value


class MockParsingPool(object):
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def apply_async(*args):
        func = args[0]
        logger.info("Executing %s", func)
        return MockFuture(func(*args[1]))

    @staticmethod
    def shutdown(*args, **kwargs):
        return True


def create_table(headers, var_char_length):
    logger.info("Creating table")
    conn = pymysql.connect(**MYSQL_DB_DETAILS)
    cur = conn.cursor()
    table_sql = "CREATE TABLE {} ({})".format(
        TABLE_NAME,
        ",".join(["`%s` varchar(%d)" % (col, var_char_length) for col in headers])
    )
    logger.info("Creating table: %s", table_sql)
    cur.execute("DROP TABLE IF EXISTS `%s`" % TABLE_NAME)
    cur.execute(table_sql)
    conn.close()


def process_line(headers, line):
    line = line.decode("utf-8", "replace").encode("ascii", "replace").decode("utf-8")
    reader = csv.reader((line,), delimiter=",")
    line_csv = next(reader)
    if len(headers) != len(line_csv):
        return
    line_csv = [item if len(item) <= MAX_LINE_LENGTH else item[0:MAX_LINE_LENGTH-3] + '...' for item in line_csv]
    return line_csv


def insert_lines(query, lines):
    conn = pymysql.connect(**MYSQL_DB_DETAILS)
    cur = conn.cursor()
    cur.executemany(query, lines)
    conn.commit()
    conn.close()


def main():
    start_time = time.time()
    # parsingPool = ProcessPoolExecutor(max_workers=WORKER_COUNT)
    # parsingPool = MockParsingPool(max_workers=WORKER_COUNT)
    parsingPool = Pool(processes=WORKER_COUNT)
    futures = deque()
    ready_lines = deque()
    _query = "INSERT INTO `%s` VALUES(" % TABLE_NAME

    csv_file = 'Parking_Violations_Issued_-_Fiscal_Year_2017.csv'
    # csv_file = 'Parking_10000.csv'

    cnt = 0
    # Produce items
    with open(csv_file, 'rb') as f:
        _headers = tuple([col .strip() for col in next(f).decode("utf-8").split(",")])
        create_table(_headers, MAX_LINE_LENGTH)
        _query += ",".join(["%s"] * len(_headers)) + ")"
        for _line in f:
            futures.append(
                parsingPool.apply_async(
                    process_line, (_headers, _line)
                )
            )
            cnt += 1
            if cnt % 10000 == 0:
                logger.info("CSV lines %8d; %6.3f lines/sec", cnt, cnt/(time.time()-start_time))

    # Consume items
    while len(futures) > 0:
        item = futures.pop()
        if not item.ready():
            futures.append(item)
            time.sleep(1)
        else:
            try:
                result = item.get(timeout=1)
            except TimeoutError:
                continue
            if not result:
                continue
            ready_lines.append(result)
            if len(ready_lines) == INSERT_BATCH_SIZE or len(futures) == 0:
                logger.info("Creating insert job for %d items", len(ready_lines))
                parsingPool.apply_async(insert_lines, (_query, ready_lines))
                ready_lines = deque()
                logger.info("Lines pending parsing %d", len(futures))

    # parsingPool.shutdown(wait=True)
    parsingPool.close()
    parsingPool.join()
    logger.info("Finished in %.3f at %.3f", time.time()-start_time, cnt/(time.time()-start_time))


if __name__ == "__main__":
    import cProfile
    pr = cProfile.Profile()
    pr.enable()
    main()
    pr.disable()
    pr.dump_stats("import.prof")
