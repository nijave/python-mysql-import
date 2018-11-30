import cProfile
import csv
import logging
import time
from collections import deque
from multiprocessing import Pool

import pymysql

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s'
)

SQL_WORKERS = 4
MAX_LINE_LENGTH = 255
INSERT_BATCH_SIZE = 25000
# How often to report progress
REPORT_EVERY_COUNT = 10000
MYSQL_DB_DETAILS = {
    "user": "root",
    "password": "",
    "host": "localhost",
    "database": "parking",
}
TABLE_NAME = "tickets"
FILE_NAME = 'Parking_Violations_Issued_-_Fiscal_Year_2017.csv'


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
    """
    Create basic table where every column is a
    varchar
    :param headers: column names
    :param var_char_length: max varchar length for column
    :return: None
    """
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


def process_lines(query, headers, batch, cnt, start_time):
    lines = [line.decode("utf-8", "replace").encode("ascii", "replace").decode("utf-8") for line in batch]
    reader = csv.reader(lines, delimiter=",")

    csv_lines = []
    for line in reader:
        if len(line) != len(headers):
            logger.info("Skipping line since it's missing columns %s", line)
            continue
        csv_lines.append([item if len(item) <= MAX_LINE_LENGTH else item[0:MAX_LINE_LENGTH-3] + '...' for item in line])

    conn = pymysql.connect(**MYSQL_DB_DETAILS)
    cur = conn.cursor()
    insert_start = time.time()
    insert_cnt = cur.executemany(query, csv_lines)
    conn.commit()  # Had issues not explicitly committing before close
    conn.close()
    insert_time = time.time() - insert_start

    logger.info("Insert %d lines of %d (%.3f lines/sec) in %.2f (%.3f inserts/sec)",
                insert_cnt, cnt, cnt/(time.time()-start_time), insert_time, insert_cnt/insert_time)


if __name__ == "__main__":
    pr = cProfile.Profile()
    pr.enable()
    start_time = time.time()
    # A mock to run everything in the same thread (I think multiprocessing might have one somewhere, though)
    # parsing_pool = MockParsingPool(max_workers=WORKER_COUNT)

    # concurrent.futures ProcessExecutorPool Futures have a large overhead and are fairly slow
    # multiprocessing is much faster for tons of tiny jobs
    inserting_pool = Pool(processes=SQL_WORKERS)
    lines = deque()
    query = "INSERT INTO `%s` VALUES(" % TABLE_NAME

    def create_batch(pool, queue):
        pool.apply_async(
            process_lines, (query, headers, queue, cnt, start_time)
        )
        return deque()

    cnt = 0
    # Produce items
    with open(FILE_NAME, 'rb') as f:
        headers = tuple([col .strip() for col in next(f).decode("utf-8").split(",")])
        create_table(headers, MAX_LINE_LENGTH)
        query += ",".join(["%s"] * len(headers)) + ")"
        for _line in f:
            lines.append(_line)
            cnt += 1
            if cnt % REPORT_EVERY_COUNT == 0:
                logger.info("CSV lines %8d; %8.3f lines/sec read; Ready queue %10d",
                            cnt, cnt/(time.time()-start_time), len(lines))
            if cnt % INSERT_BATCH_SIZE == 0:
                lines = create_batch(inserting_pool, lines)

    create_batch(inserting_pool, lines)

    pr.disable()
    pr.dump_stats("import.prof")

    inserting_pool.close()
    inserting_pool.join()
    logger.info("Job used %d workers with batches of %d", SQL_WORKERS, INSERT_BATCH_SIZE)
    logger.info("Finished in %.3f seconds at %.3f lines/sec", time.time()-start_time, cnt/(time.time()-start_time))
