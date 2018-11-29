import time
# from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import Pool
from threading import Thread, Event
from collections import deque
import csv
import pymysql

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

PARSING_WORKERS = 16
SQL_WORKERS = 4
MAX_LINE_LENGTH = 255
INSERT_BATCH_SIZE = 5000
REPORT_EVERY_COUNT = 1000  # How often to report progress
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
    conn.commit()  # Had issues not explicitly committing before close
    conn.close()


def create_batch(pool, query, ready_lines):
    batch = deque()
    try:
        cnt = 0
        for line in iter(ready_lines.pop, None):
            batch.append(line)
            cnt += 1
            if cnt >= INSERT_BATCH_SIZE:
                break
    except IndexError:
        pass
    logger.info("Creating insert job for %d items", len(batch))
    pool.apply_async(insert_lines, (query, batch))


# def queue_watcher(futures, ready_lines, pool, query, shutdown):
#     while len(futures) > 0 or not shutdown.isSet():
#         try:
#             item = futures.pop()
#         except IndexError:
#             # Nothing in queue yet but keep checking
#             logger.debug("Index error when trying to pop from futures queue")
#             continue
#
#         # Check if item's done being processed
#         if not item.ready():
#             logger.debug("Putting AsyncTask back in queue since it's not done")
#             futures.append(item)
#             time.sleep(0.01)  # Yield CPU control
#         else:
#             try:
#                 # Get the item result
#                 result = item.get(timeout=1)
#             except TimeoutError:
#                 continue
#
#             # Some issue with the result (failed data quality check?)
#             if not result:
#                 continue
#
#             # Line is ready to be inserted in db
#             logger.debug("AsyncTask complete. Adding to ready queue")
#             ready_lines.append(result)
#
#             # Batch of lines is ready for db
#             if len(ready_lines) == INSERT_BATCH_SIZE or len(futures) == 0:
#                 logger.info("Creating batch. Raw lines %d, Ready lines %d", len(futures), len(ready_lines))
#                 create_batch(pool, query, ready_lines)


def main():
    start_time = time.time()
    # A mock to run everything in the same thread (I think multiprocessing might have one somewhere, though)
    # parsing_pool = MockParsingPool(max_workers=WORKER_COUNT)

    # concurrent.futures ProcessExecutorPool Futures have a large overhead and are fairly slow
    # multiprocessing is much faster for tons of tiny jobs
    parsing_pool = Pool(processes=PARSING_WORKERS)
    parsing_complete = Event()
    inserting_pool = Pool(processes=SQL_WORKERS)
    ready_lines = deque()
    _query = "INSERT INTO `%s` VALUES(" % TABLE_NAME

    # csv_file = 'Parking_Violations_Issued_-_Fiscal_Year_2017.csv'
    csv_file = 'Parking_100000.csv'

    completed_cnt = [0]
    def add_to_queue(item):
        logger.debug("Adding item to ready queue: %s", item)
        completed_cnt[0] += 1
        if item:
            ready_lines.append(item)

        if len(ready_lines) == INSERT_BATCH_SIZE or parsing_complete.isSet():
            logger.info("Creating batch. Ready lines %d", len(ready_lines))
            create_batch(inserting_pool, _query, ready_lines)

    cnt = 0
    # Produce items
    with open(csv_file, 'rb') as f:
        _headers = tuple([col .strip() for col in next(f).decode("utf-8").split(",")])
        create_table(_headers, MAX_LINE_LENGTH)
        _query += ",".join(["%s"] * len(_headers)) + ")"
        for _line in f:
            parsing_pool.apply_async(
                process_line, (_headers, _line), callback=add_to_queue
            )
            cnt += 1
            if cnt % REPORT_EVERY_COUNT == 0:
                logger.info("CSV lines %8d; %8.3f lines/sec read; Ready queue %10d",
                            cnt, cnt/(time.time()-start_time), len(ready_lines))
                time.sleep(0)  # Yield CPU control

    parsing_pool.close()
    while completed_cnt[0] < cnt:
        logger.info("Waiting on callbacks. Submitted %d, received %d", cnt, completed_cnt[0])
        time.sleep(3)
    parsing_pool.join()
    logger.info("Parsing completed in %.3f at %.3f lines/sec", time.time()-start_time, cnt/(time.time()-start_time))
    parsing_complete.set()
    inserting_pool.close()
    inserting_pool.join()
    logger.info("Finished in %.3f seconds at %.3f lines/sec", time.time()-start_time, cnt/(time.time()-start_time))


if __name__ == "__main__":
    import cProfile
    pr = cProfile.Profile()
    pr.enable()
    main()
    pr.disable()
    pr.dump_stats("import.prof")
