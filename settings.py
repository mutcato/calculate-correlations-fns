import logging

LOG_FORMAT = "%(levelname)s %(filename)s line:%(lineno)d %(asctime)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

TIMESTREAM_DATABASE = "coinmove"
TIMESTREAM_TABLE = "technical_data"
TIMESTREAM_TEST_TABLE = "price"
