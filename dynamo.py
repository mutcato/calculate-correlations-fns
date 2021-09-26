import json
import logging
from decimal import Decimal
from datetime import date, datetime, timedelta
from time import time
from typing import List

import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

import pandas as pd

from helpers import timeit
from timestream import Closes

logger = logging.getLogger(__name__)


class Table:
    def __init__(self, table_name):
        self.dynamodb = boto3.resource(
            "dynamodb", config=Config(read_timeout=585, connect_timeout=585)
        )
        self.client = boto3.client("dynamodb")
        self.table_name = table_name

        self.table = self.dynamodb.Table(self.table_name)


class DataFrameTable:
    def __init__(
        self,
        tickers: list = ["BTC_USDT", "ETH_USDT"],
        interval: str = "5m",
        metric: str = "close",
    ):
        self.tickers = tickers
        self.interval = interval
        self.metric = metric
        self.columns_to_drop = ["BUSDT_USDT", "USDC_USDT", "DAI_USDT"]
        self.table = self._build()
        self.row_number = self.table.shape[0]
        self.column_number = self.table.shape[1]
        self.headers = list(self.table.columns)

    def _get_all_items(self):
        items_as_df = []
        for index, ticker in enumerate(self.tickers):
            item = Closes(ticker, self.interval)
            items_as_df.append(item.convert_to_dataframe())
            # if index == 7:
            #     break

        return items_as_df

    @timeit
    def _build(self):
        dataframes = self._get_all_items()
        result_dataframe = pd.concat(dataframes, axis=1)
        result_dataframe.drop(
            self.columns_to_drop, axis=1, inplace=True, errors="ignore"
        )
        return result_dataframe

    def _calculate_correlation(self, master_ticker, slave_ticker) -> dict:
        temp_dataframe = self.table[[master_ticker, slave_ticker]]
        temp_dataframe = temp_dataframe.dropna()
        try:
            correlation = {
                "date": str(date.today()),
                "pair": f"{master_ticker}-{slave_ticker}",
                "interval": self.interval,
                "metric": self.metric,
                "master": master_ticker,
                "slave": slave_ticker,
                "pearson_corr": str(
                    temp_dataframe[master_ticker].corr(temp_dataframe[slave_ticker])
                ),
                "spearman_corr": str(
                    temp_dataframe[master_ticker].corr(
                        temp_dataframe[slave_ticker], method="spearman"
                    )
                ),
                "kendall_corr": str(
                    temp_dataframe[master_ticker].corr(
                        temp_dataframe[slave_ticker], method="kendall"
                    )
                ),
                "TTL": int(time()) + 180 * 24 * 60 * 60,
            }

        except Exception as e:
            # To-do: Add logger
            print(e)

        return correlation

    @timeit
    def calculate_correlations(self) -> List[dict]:
        tickers = self.headers
        correlations = []
        i = 0
        while i < len(tickers):
            master_ticker = tickers.pop(i)
            for slave_ticker in tickers:
                correlations.append(
                    self._calculate_correlation(master_ticker, slave_ticker)
                )

        return correlations

    @staticmethod
    def convert_string(correlations):
        """
        Converts DynamoDB json export format into string format
        """
        concatenated_correlations = "\n".join(
            [str(correlation) for correlation in correlations]
        )
        return concatenated_correlations

@timeit
def write_into_s3(correlations, file_format: str = "dynamodb_export"):
    s3 = boto3.resource("s3")
    s3object = s3.Object("correlations-batch-data", f"correlations.json")

    if file_format == "json":
        data = json.dumps(correlations)
    else:
        data = correlations

    s3object.put(Body=(bytes(data, "utf-8")))


class Summary(Table):
    def __init__(self):
        Table.__init__(self, "metrics_summary")
        self.all_items = self.get_all_items()

    def get_all_items(self):
        result = self.table.scan()
        items = result["Items"]
        return items

    def filter_tickers(self, interval, metric) -> List[dict]:
        # Sorts list according to 'volume_in_usdt' field in decreasing order
        sorted_items = sorted(self.all_items, key = lambda item: item["volume_in_usdt"], reverse=True)

        filtered_tickers = [
            item["ticker"]
            for item in sorted_items
            if item["interval_metric"] == f"{interval}_{metric}"
        ]


        # Return only high volume coins/tokens
        return filtered_tickers[:200]

    def get_unique_intervals(self):
        """
        This is only '5m' for now. Other metrics can be added later
        For example: 15m, 1h, 4h, 8h, 1d, 3d,
        """
        return ["5m"]

    @staticmethod
    def get_unique_metrics():
        """
        This is only 'close' for now. Other metrics can be added later
        For example: open, high, low, volume, number of trades
        """
        return ["close"]



@timeit
def calculate_correlations_for_all_intervals_for_all_metrics(
    convert_to_str: bool = False,
) -> list:
    summary = Summary()
    intervals = summary.get_unique_intervals()
    metrics = summary.get_unique_metrics()
    result = []
    for metric in metrics:
        for interval in intervals:
            tickers = summary.filter_tickers(interval, metric)
            df = DataFrameTable(tickers, interval, metric)
            correlations = df.calculate_correlations()
            result.append(correlations)
    flattened_result = [item for items in result for item in items]
    if convert_to_str:
        return df.convert_string(flattened_result)
    return flattened_result


def lambda_handler(event, context):
    correlations = calculate_correlations_for_all_intervals_for_all_metrics(
        convert_to_str=False
    )
    write_into_s3(correlations, file_format="json")
