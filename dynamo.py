from datetime import date
from decimal import Decimal
from time import time

import boto3
import pandas as pd
from botocore.config import Config


class Table:
    def __init__(self, table_name):
        self.resource = boto3.resource(
            "dynamodb", config=Config(read_timeout=585, connect_timeout=585)
        )
        self.table_name = table_name

        self.table = self.resource.Table(self.table_name)


class Item(Table):
    def __init__(self, partition_key: dict, sort_key: dict):
        Table.__init__(self, "metrics2")
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.data = self.get()

    def get(self):
        response = self.table.get_item(
            Key={
                self.partition_key["name"]: self.partition_key["value"],
                self.sort_key["name"]: self.sort_key["value"],
            }
        )

        truncated_metrics = self.remove_duplicates(response["Item"]["metric_values"])

        return {response["Item"][self.partition_key["name"]]: truncated_metrics}

    @staticmethod
    def remove_duplicates(list_of_dicts: list) -> list:
        """
        Removes metrics recorded with the same timestamp for the same ticker
        Params: List of dicts
        Return: List of dicts
        """
        seen = set()
        new = []
        for d in list_of_dicts:
            t = tuple(d.items())
            if t not in seen:
                seen.add(t)
                new.append(d)
        return new

    def prepare_for_dataframe(self):
        ticker = list(self.data.keys())[0]
        close_prices = self.data[ticker]
        timestamps, closes = [], []
        for time_close_pair in close_prices:
            timestamp = int(list(time_close_pair.keys())[0])
            timestamps.append(timestamp)
            close_price = list(time_close_pair.values())[0]
            closes.append(close_price)

        return {"ticker": ticker, "closes": closes, "timestamps": timestamps}

    def convert_to_dataframe(self):
        prepared_item = self.prepare_for_dataframe()
        closes_float = pd.to_numeric(
            pd.Series(prepared_item["closes"], index=prepared_item["timestamps"]),
            downcast="float",
        )
        d = {prepared_item["ticker"]: closes_float}
        return pd.DataFrame(d)


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
            partition_key = {"name": "ticker", "value": ticker}
            sort_key = {
                "name": "interval_metric",
                "value": f"{self.interval}_{self.metric}",
            }
            item = Item(partition_key, sort_key)
            items_as_df.append(item.convert_to_dataframe())
            # if index == 5:
            #     break

        return items_as_df

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
                "pair": f"{master_ticker}-{slave_ticker}",
                "interval": self.interval,
                "metric": self.metric,
                "pearson_corr": Decimal(
                    str(
                        temp_dataframe[master_ticker].corr(temp_dataframe[slave_ticker])
                    )
                ),
                "spearman_corr": Decimal(
                    str(
                        temp_dataframe[master_ticker].corr(
                            temp_dataframe[slave_ticker], method="spearman"
                        )
                    )
                ),
                "kendall_corr": Decimal(
                    str(
                        temp_dataframe[master_ticker].corr(
                            temp_dataframe[slave_ticker], method="kendall"
                        )
                    )
                ),
                "time": int(time()),
            }
        except Exception as e:
            # To-do: Add logger
            print(e)

        return correlation

    def calculate_correlations(self) -> list:
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
    def sort_correlations(correlations, order_by="pearson_corr", reverse=True):
        sorted_corrs = sorted(correlations, key=lambda k: k[order_by], reverse=reverse)
        return sorted_corrs


class Summary(Table):
    def __init__(self):
        Table.__init__(self, "metrics_summary")
        self.all_items = self.get_all_items()

    def get_all_items(self):
        result = self.table.scan()
        items = result["Items"]
        return items

    def filter_tickers(self, interval, metric):
        filtered_tickers = [
            item["ticker"]
            for item in self.all_items
            if item["interval_metric"] == f"{interval}_{metric}"
        ]

        return filtered_tickers

    def get_unique_intervals(self):
        seen = []
        unique_intervals = []
        for item in self.all_items:
            if item["interval_metric"] not in seen:
                seen.append(item["interval_metric"])
                unique_intervals.append(item["interval_metric"].split("_")[0])
        return unique_intervals

    @staticmethod
    def get_unique_metrics():
        """
        This is only 'close' for now. Other metrics can be added later
        For example: open, high, low, volume, number of trades
        """
        return ["close"]


class CorrelationsTable(Table):
    def __init__(self):
        Table.__init__(self, "correlations")

    def insert(self, correlations):
        for correlation in correlations:
            correlation["date"] = f"""{correlation["interval"]}_{correlation["metric"]}_{date.today()}"""
            self.table.put_item(Item=correlation)


def calculate_correlations_for_all_intervals_for_all_metrics() -> list:
    summary = Summary()
    intervals = summary.get_unique_intervals()
    metrics = summary.get_unique_metrics()
    result = []
    for metric in metrics:
        for interval in intervals:
            tickers = summary.filter_tickers(interval, metric)
            df = DataFrameTable(tickers, interval, metric)
            correlations = df.calculate_correlations()
            sorted = df.sort_correlations(correlations)
            result.append(sorted)
    flattened_result = [item for items in result for item in items]
    return flattened_result


def lambda_handler(event, context):
    correlations = calculate_correlations_for_all_intervals_for_all_metrics()
    corr_table = CorrelationsTable()
    corr_table.insert(correlations)
