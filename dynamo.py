import boto3
import pandas as pd
from botocore.config import Config
from time import time


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
        return {
            response["Item"][self.partition_key["name"]]: response["Item"][
                "metric_values"
            ]
        }

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
    def __init__(self, interval: str = "5m", metric: str = "close"):
        self.interval = interval
        self.metric = metric
        self.columns_to_drop = ["BUSDT_USDT", "USDC_USDT", "DAI_USDT"]
        self.table = self._build(interval, metric)
        self.row_number = self.table.shape[0]
        self.column_number = self.table.shape[1]
        self.headers = list(self.table.columns)

    def _get_all_items(self, interval, metric):
        summary = Summary()
        filtered_tickers = summary.filter_tickers(interval, metric)
        items_as_df = []
        for index, ticker in enumerate(filtered_tickers):
            partition_key = {"name": "ticker", "value": ticker}
            sort_key = {"name": "interval_metric", "value": f"{interval}_{metric}"}
            item = Item(partition_key, sort_key)
            items_as_df.append(item.convert_to_dataframe())
            if index == 6:
                break

        return items_as_df

    def _build(self, interval, metric):
        dataframes = self._get_all_items(interval, metric)
        result_dataframe = pd.concat(dataframes, axis=1)
        result_dataframe.drop(self.columns_to_drop, axis = 1, inplace = True, errors="ignore")
        return result_dataframe

    def _calculate_correlation(self, master_ticker, slave_ticker)->dict:
        temp_dataframe = self.table[[master_ticker, slave_ticker]]
        temp_dataframe = temp_dataframe.dropna()
        try:
            correlation = {
                "pair": f"{master_ticker}-{slave_ticker}",
                "interval": self.interval,
                "metric": self.metric,
                "pearson_corr": temp_dataframe[master_ticker].corr(
                    temp_dataframe[slave_ticker]
                ),
                "spearman_corr": temp_dataframe[master_ticker].corr(
                    temp_dataframe[slave_ticker], method="spearman"
                ),
                "kendall_corr": temp_dataframe[master_ticker].corr(
                    temp_dataframe[slave_ticker], method="kendall"
                ),
                "time": int(time()),
            }
        except Exception as e:
            # To-do: Add logger
            print(e)

        return correlation

    def calculate_correlations(self)->list:
        tickers = self.headers
        correlations = []
        i = 0
        while i < len(tickers):
            master_ticker = tickers.pop(i)
            for slave_ticker in tickers:
                correlations.append(self._calculate_correlation(master_ticker, slave_ticker))

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
