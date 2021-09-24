from decimal import Decimal
import boto3
import pandas as pd
import settings


logger = settings.logging.getLogger()


class Closes:
    write_client = boto3.client("timestream-write")
    query_client = boto3.client("timestream-query")

    insertion_limit = 100  # you can insert this many records at once
    time_periods_for_each_interval = {
        "5m": "30d",
        "15m": "90d",
        "1h": "360d",
        "4h": "720d",
        "1d": "1440d",
    }

    def __init__(self, ticker: str = "BTC_USDT", interval: str = "5m"):
        self.database = settings.TIMESTREAM_DATABASE
        self.table = settings.TIMESTREAM_TABLE
        self.ticker = ticker
        self.interval = interval
        self.time_period = self.time_periods_for_each_interval[interval]

    def get_close_values(self) -> dict:
        """
        time_period: time period you want candle records are in. str -> 6h
        interval: candle interval. str -> 5m
        measure: which MeasureValue you want. str -> close
        return dict -> last_prices = {"BTC": {"close": []}, {"time": [16213221,16876876]}, "ETH": {"close": []}, {"time": []}}
        """

        cls = self.__class__

        try:
            response = cls.query_client.query(
                QueryString=f"""
                SELECT ticker, interval, measure_name, measure_value::double, time 
                FROM "{self.database}"."{self.table}" 
                WHERE ticker='{self.ticker}' 
                AND interval='{self.interval}' 
                AND time >= ago({self.time_period}) 
                AND measure_name='close'
                ORDER BY time ASC"""
            )
        except self.query_client.exceptions.ValidationException:
            raise
        except Exception as e:
            print(e)
        return self.serialize_into_array(response)

    @staticmethod
    def serialize_into_array(response) -> dict:
        """
        params array of dicts: [{'Data': [{'ScalarValue': 'BTC_USDT'}, {'ScalarValue': 'low'}, {'ScalarValue': '56312.91'}, {'ScalarValue': '2021-03-13 00:30:00.000000000'}]}, {'Data': [{'ScalarValue': 'ETH'}, {'ScalarValue': 'open'}, {'ScalarValue': '1739.47'}, {'ScalarValue': '2021-03-13 00:30:00.000000000'}]}]
        returns: dict of arrays:
        """

        closes, timestamps = [], []

        for row in response["Rows"]:
            ticker = row["Data"][0]["ScalarValue"]
            interval = row["Data"][1]["ScalarValue"]
            close_value = float(row["Data"][3]["ScalarValue"])
            closes.append(close_value)
            timestamp = row["Data"][4]["ScalarValue"].split(".")[0]
            timestamps.append(timestamp)

        return {
            "ticker": ticker,
            "interval": interval,
            "closes": closes,
            "timestamps": timestamps,
        }

    def convert_to_dataframe(self):
        prepared_coin_close_values = self.get_close_values()
        closes_float = pd.to_numeric(
            pd.Series(
                prepared_coin_close_values["closes"],
                index=prepared_coin_close_values["timestamps"],
            ),
            downcast="float",
        )
        d = {prepared_coin_close_values["ticker"]: closes_float}
        return pd.DataFrame(d)
