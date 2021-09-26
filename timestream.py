from typing import List
import pandas as pd
import awswrangler as wr
import settings


logger = settings.logging.getLogger()


class ClosesDataFrame:
    time_periods_for_each_interval = {
        "5m": "15d",
        "15m": "45d",
        "1h": "180d",
        "4h": "720d",
        "1d": "1440d",
    }
    columns_to_drop = ["BUSD_USDT", "USDC_USDT", "DAI_USDT"]

    def __init__(self, tickers: list = ["BTC_USDT", "ETH_USDT"], interval: str = "5m"):
        self.database = settings.TIMESTREAM_DATABASE
        self.table = settings.TIMESTREAM_TABLE
        self.tickers = self.drop_redundant_tickers(tickers)
        self.interval = interval
        self.time_period = self.time_periods_for_each_interval[interval]

    def drop_redundant_tickers(self, tickers: list) -> list:
        dropped = set(tickers) - set(self.columns_to_drop)
        return [*dropped]

    def get_close_values(self) -> pd.DataFrame:

        tickers_str = "'" + "', '".join(self.tickers) + "'"

        try:
            df = wr.timestream.query(
                f"""
                SELECT ticker, measure_value::double, time 
                FROM "{self.database}"."{self.table}" 
                WHERE ticker IN ({tickers_str}) 
                AND interval='{self.interval}' 
                AND time >= ago({self.time_period}) 
                AND measure_name='close'
                ORDER BY time ASC"""
            )
        except self.query_client.exceptions.ValidationException:
            raise
        except Exception as e:
            print(e)

        return df

    @staticmethod
    def split(df) -> List[pd.DataFrame]:
        tickers = df.ticker.unique()
        list_of_dataframes = []
        for ticker in tickers:
            reformated_df = (
                df[df.ticker == ticker]
                .set_index("time")
                .drop(columns=["ticker"], errors="ignore")
                .rename(columns={"measure_value::double": ticker})
            )
            list_of_dataframes.append(reformated_df)
        return list_of_dataframes

    @staticmethod
    def concat(dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        return pd.concat(dataframes, axis=1)

    def build(self):
        closes = self.get_close_values()
        splited_df = self.split(closes)
        concatted_df = self.concat(splited_df)
        return concatted_df
