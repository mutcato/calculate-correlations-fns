import boto3

class DataFrame:
    def __init__(self, table_name:str):
        resource = boto3.resource(
            "dynamodb"
        )
        self.table = resource.Table(table_name)
        
    def get_item(self, partition_key, sort_key):
        response = self.table.get_item(Key={
            partition_key["name"]: partition_key["value"],
            sort_key["name"]: sort_key["value"]
        })
        return response


    @staticmethod
    def get_longest_shortest_items(first_item, second_item):
        """
        Returns a tupple first element of the tupple is longest second element of the tupple is shortest.
        (longest_item, shortest_item)
        """
        length_of_the_longest = max(len(first_item["timestamps"]), len(second_item["timestamps"]))
        if len(first_item["timestamps"]) == length_of_the_longest:
            return first_item, second_item
        elif len(second_item["timestamps"]) == length_of_the_longest:
            return second_item, first_item

    def equalize(self, ticker1:str, ticker2:str, interval_metric:str):
        """
        Looks timestamps arrays in two array. Removes timestamps which are not exist in both two items and removes corresponding metric_value
        """
        first_item = [item for item in self.all_items if (item ["ticker"] == ticker1 and item["interval_metric"] == interval_metric)][0]
        second_item = [item for item in self.all_items if (item ["ticker"] == ticker2 and item["interval_metric"] == interval_metric)][0]
        if len(first_item["timestamps"]) == len(second_item["timestamps"]):
            return first_item, second_item

        difference = list(set(first_item["timestamps"])^set(second_item["timestamps"]))

        for timestamp in difference:
            if timestamp in first_item["timestamps"]:
                del first_item["metric_values"][first_item["timestamps"].index(timestamp)]
                first_item["timestamps"].remove(timestamp)

            if timestamp in second_item["timestamps"]:
                del second_item["metric_values"][second_item["timestamps"].index(timestamp)]
                second_item["timestamps"].remove(timestamp)

        return first_item, second_item

    def build_dataframe(self, tickers, interval_metric):
        for ticker in tickers:
            self.get_item(ticker, interval_metric)
            import pdb
            pdb.set_trace()


class Summary:
    def __init__(self):
        resource = boto3.resource(
            "dynamodb"
        )
        table_name = "metrics_summary"
        self.table = resource.Table(table_name)
        self.all_items = self.get_all_items()

    def get_all_items(self):
       result = self.table.scan()
       items = result["Items"]
       return items

    def filter_tickers(self, interval, metric):
        filtered_tickers = [item["ticker"] for item in self.all_items if item["interval_metric"] == f"{interval}_{metric}"]

        return filtered_tickers
