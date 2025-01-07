import polars as pl

class DataParser:
    @staticmethod
    def parse_time_series(data: dict) -> pl.DataFrame:
        time_series_key = "Time Series (Daily)"
        if time_series_key not in data:
            raise ValueError("Invalid data format")

        records = [
            {"date": date, **metrics}
            for date, metrics in data[time_series_key].items()
        ]

        df = pl.DataFrame(records)
        df = df.with_columns([
            pl.col("date").cast(pl.Date),
            pl.col("1. open").alias("open").cast(pl.Float64),
            pl.col("2. high").alias("high").cast(pl.Float64),
            pl.col("3. low").alias("low").cast(pl.Float64),
            pl.col("4. close").alias("close").cast(pl.Float64),
            pl.col("5. volume").alias("volume").cast(pl.Int64)
        ])

        return df

    @staticmethod
    def parse_overview(data: dict) -> pl.DataFrame:
        df = pl.Data