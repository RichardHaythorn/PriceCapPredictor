import urllib3
import xmltodict
import pandas as pd

ROOT = "https://api.bmreports.com/BMRS"

API_KEY = "13rphyxuucloa1t"


class PriceLoader:
    def __init__(self, report_name: str = "DERSYSDATA") -> None:
        self.report_name = report_name
        self.service_type = "XML"

    def build_url(
        self,
        from_settlement_date: str,
        to_settlement_date: str,
    ) -> str:
        base = f"{ROOT}/{self.report_name}/V1?ServiceType=XML"
        query = f"&APIKey={API_KEY}&FromSettlementDate={from_settlement_date}&ToSettlementDate={to_settlement_date}"
        return base + query

    def load_url(self, url, timeout=60) -> dict:
        resp = urllib3.request("GET", url, timeout=timeout)
        respdict = xmltodict.parse(resp.data)["response"]["responseBody"][
            "responseList"
        ]["item"]
        return respdict

    def _make_settlement_datetime(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(settlementDatetime = df["settlementDate"] + (
            30 * (df["settlementPeriod"] - 1)
        ).astype("timedelta64[m]"))
        return df

    def _make_settlement_dates(self, year: str, month: str) -> tuple[(str, str)]:
        if len(year) < 4:
            raise Exception("Year must by in YYYY format")
        from_settlement_date = f"{year}-{str(month).zfill(2)}-01"
        to_settlement_date = f"{year}-{str(int(month)+1).zfill(2)}-01"
        return from_settlement_date, to_settlement_date

    def get_df(self, year: str, month: str) -> pd.DataFrame:
        from_settlement_date, to_settlement_date = self._make_settlement_dates(
            str(year), str(month)
        )
        url = self.build_url(from_settlement_date, to_settlement_date)
        respdict = self.load_url(url)
        df = pd.DataFrame(respdict)
        df = df.astype(
            {
                "settlementDate": "datetime64[ns]",
                "settlementPeriod": "int",
                "systemSellPrice": "float64",
            }
        )
        df = df.query("settlementDate.dt.month == @month")
        df = self._make_settlement_datetime(df)
        return df

    def save(self, filename: str, dataframe: pd.DataFrame) -> None:
        dataframe.to_parquet(filename)
