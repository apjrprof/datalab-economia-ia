import pandas as pd
import yfinance as yf

TICKERS = {

    "ibovespa": "^BVSP",
    "sp500": "^GSPC",
    "nasdaq": "^IXIC",
    "dow_jones": "^DJI",

    "bova11": "BOVA11.SA",
    "smal11": "SMAL11.SA",
    "ivvb11": "IVVB11.SA",

    "usd_brl": "USDBRL=X",
    "eur_brl": "EURBRL=X",

    "ouro": "GC=F",
    "petroleo_wti": "CL=F",
    "petroleo_brent": "BZ=F",
    "soja": "ZS=F",
    "milho": "ZC=F",
    "cafe": "KC=F",

    "treasury_10y": "^TNX",
    "treasury_30y": "^TYX",

    "petr4": "PETR4.SA",
    "vale3": "VALE3.SA",
    "itub4": "ITUB4.SA",
    "bbas3": "BBAS3.SA",
    "wege3": "WEGE3.SA",

    "bitcoin": "BTC-USD",
    "ethereum": "ETH-USD",
}

START_DATE = "2017-01-01"


def fetch_series(ticker):

    df = yf.download(
        ticker,
        start=START_DATE,
        auto_adjust=False,
        progress=False
    )

    if df.empty:
        return pd.DataFrame(columns=["date", "value"])

    df = df.reset_index()

    value_col = "Adj Close" if "Adj Close" in df.columns else "Close"

    df = df[["Date", value_col]]

    df.columns = ["date", "value"]

    df["date"] = pd.to_datetime(df["date"])

    return df


def to_monthly(df):

    df = df.set_index("date").resample("ME").last().reset_index()

    df["date"] = df["date"].dt.to_period("M").astype(str)

    return df


all_dfs = []

for name, ticker in TICKERS.items():

    print("Baixando:", name)

    df = fetch_series(ticker)
    df = to_monthly(df)

    df.rename(columns={"value": name}, inplace=True)

    all_dfs.append(df)


dataset = all_dfs[0]

for df in all_dfs[1:]:
    dataset = dataset.merge(df, on="date", how="outer")

dataset = dataset.sort_values("date")

dataset.to_csv("data/processed/dataset_yfinance_mensal.csv", index=False)

print("Yahoo dataset salvo.")
