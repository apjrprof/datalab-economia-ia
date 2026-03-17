import pandas as pd

bcb = pd.read_csv("data/processed/dataset_bcb_mensal.csv")
yahoo = pd.read_csv("data/processed/dataset_yfinance_mensal.csv")

dataset_final = bcb.merge(
    yahoo,
    on="date",
    how="outer"
).sort_values("date").reset_index(drop=True)

dataset_final.to_csv(
    "site/data/dataset_macro_mercado_mensal.csv",
    index=False
)

print("Dataset final atualizado para o site.")
