
import requests
import pandas as pd
from datetime import datetime

# ======================
# CONFIGURAÇÃO DAS SÉRIES
# ======================

SERIES = {

    # JUROS
    "selic_diaria": {"code": 11, "agg": "mean"},
    "selic_acumulada_mes": {"code": 4390, "agg": "last"},
    "selic_anualizada_252": {"code": 1178, "agg": "mean"},

    # CÂMBIO
    "dolar_venda": {"code": 1, "agg": "last"},
    "dolar_compra": {"code": 10813, "agg": "last"},

    # INFLAÇÃO
    "ipca": {"code": 433, "agg": "last"},
    "ipca_monitorados": {"code": 4449, "agg": "last"},
    "ipca_livres": {"code": 11428, "agg": "last"},
    "ipca_servicos": {"code": 10844, "agg": "last"},

    # IBC-BR
    "ibc_br_sem_ajuste": {"code": 24363, "agg": "last"},
    "ibc_br_com_ajuste": {"code": 24364, "agg": "last"},

    # SETORES
    #"ibc_br_agro_sem_ajuste": {"code": 29601, "agg": "last"},
    #"ibc_br_agro_com_ajuste": {"code": 29602, "agg": "last"},
    "ibc_br_industria_sem_ajuste": {"code": 29603, "agg": "last"},
    "ibc_br_industria_com_ajuste": {"code": 29604, "agg": "last"},
    "ibc_br_servicos_sem_ajuste": {"code": 29605, "agg": "last"},
    "ibc_br_servicos_com_ajuste": {"code": 29606, "agg": "last"},

    # MERCADO DE TRABALHO
    "taxa_desocupacao": {"code": 24369, "agg": "last"},
    "pessoas_idade_trabalhar": {"code": 24370, "agg": "last"},
    "forca_trabalho": {"code": 24378, "agg": "last"},
    "ocupadas": {"code": 24379, "agg": "last"},
    "desocupadas": {"code": 24380, "agg": "last"},
    "rendimento_real_habitual": {"code": 24382, "agg": "last"},

    # CRÉDITO
    "credito_total": {"code": 20539, "agg": "last"},
    "credito_livre_pf_total": {"code": 20570, "agg": "last"},
    "comprometimento_renda_familias": {"code": 19881, "agg": "last"},
    "endividamento_familias": {"code": 19882, "agg": "last"},

    # EXTERNO
    # "reservas_internacionais": {"code": 3546, "agg": "last"},

}

START_DATE = "2017-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")


def format_date(date):
    return pd.to_datetime(date).strftime("%d/%m/%Y")


def fetch_bcb_series(code):

    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"

    params = {
        "formato": "json",
        "dataInicial": format_date(START_DATE),
        "dataFinal": format_date(END_DATE)
    }

    r = requests.get(url, params=params)
    r.raise_for_status()

    df = pd.DataFrame(r.json())

    df.rename(columns={"data": "date", "valor": "value"}, inplace=True)

    df["date"] = pd.to_datetime(df["date"], dayfirst=True)
    df["value"] = df["value"].str.replace(",", ".").astype(float)

    return df


def to_monthly(df, agg):

    df = df.set_index("date")

    if agg == "mean":
        df = df.resample("M").mean()
    else:
        df = df.resample("M").last()

    df = df.reset_index()
    df["date"] = df["date"].dt.to_period("M").astype(str)

    return df


dfs = []

for name, info in SERIES.items():

    print("Baixando:", name)

    df = fetch_bcb_series(info["code"])
    df = to_monthly(df, info["agg"])

    df.rename(columns={"value": name}, inplace=True)

    dfs.append(df)


dataset = dfs[0]

for df in dfs[1:]:
    dataset = dataset.merge(df, on="date", how="outer")

dataset_bcb = dataset.sort_values("date")

dataset_bcb.to_csv("data/processed/dataset_bcb_mensal.csv", index=False)

print("BCB dataset salvo.")
