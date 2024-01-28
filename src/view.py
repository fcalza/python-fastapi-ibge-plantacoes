import pandas as pd

from datetime import datetime

from sqlalchemy import inspect
from sqlalchemy.orm import Session
from typing import Optional, List, Union

from src.database import ProducaoMunicipios, ViewProdutividadeEstados
from src.schemas import (
    ListProducaoMunicipios,
    ProdutividadeAnoEstados,
    ProdutividadePorEstado,
    InputAnosMunicipios,
)
from src.service import consulta_area_colhida, consulta_quantidade_produzida
from src.exceptions import ProcessamentoException


# @TODO melhorar essa função
def insert_or_update(year, df_atualizado, db: Session) -> None:
    consulta = (
        db.query(ProducaoMunicipios).filter(ProducaoMunicipios.pm_ano == year).statement
    )
    df = pd.read_sql(consulta, db.bind)
    if df.empty:
        df_atualizado.to_sql(
            "producao_municipios", db.bind, if_exists="append", index=False
        )
        return

    df_insert = df_atualizado[
        ~df_atualizado["pm_municipio_id"].isin(df["pm_municipio_id"])
    ]
    if not df_insert.empty:
        df_insert.to_sql(
            "producao_municipios", db.bind, if_exists="append", index=False
        )

    df_update = df_atualizado.merge(
        df, on="pm_municipio_id", how="inner", suffixes=("_novo", "_database")
    )
    df_update_diff = df_update[
        (df_update["pm_area_novo"] != df_update["pm_area_database"])
        | (df_update["pm_quantidade_novo"] != df_update["pm_quantidade_database"])
    ]
    for index, row in df_update_diff.iterrows():
        db.query(ProducaoMunicipios).filter(
            ProducaoMunicipios.pm_municipio_id == row["pm_municipio_id"],
            ProducaoMunicipios.pm_ano == row["pm_ano_database"],
        ).update(
            {"pm_area": row["pm_area_novo"], "pm_quantidade": row["pm_quantidade_novo"]}
        )
    db.commit()


def processar_dados_retorno_plantacoes(ano: int) -> pd.DataFrame:
    print("consultando ano: ", ano)
    pd1 = pd.DataFrame(consulta_area_colhida(ano))
    print(f"{ano} - area_colhida")
    pd2 = pd.DataFrame(consulta_quantidade_produzida(ano))
    print(f"{ano} - quantidade_produzida")

    pd1 = pd1.drop(0)
    pd1["V"] = pd.to_numeric(pd1["V"], errors="coerce").fillna(0).astype(int)
    pd1["D1C"] = pd.to_numeric(pd1["D1C"], errors="coerce").fillna(0).astype(int)
    pd1_novo = pd1.loc[:, ["D1C", "V", "D3C"]].rename(
        columns={"V": "pm_area", "D1C": "pm_municipio_id", "D3C": "pm_ano"}
    )

    pd2 = pd2.drop(0)
    pd2["V"] = pd.to_numeric(pd2["V"], errors="coerce").fillna(0).astype(int)
    pd2["D1C"] = pd.to_numeric(pd2["D1C"], errors="coerce").fillna(0).astype(int)
    pd2_novo = pd2.loc[:, ["D1C", "V"]].rename(
        columns={"V": "pm_quantidade", "D1C": "pm_municipio_id"}
    )
    pd_novo = pd1_novo.merge(pd2_novo, on="pm_municipio_id", how="inner")

    return pd_novo


# @todo metodo asyncio
def processar_dados_plantacoes(db: Session) -> None:
    processados = []
    nao_processados = []
    for ano in range(2018, datetime.now().year):
        pd_novo = processar_dados_retorno_plantacoes(ano)
        if pd_novo.size <= 0:
            nao_processados.append(ano)
            continue
        insert_or_update(ano, pd_novo, db)
        processados.append(ano)

    if nao_processados:
        msgm = (
            f"Não há dados suficientes para processar o(s) ano(s) de {nao_processados}"
        )
        if processados:
            msgm = f"{msgm}. Os anos de {processados} foram processados com sucesso."
        raise ProcessamentoException(msgm)


def processar_dados_plantacoes_por_ano(ano, db: Session) -> None:
    pd_novo = processar_dados_retorno_plantacoes(ano)
    if pd_novo.size > 0:
        insert_or_update(ano, pd_novo, db)
    raise ProcessamentoException(
        f"Não há dados suficientes para processar o ano de {ano}"
    )


def get_municipio(municipio_id: int, db: Session) -> ListProducaoMunicipios:
    municipios = (
        db.query(ProducaoMunicipios)
        .filter(ProducaoMunicipios.pm_municipio_id == municipio_id)
        .all()
    )
    retorno = ListProducaoMunicipios(
        dados=[
            {
                "municipio_id": municipio.pm_municipio_id,
                "ano": municipio.pm_ano,
                "area_colhida": municipio.pm_area,
                "quantidade_produzida": municipio.pm_quantidade,
            }
            for municipio in municipios
        ]
    )
    return retorno


def get_municipio_por_ano(
    ano: int, municipio_id: int, db: Session
) -> Optional[ListProducaoMunicipios]:
    municipio = (
        db.query(ProducaoMunicipios)
        .filter(ProducaoMunicipios.pm_municipio_id == municipio_id)
        .filter(ProducaoMunicipios.pm_ano == ano)
        .first()
    )
    if municipio:
        municipio = ListProducaoMunicipios(
            dados=[
                {
                    "municipio_id": municipio.pm_municipio_id,
                    "ano": municipio.pm_ano,
                    "area_colhida": municipio.pm_area,
                    "quantidade_produzida": municipio.pm_quantidade,
                }
            ]
        )
    return municipio


def object_as_dict(obj):
    return {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}


def get_produtividade_estados_por_ano(
    dados: ProdutividadeAnoEstados, db: Session
) -> List:
    produtividade_estados = (
        db.query(ViewProdutividadeEstados)
        .filter(ViewProdutividadeEstados.estado.in_(dados.estados))
        .filter(ViewProdutividadeEstados.pm_ano == dados.ano)
        .all()
    )
    # list = [
    #     object_as_dict(produtividade_estado)
    #     for produtividade_estado in produtividade_estados
    # ]

    produtividade_estados = [
        ProdutividadePorEstado(
            estado=produtividade_estado.estado,
            produtividade=(
                float(produtividade_estado.produtividade)
                if produtividade_estado.produtividade
                else 0
            ),
        )
        for produtividade_estado in produtividade_estados
    ]
    return produtividade_estados


def get_municipios_quantidade_produzida(
    dados: InputAnosMunicipios, db: Session
) -> dict:
    consulta = (
        db.query(ProducaoMunicipios)
        .filter(ProducaoMunicipios.pm_municipio_id.in_(dados.municipios))
        .filter(ProducaoMunicipios.pm_ano.in_(dados.anos))
        .all()
    )
    if consulta:
        consulta = [
            {
                "municipio_id": municipio.pm_municipio_id,
                "ano": municipio.pm_ano,
                "area_colhida": municipio.pm_area,
                "quantidade_produzida": municipio.pm_quantidade,
            }
            for municipio in consulta
        ]
        df = pd.DataFrame(consulta)
        df_transformado = df.pivot_table(
            index="ano", columns="municipio_id", values="quantidade_produzida"
        )
        consulta = df_transformado.to_dict(orient="index")

    return consulta


def delete(year: int, db: Session) -> None:
    db.query(ProducaoMunicipios).filter(ProducaoMunicipios.pm_ano == year).delete()
    db.commit()
