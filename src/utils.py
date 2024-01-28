from fastapi import HTTPException

from src.schemas import InputAnosMunicipios


def validar_solicitacao(dados: InputAnosMunicipios):
    total_dados = len(dados.municipios) * len(dados.anos)
    if total_dados > 100:
        raise HTTPException(
            status_code=400, detail="Solicitação excede limite de 100 dados"
        )
