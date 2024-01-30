import json
import requests
from src.schemas import RetornoConsultaPlantacoes


def consulta_area_colhida(year: int) -> json:
    area_colhida = f"https://apisidra.ibge.gov.br/values/t/5457/n6/all/v/216/p/{year}/c782/40124?formato=json"
    #exemplo modelo de retorno padrão com model do pydantic
    consulta_plantacoes = RetornoConsultaPlantacoes.model_validate(requests.get(area_colhida).json())
    return consulta_plantacoes.model_dump()


def consulta_quantidade_produzida(year: int) -> json:
    quantidade_produzida = f"https://apisidra.ibge.gov.br/values/t/5457/n6/all/v/214/p/{year}/c782/40124?formato=json"
    return requests.get(quantidade_produzida).json()
