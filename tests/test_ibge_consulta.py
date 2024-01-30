import json
import responses

from fastapi.encoders import jsonable_encoder

from src.schemas import RetornoConsultaPlantacoes
from src.service import consulta_area_colhida, consulta_quantidade_produzida


def test_payload_retorno_consulta_area_colhida():
    year = 2018
    with open("./tests/area_colhida.json") as f:
        retorno = RetornoConsultaPlantacoes(json.load(f))

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"https://apisidra.ibge.gov.br/values/t/5457/n6/all/v/216/p/{year}/c782/40124?formato=json",
            json=jsonable_encoder(retorno),
        )
        consulta = consulta_area_colhida(2018)

    assert consulta[0]["V"] == "Valor"
    assert consulta[0]["D1C"] == "Município (Código)"
    assert consulta[1]["V"] == "450"
    assert consulta[1]["D1C"] == "1100015"
    assert consulta[1]["D2N"] == "Área colhida"


def test_payload_retorno_consulta_quantidade_produzida():
    year = 2018
    with open("./tests/quantidade_produzida.json") as f:
        retorno_json = json.load(f)
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"https://apisidra.ibge.gov.br/values/t/5457/n6/all/v/214/p/{year}/c782/40124?formato=json",
            json=retorno_json,
        )
        consulta = consulta_quantidade_produzida(2018)

    RetornoConsultaPlantacoes(retorno_json)

    assert consulta[0]["V"] == "Valor"
    assert consulta[0]["D1C"] == "Município (Código)"
    assert consulta[1]["V"] == "1350"
    assert consulta[1]["D1C"] == "1100015"
    assert consulta[1]["D2N"] == "Quantidade produzida"
