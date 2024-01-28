import json
import requests


def consulta_area_colhida(year: int) -> json:
    area_colhida = f"https://apisidra.ibge.gov.br/values/t/5457/n6/all/v/216/p/{year}/c782/40124?formato=json"
    return requests.get(area_colhida).json()


def consulta_quantidade_produzida(year: int) -> json:
    quantidade_produzida = f"https://apisidra.ibge.gov.br/values/t/5457/n6/all/v/214/p/{year}/c782/40124?formato=json"
    return requests.get(quantidade_produzida).json()
