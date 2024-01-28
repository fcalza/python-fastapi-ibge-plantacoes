from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, field_validator, Field, RootModel
from typing import List, Optional


class DadosPlantacao(BaseModel):
    NC: str  # "Nível Territorial (Código)"
    NN: str  # "Nível Territorial"
    MC: str  # "Unidade de Medida (Código)"
    MN: str  # Unidade de Medida"
    V: str  #:"Valor"
    D1C: str  # "Município (Código)"
    D1N: str  # "Município"
    D2C: str  # "Variável (Código)"
    D2N: str  # "Variável"
    D3C: str  # Ano (Código)"
    D3N: str  # "Ano"
    D4C: str  # "Produto das lavouras temporárias e permanentes (Código)"
    D4N: str  # "Produto das lavouras temporárias e permanentes"


class RetornoConsultaPlantacoes(RootModel):
    root: List[DadosPlantacao]


class ProducaoMunicipios(BaseModel):
    municipio_id: int
    ano: int
    area_colhida: int
    quantidade_produzida: int


class ListProducaoMunicipios(BaseModel):
    dados: Optional[List[ProducaoMunicipios]] = None


class ProdutividadePorEstado(BaseModel):
    estado: str
    produtividade: Decimal

    # def __init__(self, **data: dict):
    #     produtividade = Decimal(data["produtividade"]) if data["produtividade"] else 0
    #     super().__init__(estado=data["estado"], produtividade=produtividade)


class ProdutividadeAnoEstados(BaseModel):
    ano: int
    estados: List[str]

    @field_validator("estados")
    def ciglas_estado(cls, value):
        for v in value:
            assert len(v) == 2, f"Sigla do estado {v} deve ter 2 caracteres"
            assert v.isalpha(), f"Sigla do estado {v} deve conter apenas letras"
            assert v.upper() in [
                "AC",
                "AL",
                "AP",
                "AM",
                "BA",
                "CE",
                "DF",
                "ES",
                "GO",
                "MA",
                "MT",
                "MS",
                "MG",
                "PA",
                "PB",
                "PR",
                "PE",
                "PI",
                "RJ",
                "RN",
                "RS",
                "RO",
                "RR",
                "SC",
                "SP",
                "SE",
                "TO",
            ], f"Sigla do estado {v} não é válida"
        return [v.upper().strip() for v in value]


class PadraoRetorno(BaseModel):
    success: bool = False
    data: Optional[List] = None
    message: str = ""


class RetornoMunicipios(PadraoRetorno):
    data: Optional[List[ProducaoMunicipios]] = None


class RetornoProdutividades(PadraoRetorno):
    data: Optional[List[ProdutividadePorEstado]] = None


class AreaColhida(BaseModel):
    area_colhida: int


class RetornoAreaColhida(PadraoRetorno):
    data: AreaColhida


class RetornoQuantidadeProduziaMunicipioPorAno(PadraoRetorno):
    data: dict = Field(
        ...,
        example={
            "2018": {"1100031": 86234, "1100056": 112620},
            "2019": {"1100031": 84204, "1100056": 129956},
        },
    )


class InputAnosMunicipios(BaseModel):
    municipios: List[int]
    anos: List[int]

    @field_validator("municipios")
    def municipios_val(cls, value):
        for v in value:
            assert (
                len(str(v)) <= 7
            ), f"Código do município {v} deve ter no máximo 7 caracteres"
            assert isinstance(
                v, int
            ), f"Código do município {v} deve conter apenas números"
        return value

    @field_validator("anos")
    def anos_val(cls, value):
        ano_atual = datetime.now().year

        for ano in value:
            if not isinstance(ano, int) or not 2018 <= ano < ano_atual:
                raise ValueError(
                    f"O ano {ano} deve ter 4 dígitos numéricos maior ou igual a 2018 e menor que {ano_atual}."
                )

        return value
