from dotenv import load_dotenv

load_dotenv()

import logging

from typing import List
from fastapi import FastAPI, Depends, HTTPException, Path, Query
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import ValidationError
from sqlalchemy.orm import Session
from datetime import datetime
from starlette import status
from starlette.responses import RedirectResponse

from src.autenticacao_jwt import create_jwt_token, decode_jwt_token, oauth2_scheme
from src.exceptions import ProcessamentoException
from src.utils import validar_solicitacao
from src.database import get_db
from src.schemas import (
    PadraoRetorno,
    ProdutividadeAnoEstados,
    RetornoProdutividades,
    RetornoMunicipios,
    InputAnosMunicipios,
    RetornoQuantidadeProduziaMunicipioPorAno,
    RetornoAreaColhida,
)
from src.view import (
    get_municipio,
    processar_dados_plantacoes,
    delete,
    get_municipio_por_ano,
    get_produtividade_estados_por_ano,
    get_municipios_quantidade_produzida,
    processar_dados_plantacoes_por_ano,
)


app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
ano_limite = datetime.now().year - 1


@app.exception_handler(Exception)
async def exception_handler(request, exc):
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=PadraoRetorno(message="Erro interno do servidor").model_dump(),
    )


@app.exception_handler(ValidationError)
async def exception_handler(request, exc):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=PadraoRetorno(message=str(exc).split("\n")[2].strip()).model_dump(),
    )


@app.exception_handler(HTTPException)
async def exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content=PadraoRetorno(message=exc.detail).model_dump(),
    )


@app.get("/", include_in_schema=False)
def redirect():
    return RedirectResponse(url="/docs")


@app.post("/token")
def login(request_form_user: OAuth2PasswordRequestForm = Depends()):
    """
    Endpoint para autenticar usuário e gerar token JWT.
    Expiração do token: 10 minutos
    """
    if (
        request_form_user.username == "username"
        and request_form_user.password == "password"
    ):
        token = create_jwt_token(request_form_user.username)
        return {"access_token": token, "token_type": "bearer"}
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )


@app.post(
    "/municipio/processar",
    tags=["Processamento - municípios"],
    summary=f"Processa os dados de municípios brasileiros de 2018 até {datetime.now().year - 1}",
    response_model=PadraoRetorno,
)
def processar_dados(
    db: Session = Depends(get_db), jwt_token: str = Depends(decode_jwt_token)
):
    try:
        processar_dados_plantacoes(db)
        PadraoRetorno(
            success=True,
            data=None,
            message=f"Dados processados com sucesso de 2018 até {datetime.now().year - 1}",
        ).model_dump()
    except ProcessamentoException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao processar dados",
        )
    return


@app.post(
    "/municipio/processar/{ano}",
    tags=["Processamento - municípios"],
    summary="Processa os dados de municípios brasileiros em um ano",
    response_model=PadraoRetorno,
)
def processar_dados_por_ano(
    ano: int = Path(
        ...,
        description="Ano de produção (até 4 dígitos)",
        ge=2018,
        le=datetime.now().year - 1,
    ),
    db: Session = Depends(get_db),
    jwt_token: str = Depends(decode_jwt_token),
):
    try:
        processar_dados_plantacoes_por_ano(ano, db)
        return PadraoRetorno(
            success=True,
            data=None,
            message=f"Dados processados com sucesso para o ano {ano}",
        ).model_dump()
    except ProcessamentoException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao processar dados",
        )


@app.delete(
    "/municipio/deletar/{ano}",
    tags=["Processamento - municípios"],
    summary="Deleta os dados de municípios brasileiros em um ano",
    response_model=PadraoRetorno,
)
def deletar_dados_de_um_ano(
    ano: int = Path(
        ..., description="Ano de produção (até 4 dígitos)", ge=2018, le=ano_limite
    ),
    db: Session = Depends(get_db),
    jwt_token: str = Depends(oauth2_scheme),
):
    if ano < 2018 or ano >= 2024:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Ano de produção deve ser um número inteiro positivo",
        )
    delete(ano, db)
    return PadraoRetorno(
        success=True, data=None, message=f"Dados deletados com sucessopara o ano {ano}"
    ).model_dump()


@app.get(
    "/municipio/{codigo_municipio}",
    tags=["Municípios"],
    summary="Retorna os dados de um município",
    response_model=RetornoMunicipios,
)
def consulta_dados_de_um_municipio(
    codigo_municipio: int = Path(
        ..., description="Código do município (até 7 dígitos)"
    ),
    db: Session = Depends(get_db),
):
    municipios = get_municipio(codigo_municipio, db)
    if not municipios:
        raise HTTPException(status_code=404, detail="Municipio não encontrado")
    return RetornoMunicipios(
        success=True, data=[municipio.model_dump() for municipio in municipios.dados]
    ).model_dump()


@app.get(
    "/municipio/{ano}/{codigo_municipio}",
    tags=["Municípios"],
    response_model=RetornoMunicipios,
    summary="Retorna os dados de um município brasileiro em um ano",
)
def consulta_um_municipio_por_ano(
    ano: int = Path(
        ..., description="Ano de produção (até 4 dígitos)", ge=2018, le=ano_limite
    ),
    codigo_municipio: int = Path(
        ..., description="Código do município (até 7 dígitos)"
    ),
    db: Session = Depends(get_db),
):
    municipio = get_municipio_por_ano(ano, codigo_municipio, db)
    if not municipio:
        raise HTTPException(status_code=404, detail="Municipio não encontrado")
    return RetornoMunicipios(
        success=True, data=[municipio.dados[0].model_dump()]
    ).model_dump()


@app.get(
    "/municipio/area_colhida/{ano}/{codigo_municipio}",
    tags=["Municípios"],
    summary="Retorna a área colhida de um município brasileiro em um ano",
    response_model=RetornoAreaColhida,
)
def consulta_area_colhida(
    ano: int = Path(
        ..., description="Ano de produção (até 4 dígitos)", ge=2018, le=ano_limite
    ),
    codigo_municipio: int = Path(
        ..., description="Código do município (até 7 dígitos)"
    ),
    db: Session = Depends(get_db),
):
    """
    Endpoint para retornar o valor de área colhida informando UM município brasileiro em UM ano
    """
    if ano < 2018 or ano >= 2024 or not codigo_municipio:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Valide os valores informados",
        )

    municipio = get_municipio_por_ano(ano, codigo_municipio, db)
    if not municipio:
        raise HTTPException(status_code=404, detail="Municipio não encontrado")
    return RetornoAreaColhida(
        success=True, data={"area_colhida": municipio.dados[0].area_colhida}
    ).model_dump()


@app.get(
    "/produtividade/{ano}/estados",
    tags=["Produtividade"],
    summary="Retorna a produtividade de estados brasileiros em um ano",
    response_model=RetornoProdutividades,
)
def produtividade_estados_ano(
    ano: int = Path(
        ..., description="Ano de produção (até 4 dígitos)", ge=2018, le=ano_limite
    ),
    estados: List[str] = Query(
        None, description="Lista de siglas de estado. Ex: ['SC', 'RS', 'PR']"
    ),
    db: Session = Depends(get_db),
):
    """
    Endpoint para retornar o(s) valor(es) de produtividade informando UM ou MAIS
    estados brasileiros simultaneamente em UM ano

    Parâmetros:
    - **ano**: Ano no formato YYYY.
    - **estados**: Lista de siglas de estado. Ex: ['SC', 'RS', 'PR']

    """
    if ano < 2018 or ano >= 2024 or not estados:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Valide os valores informados",
        )
    dados = get_produtividade_estados_por_ano(
        ProdutividadeAnoEstados(ano=ano, estados=estados), db
    )
    if not dados:
        raise HTTPException(status_code=404, detail="Dados não encontrados")
    return RetornoProdutividades(
        success=True, data=[dado.model_dump() for dado in dados]
    ).model_dump()


# @todo forma diferente de fazer a consulta com um payload muito complexo/grande
@app.post(
    "/municipios/quantidade_produzida",
    tags=["Municípios"],
    summary="Retorna os dados de quantidade produzida de municípios brasileiros em anos",
    response_model=RetornoQuantidadeProduziaMunicipioPorAno,
)
def quantidade_produzida_municipios_por_ano(
    dados: InputAnosMunicipios, db: Session = Depends(get_db)
):
    """
    Endpoint para retornar múltiplos valores de “quantidade produzida” informando UM ou MAIS municípios E UM ou MAIS anos.
    Limite para consulta de 100 dados simultaneamente. Isto é, a combinação de municípios e anos não resulte em mais do que 100 informações.
    Por exemplo:<br>
    100 municípios com 1 ano = 100 dados<br>
    20 municípios com 5 anos = 100 dados<br>
    1 município para 5 anos = 5 dados<br>

    """
    validar_solicitacao(dados)
    resultado = get_municipios_quantidade_produzida(dados, db)
    if not resultado:
        raise HTTPException(status_code=404, detail="Dados não encontrados")
    return RetornoQuantidadeProduziaMunicipioPorAno(
        success=True, data=resultado
    ).model_dump()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("__main__:app", port=8000, reload=True)
