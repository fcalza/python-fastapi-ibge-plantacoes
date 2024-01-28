### ter o pipenv instalado
### ter o python3.8 instalado

cp .env_exemplo .env
pipenv install --dev
pipenv shell
### Alterar os dados do arquivo .env
DB_HOST
DB_PASS
DB_USER
SECRET_KEY

## Rodar o projeto
./run.sh


## para rodar os testes
pytest