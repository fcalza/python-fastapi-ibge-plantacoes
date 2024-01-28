import os

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.types import DECIMAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from databases import Database

DATABASE_URL = (
    f"mysql+mysqlconnector://{os.environ['DB_USER']}"
    f":{os.environ['DB_PASS']}@"
    f"{os.environ['DB_HOST']}"
)
engine = create_engine(DATABASE_URL)
database = Database(DATABASE_URL)

Base = declarative_base()
Base.metadata.create_all(bind=engine)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# @todo index
class ProducaoMunicipios(Base):
    __tablename__ = "producao_municipios"

    pm_municipio_id = Column(Integer, primary_key=True)
    pm_ano = Column(Integer, primary_key=True)
    pm_area = Column(Integer)
    pm_quantidade = Column(Integer)


# @todo index
class ViewProdutividadeEstados(Base):
    __tablename__ = "view_produtividade_estados"

    estado = Column(String(2), primary_key=True)
    pm_ano = Column(Integer)
    total_area = Column(DECIMAL(15, 10))
    total_quantidade = Column(DECIMAL(15, 10))
    produtividade = Column(DECIMAL(5, 10))
