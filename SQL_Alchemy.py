
from sqlalchemy import Column, Integer, String,Float,Date,ForeignKey,create_engine,ForeignKeyConstraint
from sqlalchemy.orm import declarative_base, sessionmaker, relationship


# 1) Verwende den Datenbanknamen, den du in pgAdmin4 erstellt hast:
DATABASE = "GAM_DB"  # oder "sqlalchemy_test", falls du so benannt hast
USER = "postgres"                # dein PostgreSQL-Benutzername
PASSWORD = "rootroot"            # dein PostgreSQL-Passwort
HOST = "localhost"               # bei lokalem Betrieb
PORT = "5432"                    # Standard-Port von PostgreSQL

# 2) Engine erstellen: Hier die Verbindung zur DB
engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

# 3) Session einrichten
Session = sessionmaker(bind=engine)
session = Session()

# 4) Basis-Klasse für die ORM-Modelle
Base = declarative_base()


# 5) Beispiel-Tabelle (ORM-Klasse)


class Provider(Base):
    __tablename__ = 'PROVIDER'
    provider_id = Column(Integer, primary_key=True)
    name = Column(String)

# --------------- Interest Rate -----------------------


class Interest_Rate(Base):
    __tablename__ = 'INTEREST_RATE'
    ir_id = Column(Integer, primary_key=True)
    currency = Column(String(3))
    duration = Column(Float)


class IR_Ref(Base):
    __tablename__ = "IR_REF"
    series_id = Column(Integer, primary_key=True)
    provider_id = Column(Integer, ForeignKey('PROVIDER.provider_id'))
    ir_id = Column(Integer, ForeignKey('INTEREST_RATE.ir_id'))


class Interest_Rates_TS(Base):
    __tablename__ = "IR_TS"
    date = Column(Date, primary_key=True)
    rate = Column(Float)
    series_id = Column(Integer, ForeignKey('IR_REF.series_id'), primary_key=True)


# --------------- FX Rates -----------------------

class FX_Rates(Base):
    __tablename__ = 'FX_RATES'
    base_cur = Column(String(3), primary_key=True)
    quote_cur = Column(String(3), primary_key=True)
    duration = Column(String, primary_key=True)


class FX_Ref(Base):
    __tablename__ = "FX_REF"
    series_id = Column(Integer, primary_key=True)
    provider_id = Column(Integer, ForeignKey('PROVIDER.provider_id'))
    base_cur = Column(String(3))
    quote_cur = Column(String(3))
    duration = Column(String)

    __table_args__ = (
        ForeignKeyConstraint(
            ['base_cur', 'quote_cur', 'duration'],
            ['FX_RATES.base_cur', 'FX_RATES.quote_cur', 'FX_RATES.duration']
        ),
    )
class FX_TS(Base):
    __tablename__ = "FX_TS"
    date = Column(Date, primary_key=True)
    rate = Column(Float)
    series_id = Column(Integer, ForeignKey('FX_REF.series_id'), primary_key=True)

# --------------- CPI Rates -----------------------

class CPI_Rates(Base):
    __tablename__ = 'CPI_RATES'
    currency = Column(String(3), primary_key=True)

class CPI_Ref(Base):
    __tablename__ = "CPI_REF"
    series_id = Column(Integer, primary_key=True)
    provider_id = Column(Integer, ForeignKey('PROVIDER.provider_id'))
    currency = Column(String(3), ForeignKey('CPI_RATES.currency'))


class CPI_TS(Base):
    __tablename__ = "CPI_TS"
    date = Column(Date, primary_key=True)
    rate = Column(Float)
    series_id = Column(Integer, ForeignKey('CPI_REF.series_id'), primary_key=True)


# ---------------- Hedging Strategy -----------------------

class Hedging_Strategy(Base):
    __tablename__ = "HEDGING_STRATEGY"
    hedge_id = Column(Integer, primary_key=True)
    name = Column(String)
    version = Column(Float)





class Simulation_Ref(Base):
    __tablename__ = "SIMULATION_REF"
    simulation_id = Column(Integer, primary_key=True)
    hedge_id = Column(Integer, ForeignKey('HEDGING_STRATEGY.hedge_id'))
    portfolio_id = Column(Integer, ForeignKey('PORTFOLIO.portfolio_id'))


class Simulation_TS(Base):
    __tablename__ = "SIMULATION_TS"
    date = Column(Date, primary_key=True)
    simulation_id = Column(Integer, ForeignKey('SIMULATION_REF.simulation_id'), primary_key=True)
    unhedged_growth =Column(Float)
    hedged_growth = Column(Float)




# ------------------ Asset -----------------------



class Asset(Base):
    __tablename__ = 'ASSET'
    asset_ticker = Column(String, primary_key=True, comment= 'balababa')
    currency = Column(String)

class Asset_Ref(Base):
    __tablename__ = "ASSET_REF"
    series_id = Column(Integer, primary_key=True, autoincrement=True)
    provider_id = Column(Integer, ForeignKey('PROVIDER.provider_id'))
    asset_ticker = Column(String, ForeignKey('ASSET.asset_ticker'))

class Asset_TS(Base):
    __tablename__ = "ASSET_TS"
    date = Column(Date, primary_key=True)
    close = Column(Float)
    series_id = Column(Integer, ForeignKey('ASSET_REF.series_id'), primary_key=True)

# ------------------ Portfolio  -----------------------

class portfolio_asset_connection(Base):
    __tablename__ = "PORTFOLIO_ASSET_CONNECTION"
    asset_ticker = Column(String, ForeignKey('ASSET.asset_ticker'),primary_key=True)
    portfolio_id = Column(Integer, ForeignKey('PORTFOLIO.portfolio_id'),primary_key=True)
    weight = Column(Float)







# 6) Tabellen in der DB erstellen (falls noch nicht vorhanden)
Base.metadata.create_all(engine)