
from sqlalchemy import Column, Integer, String,Float,Date,ForeignKey
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
class Currency(Base):
    __tablename__ = "currency"
    date = Column(Date)

class Infaltion(Base):
    __tablename__ = 'infaltion'
    date = Column(Date)

class InterestRate(Base):
    __tablename__ = 'interest_rate'
    date = Column(Date)
    rate = Column(Float)

class Forex(Base):
    __tablename__ = 'forex'
    date = Column(Date)


# 6) Tabellen in der DB erstellen (falls noch nicht vorhanden)
Base.metadata.create_all(engine)