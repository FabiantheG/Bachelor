
from database.models.base import Base
from database.session import engine  # stellt Verbindung zur Datenbank her
import database.models.provider
import database.models.economic_indicator
import database.models.cpi
import database.models.gdp
import database.models.fx
import database.models.interest_rate
import database.models.asset
import database.models.hedging
import database.models.portfolio

# Alle Tabellen löschen und neu erstellen (Achtung: Daten gehen verloren!)
Base.metadata.drop_all(engine)   # nur falls du alles neu starten willst
Base.metadata.create_all(engine)

print("✅ Datenbanktabellen wurden erfolgreich neu erstellt.")
