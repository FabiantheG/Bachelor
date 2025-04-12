
# creat_table_einmalig.py

from database.models.base import Base
from database.session import engine
import database.models  # sorgt dafür, dass alle Tabellen-Klassen registriert sind

if __name__ == "__main__":
    print("Erstelle Tabellen falls nicht vorhanden...")
    Base.metadata.create_all(bind=engine)
    print("Tabellen erfolgreich erstellt oder bereits vorhanden.")
