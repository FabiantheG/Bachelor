# main.py (in Database/)
from database.models.base import Base  # zentrale Base
from models import Provider #import all models (not just provider) with __init__.py
from database.config import engine # connection with the database
from session import session



# Tabellen erstellen
Base.metadata.create_all(engine)

