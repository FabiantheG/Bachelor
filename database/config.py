
from sqlalchemy import create_engine

# DB-Settings
DATABASE = "GAM_DB"
USER = "postgres"
PASSWORD = "rootroot"
HOST = "localhost"
PORT = "5432"

engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")