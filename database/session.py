from sqlalchemy.orm import sessionmaker
from database.config import engine

Session = sessionmaker(bind=engine)
session = Session()