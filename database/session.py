from sqlalchemy.orm import sessionmaker
from config import engine

Session = sessionmaker(bind=engine)
session = Session()