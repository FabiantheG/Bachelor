from database.models import *
from database.session import engine  # connection to database

delete = True
create = True

if delete == True:
    Base.metadata.drop_all(engine)
    print("✅ Database has been deleted.")

if create == True:
    Base.metadata.create_all(engine)
    print("✅ Database has been created.")


