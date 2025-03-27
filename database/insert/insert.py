

from database.session import session
from database.models import *

prov = Provider(name = 'yahoo',provider_id = 9997)
session.add(prov)
session.commit()

