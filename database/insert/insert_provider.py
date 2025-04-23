from database.models.provider import PROVIDER
from database.session import session



def insert_new_provider(name: str) -> PROVIDER:
    """
    Inserts a new provider into the PROVIDER table if it does not already exist.

    Parameters:
    -----------
    name : str
        The name of the provider (e.g. 'bloomberg')

    Returns:
    --------
    Provider
        The Provider object from the database, either newly created or already existing.
    """
    with session:
        with session.begin():
            provider = session.query(PROVIDER).filter_by(name=name).first()
            if not provider:
                provider = PROVIDER(name=name)
                session.add(provider)
                session.flush()  # ensures provider_id is available immediately
                print(f"New provider '{name}' added with ID {provider.provider_id}.")
            else:
                print(f"Provider '{name}' already exists with ID {provider.provider_id}.")
            return provider


#insert_new_provider('bloomberg2322')


