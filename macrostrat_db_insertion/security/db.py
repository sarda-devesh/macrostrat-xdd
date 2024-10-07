import datetime

from sqlalchemy import select, update

from macrostrat_db_insertion.database import get_session_maker, get_engine
from macrostrat_db_insertion.security.schema import Token


def get_access_token(token: str):
    """The sole database call """

    session_maker = get_session_maker()
    with session_maker() as session:

        select_stmt = select(Token).where(Token.token == token)

        # Check that the token exists
        result = (session.scalars(select_stmt)).first()

        # Check if it has expired
        if result.expires_on < datetime.datetime.now(datetime.timezone.utc):
            return None

        # Update the used_on column
        if result is not None:
            stmt = update(Token).where(Token.token == token).values(used_on=datetime.datetime.utcnow())
            session.execute(stmt)
            session.commit()

        return (session.scalars(select_stmt)).first()
