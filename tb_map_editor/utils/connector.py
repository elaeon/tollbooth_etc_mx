from sqlmodel import Session, create_engine
from typing import Annotated
from sqlmodel import Session
from fastapi import Depends


_sql_filename = "tb_map_editor.db"
_sqlite_url = f"sqlite:///{_sql_filename}"

_connect_args = {"check_same_thread": False}
engine = create_engine(_sqlite_url, connect_args=_connect_args)


def get_session():
    with Session(engine) as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]
