from sqlmodel import Session, create_engine, SQLModel
from typing import Annotated
from sqlmodel import Session
from fastapi import Depends

import os


def _get_db_project_dir():
    project_base_dir = "tollbooth_etc_mx/tb_map_editor"
    filepath = os.path.abspath(os.path.dirname(__file__))
    index = filepath.find(project_base_dir)
    db_dir = os.path.join(filepath[:index], project_base_dir, "db/")
    return db_dir


_db_dir = _get_db_project_dir()
_sql_filename = "tb_map_editor.db"

_sql_filepath = os.path.join(_db_dir, _sql_filename)
sqlite_url = f"sqlite:///{_sql_filepath}"

_connect_args = {"check_same_thread": False}
_engine = create_engine(sqlite_url, connect_args=_connect_args)


def create_db_and_tables():
    SQLModel.metadata.create_all(_engine)


def get_session():
    with Session(_engine) as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]
