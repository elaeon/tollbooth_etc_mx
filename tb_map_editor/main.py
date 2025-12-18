from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .model import Tollbooth
from contextlib import asynccontextmanager
from .utils.connector import SessionDep, engine

from sqlmodel import SQLModel


def create_db_and_tables():
    print(SQLModel.metadata.tables, "M")
    SQLModel.metadata.create_all(engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static", follow_symlink=True), name="static")

templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
def map_root(request: Request):
    return templates.TemplateResponse(
        request=request, name="map.html"
    )


@app.post("/tollbooth/")
def create_tollbooth(tollbooth: Tollbooth, session: SessionDep) -> Tollbooth:
    session.add(tollbooth)
    session.commit()
    session.refresh(tollbooth)
    return tollbooth

