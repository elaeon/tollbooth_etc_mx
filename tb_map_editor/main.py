from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .model import Tollbooth
from contextlib import asynccontextmanager
from .utils.connector import SessionDep, create_db_and_tables

from typing import Annotated


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
#def create_tollbooth(tollbooth: Tollbooth, session: SessionDep) -> Tollbooth:
def create_tollbooth(tollbooth_name: Annotated[str, Form()], coords: Annotated[str, Form()], session: SessionDep):
    tollbooth = Tollbooth(
        tollbooth_name=tollbooth_name, coords=coords, status="open", state="", place="", lines=0, type="toll" 
        )
    session.add(tollbooth)
    session.commit()
    session.refresh(tollbooth)
    return tollbooth


@app.get("/tollbooth/")
def show_tb(request: Request):
    return templates.TemplateResponse(
        request=request, name="tb.html"
    )
