from fastapi import FastAPI, Request, Form, Body
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sqlmodel import select

from .model import Tollbooth, TollboothSts, TbImt
from contextlib import asynccontextmanager
from .utils.connector import SessionDep, create_db_and_tables

from typing import Annotated, Any
import logging
import sys


_log = logging.getLogger(__name__)
_log.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
_log.addHandler(handler)


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


# @app.post("/tollbooth/")
# #def create_tollbooth(tollbooth: Tollbooth, session: SessionDep) -> Tollbooth:
# def create_tollbooth(tollbooth_name: Annotated[str, Form()], coords: Annotated[str, Form()], session: SessionDep):
#     tollbooth = Tollbooth(
#         tollbooth_name=tollbooth_name, coords=coords, status="open", state="", place="", lines=0, type="toll" 
#         )
#     session.add(tollbooth)
#     session.commit()
#     session.refresh(tollbooth)
#     return tollbooth


@app.post("/api/tollbooths/")
def fetch_tollbooths(body: Annotated[Any, Body()], session: SessionDep, offset: int=0, limit: int=100):
    param, value = map(str.strip, body["query"].split(":"))
    stm = select(Tollbooth).where(getattr(Tollbooth, param) == value)
    tollbooths = session.exec(stm.offset(offset).limit(limit))
    data = []
    for tb in tollbooths:
        tb_data = tb.online_filled_fields(exclude_fields={"legacy_id"})
        data.append(tb_data)
    return data


@app.post("/api/tollbooths_sts")
def fetch_tollbooths_sts(body: Annotated[Any, Body()], session: SessionDep, offset: int=0, limit=1000):
    stm = select(TollboothSts)
    tollbooths_sts = session.exec(stm.offset(offset).limit(limit))
    data = []
    for tb_sts in tollbooths_sts:
        data.append({
            "tollbooth_id": tb_sts.tollboothsts_id,
            "tollbooth_name": tb_sts.tollbooth_name,
            "way": tb_sts.way,
            "lat": tb_sts.lat,
            "lng": tb_sts.lng,
            "source": TollboothSts.name()
        })
    return data


@app.post("/api/tollbooth_upsert/")
def upsert_tollbooth(tollbooth: Tollbooth, session: SessionDep):
    _log.debug(tollbooth)
    # session.add(tollbooth)
    # session.commit()
    # session.refresh(tollbooth)
    return {"tollbooth_id": tollbooth.tollbooth_id}


@app.post("/api/tollbooths_imt")
def fetch_tb_imt(body: Annotated[Any, Body()], session: SessionDep, offset: int=0, limit=1000):
    param, value = map(str.strip, body["query"].split(":"))
    stm = select(TbImt).where(TbImt.calirepr != "Virtual").where(getattr(TbImt, param) == value)
    tbs = session.exec(stm.offset(offset).limit(limit))
    data = []
    for tb in tbs:
        data.append({
            "tollbooth_id": tb.tollbooth_imt_id,
            "tollbooth_name": tb.tollbooth_name,
            "lat": tb.lat,
            "lon": tb.lon,
            "source": TbImt.name()
        })
    return data


@app.post("/api/empty_data")
def get_tb_data(body: Annotated[Any, Body()]):
    empty_tb_data = {}
    if body.get("source") == "tollbooth":
        empty_tb_data = Tollbooth.online_empty_fields()
    return empty_tb_data
