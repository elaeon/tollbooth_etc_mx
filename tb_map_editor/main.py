from fastapi import FastAPI, Request, Body, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sqlmodel import select, or_, join, not_

from .model import Tollbooth, TbSts, TbImt, TbStretchId, Stretch, StretchToll, TbNeighbour
from contextlib import asynccontextmanager
from .utils.connector import SessionDep, create_db_and_tables
from .utils.query_parser import parse_query

from typing import Annotated, Any
import logging
import sys
import polars as pl
import polars_h3 as plh3
import datetime


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
    query_endpoints = {
        "tb": "fetch_tollbooths",
        "tbsts": "fetch_tollbooths_sts",
        "tbimt": "fetch_tollbooths_imt"
    }
    return templates.TemplateResponse(
        request=request, name="map.html", context={"query_endpoints": query_endpoints}
    )


@app.post("/api/tollbooths/")
def fetch_tollbooths(body: Annotated[Any, Body()], session: SessionDep, offset: int=0, limit: int=1000):
    if body.get("suggestions", False) is True:
        stm = select(Tollbooth).where(Tollbooth.tollbooth_name.ilike(f"%{body['query']}%"))
        tollbooths = session.exec(stm)
        data = []
        for row in tollbooths:
            data.append(row)
        print(data)
    else:
        try:
            parsed = parse_query(body["query"])
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        if parsed["param"] == "empty_stretch":
            # Find tollbooths not in either TbStretchId.tollbooth_id_in or tollbooth_id_out
            subquery_in = select(TbStretchId.tollbooth_id_in)
            subquery_out = select(TbStretchId.tollbooth_id_out)
            stm = select(Tollbooth).where(
                not_(Tollbooth.tollbooth_id.in_(subquery_in)),
                not_(Tollbooth.tollbooth_id.in_(subquery_out))
            )
            tollbooths = session.exec(stm)
            data = []
            for tb in tollbooths:
                tb_data = tb.online_filled_fields(exclude_fields={"legacy_id"})
                data.append(tb_data)
        else:
            param = parsed["param"]
            values = parsed.get("values", [])
            if param in ["id", "name"]:
                param = f"tollbooth_{param}"
            if len(values) > 1:
                params = []
                for value in values:
                    params.append(getattr(Tollbooth, param) == value)
                stm = select(Tollbooth).where(or_(*params))
                tollbooths = session.exec(stm.offset(offset).limit(limit))
            else:
                if values[0] == "all":
                    stm = select(Tollbooth)
                    tollbooths = session.exec(stm)
                else:
                    stm = select(Tollbooth).where(getattr(Tollbooth, param) == values[0])
                    tollbooths = session.exec(stm.offset(offset).limit(limit))
            data = []
            for tb in tollbooths:
                tb_data = tb.online_filled_fields(exclude_fields={"legacy_id"})
                data.append(tb_data)
    return data


@app.post("/api/tollbooths_sts")
def fetch_tollbooths_sts(body: Annotated[Any, Body()], session: SessionDep, offset: int=0, limit=1000):
    if body["query"]:
        try:
            parsed = parse_query(body["query"])
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        stm = select(TbSts)
        param = parsed.get("param")
        if param in ["id"]:
            param = f"tollbooth_{param}"
        values = parsed.get("values", [])
        if len(values) > 1:
            params = []
            for value in values:
                params.append(getattr(TbSts, param) == value)
            stm = stm.where(or_(*params))
        else:
            stm = stm.where(getattr(TbSts, param) == values[0])
    else:
        stm = select(TbSts)
    tollbooths_sts = session.exec(stm.offset(offset).limit(limit))
    data = []
    for tb_sts in tollbooths_sts:
        data.append({
            "tollbooth_id": tb_sts.index,
            "tollbooth_name": tb_sts.tollbooth_name,
            "stretch_name": tb_sts.stretch_name,
            "lat": tb_sts.lat,
            "lng": tb_sts.lng,
            "info_year": tb_sts.info_year,
            "source": TbSts.name()
        })
    return data


@app.post("/api/tollbooth_upsert/")
def upsert_tollbooth(tollbooth: Tollbooth, session: SessionDep):
    _log.debug(tollbooth)
    if tollbooth.tollbooth_id is not None:
        db_tb = session.get(Tollbooth, tollbooth.tollbooth_id)
        if not db_tb:
            raise HTTPException(status_code=404, detail="Tollbooth not found")
        tb_data = tollbooth.model_dump(exclude_unset=True) 
        db_tb.sqlmodel_update(tb_data)
        session.add(db_tb)
        session.commit()
        session.refresh(db_tb)
    else:
        info_year = datetime.date.today().year
        tollbooth.info_year = info_year
        session.add(tollbooth)
        session.commit()
        session.refresh(tollbooth)
    
    return {"tollbooth_id": tollbooth.tollbooth_id}


@app.post("/api/tollbooths_imt")
def fetch_tollbooths_imt(body: Annotated[Any, Body()], session: SessionDep, offset: int=0, limit=1000):
    if body["query"]:
        try:
            parsed = parse_query(body["query"])
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        param = parsed.get("param")
        values = parsed.get("values", [])
        stm = select(TbImt)#.where(TbImt.calirepr != "Virtual")
        if param in ["id"]:
            param = f"tollbooth_{param}"
        if len(values) > 1:
            params = []
            for value in values:
                params.append(getattr(TbImt, param) == value)
            stm = stm.where(or_(*params))
        else:
            stm = stm.where(getattr(TbImt, param) == values[0])
    else:
        stm = select(TbImt).where(TbImt.calirepr != "Virtual")
    tbs = session.exec(stm.offset(offset).limit(limit))
    data = []
    for tb in tbs:
        data.append({
            "tollbooth_id": tb.tollbooth_id,
            "tollbooth_name": tb.tollbooth_name,
            "calirepr": tb.calirepr,
            "area": tb.area,
            "subarea": tb.subarea,
            "lat": tb.lat,
            "lng": tb.lng,
            "source": TbImt.name()
        })
    return data


@app.post("/api/empty_data")
def get_tb_tpl(body: Annotated[Any, Body()]):
    empty_tb_data = {}
    if body.get("source") == "tollbooth":
        empty_tb_data = Tollbooth.online_empty_fields()
    return empty_tb_data


@app.post("/api/query_tollbooths")
def query_tollbooths(body: Annotated[Any, Body()], session: SessionDep, offset: int=0, limit=20):
    print(body)
    params = [
        TbStretchId.tollbooth_id_in == body.get("tollbooth_id"),
        TbStretchId.tollbooth_id_out == body.get("tollbooth_id")
    ]
    stm = select(TbStretchId, Stretch, StretchToll).join(TbStretchId).join(StretchToll, isouter=True).where(or_(*params))
    tb_stretch = session.exec(stm.offset(offset).limit(limit))
    data = []
    for tb_st, stretch, stretch_toll in tb_stretch:
        tolls = stretch_toll.get_not_null_fields()
        fields = {
            "stretch_id": stretch.stretch_id,
            "stretch_name": stretch.stretch_name,
            "tollbooth_id_in": tb_st.tollbooth_id_in,
            "tollbooth_id_out": tb_st.tollbooth_id_out
        }
        fields.update(tolls)
        data.append(fields)
    return data


@app.post("/api/tollbooth_neightbours")
def tollbooth_neighbours(body: Annotated[Any, Body()], session: SessionDep, offset: int=0, limit=20):
    stm = select(Tollbooth).select_from(
            join(Tollbooth, TbNeighbour, TbNeighbour.neighbour_id == Tollbooth.tollbooth_id)
        ).where(
        TbNeighbour.tollbooth_id == body.get("tollbooth_id"),
        TbNeighbour.scope == 'local-local'
    )
    tollbooths = session.exec(stm.offset(offset).limit(limit))
    data = []
    for tb in tollbooths:
        data.append(tb)
    return data
    
