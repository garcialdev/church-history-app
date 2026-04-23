from fastapi import FastAPI, Depends, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from sqlalchemy.orm import Session
from typing import Optional

limiter = Limiter(key_func=get_remote_address)

from database import get_db
from schemas import FigureCard, FigureDetail, FigureListResponse, FilterOptions, BeliefBase, EraBase
from queries import (
    get_figures, get_figure_by_id, get_figure_beliefs,
    get_figure_eras, get_all_beliefs, get_all_eras, get_filter_options,
    get_era_range_counts, get_random_figure_id, get_related_figures, get_map_figures,
    get_all_figures_for_caching, save_cached_image_url
)
from image_service import resolve_image, parse_nocodb_thumbnail

app = FastAPI(
    title="Church History API",
    description="API for browsing historical figures in church history",
    version="1.0.0",
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://churcharchive.bbs1.net",
        "http://churcharchive.bbs1.net",
        "http://localhost:3003",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def map_row_to_card(row, beliefs, image_url):
    return {
        "id": row["id"],
        "name": row["name"],
        "alternative_names": row["alternative_names"],
        "gender": row["gender"],
        "type": row["type"],
        "role_office": row["role_office"],
        "denomination_tradition": row["denomination_tradition"],
        "born": row["born"],
        "death": row["death"],
        "era_type": row["era_type"],
        "century": row["century"],
        "birthplace": row["birthplace"],
        "primary_region": row["primary_region"],
        "short_description": row["short_description"],
        "is_martyr": row["is_martyr"],
        "believer_saved": row["believer_saved"],
        "image_url": image_url,
        "beliefs": [
            {"id": b["id"], "belief_name": b["belief_name"], "description": b["description"]}
            for b in beliefs
        ],
    }


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/figures/map")
async def get_map_figures_route(db: Session = Depends(get_db)):
    rows = get_map_figures(db)
    results = []
    for row in rows:
        image_url = await resolve_image(row["thumbnail_json"], row["name"], row["wikipedia_name"], row.get("cached_image_url"))
        results.append({
            "id": row["id"],
            "name": row["name"],
            "type": row["type"],
            "role_office": row["role_office"],
            "century": row["century"],
            "birthplace": row["birthplace"],
            "image_url": image_url,
        })
    return results


@app.get("/figures/random")
async def get_random_figure(db: Session = Depends(get_db)):
    figure_id = get_random_figure_id(db)
    if not figure_id:
        raise HTTPException(status_code=404, detail="No figures found")
    row = get_figure_by_id(db, figure_id)
    beliefs = get_figure_beliefs(db, figure_id)
    eras = get_figure_eras(db, figure_id)
    image_url = await resolve_image(row["thumbnail_json"], row["name"], row["wikipedia_name"], row.get("cached_image_url"))
    return {
        **map_row_to_card(row, beliefs, image_url),
        "long_biography": row["long_biography"],
        "famous_quotes": row["famous_quotes"],
        "major_works": row["major_works"],
        "key_life_events": row["key_life_events"],
        "primary_contributions": row["primary_contributions"],
        "scripture_references": row["scripture_references"],
        "biblical_books": row["biblical_books"],
        "associated_movements": row["associated_movements"],
        "external_references": row["external_references"],
        "notes": row["notes"],
        "deathplace": row["deathplace"],
        "eras": [
            {"id": e["id"], "name": e["name"], "time_span": e["time_span"]}
            for e in eras
        ],
    }


@app.get("/figures", response_model=FigureListResponse)
async def list_figures(
    search: Optional[str] = Query(None),
    type_filter: Optional[str] = Query(None, alias="type"),
    century: Optional[str] = Query(None),
    century_keywords: Optional[str] = Query(None),
    gender: Optional[str] = Query(None),
    denomination: Optional[str] = Query(None),
    role_office: Optional[str] = Query(None),
    belief_id: Optional[int] = Query(None),
    is_martyr: Optional[bool] = Query(None),
    sort: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(24, ge=1, le=500),
    db: Session = Depends(get_db),
):
    era_centuries = [k.strip() for k in century_keywords.split(",")] if century_keywords else None
    total, rows = get_figures(
        db, search, type_filter, century, era_centuries, gender,
        denomination, role_office, belief_id, is_martyr, sort, page, page_size
    )
    results = []
    for row in rows:
        beliefs = get_figure_beliefs(db, row["id"])
        image_url = await resolve_image(row["thumbnail_json"], row["name"], row["wikipedia_name"], row.get("cached_image_url"))
        results.append(map_row_to_card(row, beliefs, image_url))

    return {"total": total, "page": page, "page_size": page_size, "results": results}


@app.get("/figures/{figure_id}/related")
async def get_figure_related(figure_id: int, db: Session = Depends(get_db)):
    row = get_figure_by_id(db, figure_id)
    if not row:
        raise HTTPException(status_code=404, detail="Figure not found")
    related = get_related_figures(db, figure_id, row["century"], row["type"])
    results = []
    for r in related:
        image_url = await resolve_image(r["thumbnail_json"], r["name"], r["wikipedia_name"], r.get("cached_image_url"))
        results.append({
            "id": r["id"],
            "name": r["name"],
            "type": r["type"],
            "role_office": r["role_office"],
            "century": r["century"],
            "born": r["born"],
            "death": r["death"],
            "era_type": r["era_type"],
            "short_description": r["short_description"],
            "image_url": image_url,
        })
    return results


@app.get("/figures/{figure_id}", response_model=FigureDetail)
async def get_figure(figure_id: int, db: Session = Depends(get_db)):
    row = get_figure_by_id(db, figure_id)
    if not row:
        raise HTTPException(status_code=404, detail="Figure not found")

    beliefs = get_figure_beliefs(db, figure_id)
    eras = get_figure_eras(db, figure_id)
    image_url = await resolve_image(row["thumbnail_json"], row["name"], row["wikipedia_name"], row.get("cached_image_url"))

    return {
        **map_row_to_card(row, beliefs, image_url),
        "long_biography": row["long_biography"],
        "famous_quotes": row["famous_quotes"],
        "major_works": row["major_works"],
        "key_life_events": row["key_life_events"],
        "primary_contributions": row["primary_contributions"],
        "scripture_references": row["scripture_references"],
        "biblical_books": row["biblical_books"],
        "associated_movements": row["associated_movements"],
        "external_references": row["external_references"],
        "notes": row["notes"],
        "deathplace": row["deathplace"],
        "eras": [
            {"id": e["id"], "name": e["name"], "time_span": e["time_span"]}
            for e in eras
        ],
    }


@app.post("/admin/cache-images")
async def cache_images(db: Session = Depends(get_db)):
    """
    Resolve and cache image URLs for all figures that don't have one yet.
    Call this once after setup, then periodically when you add new figures.
    """
    figures = get_all_figures_for_caching(db)
    updated = 0
    skipped = 0
    failed = 0

    for f in figures:
        # Skip if already has a NocoDB thumbnail or cached URL
        if parse_nocodb_thumbnail(f["thumbnail_json"]):
            skipped += 1
            continue
        if f["cached_image_url"] and f["cached_image_url"].strip():
            skipped += 1
            continue

        # Try to resolve via Wikipedia
        url = await resolve_image(
            f["thumbnail_json"], f["name"],
            f["wikipedia_name"], f["cached_image_url"]
        )
        if url:
            save_cached_image_url(db, f["id"], url)
            updated += 1
        else:
            failed += 1

    return {
        "status": "done",
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "total": len(figures)
    }


@app.get("/era-ranges")
def list_era_ranges(db: Session = Depends(get_db)):
    return get_era_range_counts(db)


@app.get("/beliefs", response_model=list[BeliefBase])
def list_beliefs(db: Session = Depends(get_db)):
    rows = get_all_beliefs(db)
    return [{"id": r["id"], "belief_name": r["belief_name"], "description": r["description"]} for r in rows]


@app.get("/eras", response_model=list[EraBase])
def list_eras(db: Session = Depends(get_db)):
    rows = get_all_eras(db)
    return [{"id": r["id"], "name": r["name"], "time_span": r["time_span"]} for r in rows]


@app.get("/filters", response_model=FilterOptions)
def list_filter_options(db: Session = Depends(get_db)):
    opts = get_filter_options(db)
    beliefs = get_all_beliefs(db)
    eras = get_all_eras(db)
    return {
        **opts,
        "beliefs": [{"id": b["id"], "belief_name": b["belief_name"], "description": b["description"]} for b in beliefs],
        "eras": [{"id": e["id"], "name": e["name"], "time_span": e["time_span"]} for e in eras],
        "role_offices": opts["role_offices"],
    }


# ── ADMIN ROUTES ──────────────────────────────────────────────────────────────

from fastapi import Header
from fastapi.responses import JSONResponse
from config import ADMIN_PASSWORD, create_token, validate_token, revoke_token
from queries import (
    admin_get_all_figures, admin_get_figure, admin_create_figure,
    admin_update_figure, admin_delete_figure, admin_get_stats
)
from pydantic import BaseModel
from typing import Any, Dict

def require_admin(x_admin_token: str = Header(...)):
    if not validate_token(x_admin_token):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return x_admin_token


class LoginRequest(BaseModel):
    password: str

class FigurePayload(BaseModel):
    name: str
    type: Optional[str] = None
    gender: Optional[str] = None
    century: Optional[str] = None
    born: Optional[int] = None
    death: Optional[int] = None
    era_type: Optional[str] = None
    role_office: Optional[str] = None
    denomination: Optional[str] = None
    alternative_names: Optional[str] = None
    short_description: Optional[str] = None
    long_biography: Optional[str] = None
    famous_quotes: Optional[str] = None
    major_works: Optional[str] = None
    key_life_events: Optional[str] = None
    primary_contributions: Optional[str] = None
    scripture_references: Optional[str] = None
    biblical_books: Optional[str] = None
    associated_movements: Optional[str] = None
    external_references: Optional[str] = None
    notes: Optional[str] = None
    birthplace: Optional[str] = None
    deathplace: Optional[str] = None
    primary_region: Optional[str] = None
    wikipedia_name: Optional[str] = None
    cached_image_url: Optional[str] = None
    is_martyr: Optional[str] = None
    believer_saved: Optional[str] = None


@app.post("/admin/login")
@limiter.limit("5/minute")
def admin_login(request: Request, req: LoginRequest):
    if req.password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    return {"token": create_token()}


@app.post("/admin/logout")
def admin_logout(token: str = Depends(require_admin)):
    revoke_token(token)
    return {"status": "logged out"}


@app.get("/admin/stats")
def admin_stats(db: Session = Depends(get_db), _=Depends(require_admin)):
    return admin_get_stats(db)


@app.get("/admin/figures")
def admin_list_figures(db: Session = Depends(get_db), _=Depends(require_admin)):
    rows = admin_get_all_figures(db)
    return [dict(r) for r in rows]


@app.get("/admin/figures/{figure_id}")
def admin_get_figure_detail(figure_id: int, db: Session = Depends(get_db), _=Depends(require_admin)):
    row = admin_get_figure(db, figure_id)
    if not row:
        raise HTTPException(status_code=404, detail="Not found")
    return dict(row)


@app.post("/admin/figures")
def admin_create(payload: FigurePayload, db: Session = Depends(get_db), _=Depends(require_admin)):
    new_id = admin_create_figure(db, payload.model_dump())
    return {"id": new_id, "status": "created"}


@app.put("/admin/figures/{figure_id}")
def admin_update(figure_id: int, payload: FigurePayload, db: Session = Depends(get_db), _=Depends(require_admin)):
    admin_update_figure(db, figure_id, payload.model_dump())
    return {"status": "updated"}


@app.delete("/admin/figures/{figure_id}")
def admin_delete(figure_id: int, db: Session = Depends(get_db), _=Depends(require_admin)):
    admin_delete_figure(db, figure_id)
    return {"status": "deleted"}


# ── ADMIN BELIEFS ─────────────────────────────────────────────────────────────

from queries import (
    admin_get_beliefs, admin_get_figure_belief_ids,
    admin_set_figure_beliefs, admin_create_belief, admin_update_belief, admin_delete_belief
)

class BeliefCreatePayload(BaseModel):
    name: str
    description: Optional[str] = None

class FigureBeliefsPayload(BaseModel):
    belief_ids: list[int]

@app.get("/admin/beliefs")
def admin_list_beliefs(db: Session = Depends(get_db), _=Depends(require_admin)):
    rows = admin_get_beliefs(db)
    return [dict(r) for r in rows]

@app.get("/admin/figures/{figure_id}/beliefs")
def admin_figure_beliefs(figure_id: int, db: Session = Depends(get_db), _=Depends(require_admin)):
    return admin_get_figure_belief_ids(db, figure_id)

@app.put("/admin/figures/{figure_id}/beliefs")
def admin_update_figure_beliefs(figure_id: int, payload: FigureBeliefsPayload, db: Session = Depends(get_db), _=Depends(require_admin)):
    admin_set_figure_beliefs(db, figure_id, payload.belief_ids)
    return {"status": "updated"}

@app.post("/admin/beliefs")
def admin_create_belief_route(payload: BeliefCreatePayload, db: Session = Depends(get_db), _=Depends(require_admin)):
    new_id = admin_create_belief(db, payload.name, payload.description or "")
    return {"id": new_id, "status": "created"}

@app.put("/admin/beliefs/{belief_id}")
def admin_update_belief_route(belief_id: int, payload: BeliefCreatePayload, db: Session = Depends(get_db), _=Depends(require_admin)):
    admin_update_belief(db, belief_id, payload.name, payload.description or "")
    return {"status": "updated"}

@app.delete("/admin/beliefs/{belief_id}")
def admin_delete_belief_route(belief_id: int, db: Session = Depends(get_db), _=Depends(require_admin)):
    admin_delete_belief(db, belief_id)
    return {"status": "deleted"}