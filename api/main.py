from fastapi import FastAPI, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import Optional

from database import get_db
from schemas import FigureCard, FigureDetail, FigureListResponse, FilterOptions, BeliefBase, EraBase
from queries import (
    get_figures, get_figure_by_id, get_figure_beliefs,
    get_figure_eras, get_all_beliefs, get_all_eras, get_filter_options,
    get_era_range_counts
)
from image_service import resolve_image

app = FastAPI(
    title="Church History API",
    description="API for browsing historical figures in church history",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten this to your frontend URL in production
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


@app.get("/figures", response_model=FigureListResponse)
async def list_figures(
    search: Optional[str] = Query(None),
    type_filter: Optional[str] = Query(None, alias="type"),
    century: Optional[str] = Query(None),
    gender: Optional[str] = Query(None),
    denomination: Optional[str] = Query(None),
    belief_id: Optional[int] = Query(None),
    is_martyr: Optional[bool] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(24, ge=1, le=100),
    db: Session = Depends(get_db),
):
    total, rows = get_figures(
        db, search, type_filter, century, gender,
        denomination, belief_id, is_martyr, page, page_size
    )
    results = []
    for row in rows:
        beliefs = get_figure_beliefs(db, row["id"])
        image_url = await resolve_image(row["thumbnail_json"], row["name"])
        results.append(map_row_to_card(row, beliefs, image_url))

    return {"total": total, "page": page, "page_size": page_size, "results": results}


@app.get("/figures/{figure_id}", response_model=FigureDetail)
async def get_figure(figure_id: int, db: Session = Depends(get_db)):
    row = get_figure_by_id(db, figure_id)
    if not row:
        raise HTTPException(status_code=404, detail="Figure not found")

    beliefs = get_figure_beliefs(db, figure_id)
    eras = get_figure_eras(db, figure_id)
    image_url = await resolve_image(row["thumbnail_json"], row["name"])

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
        "father": row["father"],
        "mother": row["mother"],
        "spouse": row["spouse"],
        "children": row["children"],
        "genealogy_notes": row["genealogy_notes"],
        "burial_site": row["burial_site"],
        "external_references": row["external_references"],
        "notes": row["notes"],
        "eras": [
            {"id": e["id"], "name": e["name"], "time_span": e["time_span"]}
            for e in eras
        ],
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
    }