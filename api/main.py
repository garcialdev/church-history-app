from fastapi import FastAPI, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import Optional

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://historyapp.bbs1.net",
        "http://historyapp.bbs1.net",
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
        denomination, belief_id, is_martyr, sort, page, page_size
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
        "eras": [
            {"id": e["id"], "name": e["name"], "time_span": e["time_span"]}
            for e in eras
        ],
    }


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
    }