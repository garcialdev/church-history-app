from fastapi import FastAPI, Depends, Query, HTTPException, Request, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from sqlalchemy.orm import Session
from typing import Optional
import os, shutil, time, pathlib, asyncio

limiter = Limiter(key_func=get_remote_address)

from database import get_db
from schemas import FigureCard, FigureDetail, FigureListResponse, FilterOptions, BeliefBase, EraBase
from queries import (
    get_figures, get_figure_by_id, get_figure_beliefs, get_bulk_beliefs,
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

@app.middleware("http")
async def add_private_network_access_header(request: Request, call_next):
    # Chrome sends a preflight with Access-Control-Request-Private-Network on local-network requests.
    # We must echo the permission header on both the preflight (OPTIONS) and the real response.
    if request.method == "OPTIONS":
        from fastapi.responses import Response as FastResponse
        resp = FastResponse(status_code=204)
        resp.headers["Access-Control-Allow-Origin"] = request.headers.get("origin", "*")
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "*"
        resp.headers["Access-Control-Allow-Private-Network"] = "true"
        resp.headers["Access-Control-Max-Age"] = "86400"
        return resp
    response = await call_next(request)
    response.headers["Access-Control-Allow-Private-Network"] = "true"
    return response

UPLOADS_DIR = pathlib.Path("/app/uploads")
UPLOADS_DIR.mkdir(exist_ok=True)
PUBLIC_API_URL = os.environ.get("PUBLIC_API_URL", "https://churcharchiveapi.bbs1.net")
app.mount("/uploads", StaticFiles(directory=str(UPLOADS_DIR)), name="uploads")


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
            "born": row["born"],
            "death": row["death"],
            "era_type": row["era_type"],
            "birthplace": row["birthplace"],
            "primary_region": row.get("primary_region"),
            "denomination_tradition": row.get("denomination_tradition"),
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
        "image_credit": row.get("image_credit"),
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
    # Single query for all beliefs, parallel resolution for all images
    beliefs_map = get_bulk_beliefs(db, [r["id"] for r in rows])
    image_urls = await asyncio.gather(*[
        resolve_image(r["thumbnail_json"], r["name"], r["wikipedia_name"], r.get("cached_image_url"))
        for r in rows
    ])
    results = [
        map_row_to_card(row, beliefs_map.get(row["id"], []), image_url)
        for row, image_url in zip(rows, image_urls)
    ]
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
        "image_credit": row.get("image_credit"),
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

import csv, io, base64, datetime
from fastapi import Header
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse
from config import ADMIN_PASSWORD, create_token, validate_token, revoke_token
from queries import (
    admin_get_all_figures, admin_get_figure, admin_create_figure,
    admin_update_figure, admin_delete_figure, admin_get_stats, clear_figure_image,
    get_all_beliefs_grouped,
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
    image_credit: Optional[str] = None
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


@app.post("/admin/upload-image")
async def upload_image(
    figure_id: int = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _=Depends(require_admin),
):
    ext = pathlib.Path(file.filename).suffix.lower() or ".jpg"
    filename = f"{figure_id}_{int(time.time())}{ext}"
    dest = UPLOADS_DIR / filename
    with dest.open("wb") as buf:
        shutil.copyfileobj(file.file, buf)
    url = f"{PUBLIC_API_URL}/uploads/{filename}"
    # Save new URL and clear any legacy NocoDB thumbnail so cached_url wins
    clear_figure_image(db, figure_id)
    save_cached_image_url(db, figure_id, url)
    return {"image_url": url}


@app.delete("/admin/figures/{figure_id}/image")
def delete_figure_image(figure_id: int, db: Session = Depends(get_db), _=Depends(require_admin)):
    clear_figure_image(db, figure_id)
    return {"status": "cleared"}


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

ALL_EXPORT_COLUMNS = [
    "id", "name", "type", "gender", "century", "born", "death",
    "era_type", "role_office", "denomination_tradition",
    "short_description", "alternative_names", "birthplace",
    "primary_region", "is_martyr", "believer_saved",
    "wikipedia_name", "cached_image_url", "beliefs",
]

@app.get("/admin/export/csv")
def export_figures_csv(
    fields: Optional[str] = None,
    db: Session = Depends(get_db),
    _=Depends(require_admin),
):
    figures = admin_get_all_figures(db)
    needs_beliefs = fields is None or "beliefs" in fields.split(",")
    beliefs_map = get_all_beliefs_grouped(db) if needs_beliefs else {}

    if fields:
        requested = [f.strip() for f in fields.split(",")]
        columns = [c for c in ALL_EXPORT_COLUMNS if c in requested]
    else:
        columns = ALL_EXPORT_COLUMNS

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for f in figures:
        row = dict(f)
        if "beliefs" in columns:
            row["beliefs"] = "; ".join(beliefs_map.get(f["id"], []))
        writer.writerow(row)
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=ecclesia_export.csv"},
    )


def _esc(s) -> str:
    return (str(s or "")
        .replace("&", "&amp;").replace("<", "&lt;")
        .replace(">", "&gt;").replace('"', "&quot;"))


def _image_src(url: str) -> str:
    if not url:
        return ""
    if "/uploads/" in url:
        filepath = UPLOADS_DIR / url.split("/uploads/")[-1]
        if filepath.exists():
            ext = filepath.suffix.lstrip(".").lower()
            mime = "image/jpeg" if ext in ("jpg", "jpeg") else f"image/{ext}"
            try:
                return f"data:{mime};base64,{base64.b64encode(filepath.read_bytes()).decode()}"
            except Exception:
                pass
    return url


@app.get("/admin/export/report", response_class=HTMLResponse)
def export_figures_report(
    fields: Optional[str] = None,
    db: Session = Depends(get_db),
    _=Depends(require_admin),
):
    figures = list(admin_get_all_figures(db))
    beliefs_map = get_all_beliefs_grouped(db)

    requested = set(fields.split(",")) if fields else set(ALL_EXPORT_COLUMNS)
    requested.add("name")

    include_img = "cached_image_url" in requested
    today = datetime.date.today().strftime("%B %d, %Y")
    total = len(figures)

    cards = []
    for f in figures:
        name = _esc(f.get("name") or "—")
        ftype = f.get("type") or ""
        bcls = ftype.lower() if ftype in ("Person", "Event", "Group") else "x"

        src = _image_src(f["cached_image_url"]) if include_img and f.get("cached_image_url") else ""
        initial = (f.get("name") or "?")[0].upper()
        img_html = (f'<div class="ci"><img src="{src}" alt="" /></div>' if src
                    else f'<div class="ci cin"><span>{initial}</span></div>')

        meta = []
        if "century" in requested and f.get("century"):
            meta.append(_esc(f["century"]) + " cent.")
        if "born" in requested and f.get("born"):
            meta.append(f"b.&thinsp;{f['born']}")
        if "death" in requested and f.get("death"):
            meta.append(f"d.&thinsp;{f['death']}")

        meta_inner = (f'<span class="b b-{bcls}">{_esc(ftype)}</span>' if "type" in requested and ftype else "")
        meta_inner += (f'<span class="cd">{" &middot; ".join(meta)}</span>' if meta else "")
        meta_block = f'<div class="cm">{meta_inner}</div>' if meta_inner else ""

        role   = f'<div class="cr">{_esc(f["role_office"])}</div>'              if "role_office"            in requested and f.get("role_office")            else ""
        denom  = f'<div class="cde">{_esc(f["denomination_tradition"])}</div>'  if "denomination_tradition" in requested and f.get("denomination_tradition") else ""
        desc   = f'<p class="cdesc">{_esc(f["short_description"])}</p>'         if "short_description"      in requested and f.get("short_description")      else ""

        beliefs_list = beliefs_map.get(f["id"], [])
        tags   = (f'<div class="ctags">{"".join(f"<span class=tag>{_esc(b)}</span>" for b in beliefs_list)}</div>'
                  if "beliefs" in requested and beliefs_list else "")

        flags = ""
        if "is_martyr" in requested and f.get("is_martyr") == "Yes":
            flags += '<span class="fl flm">Martyr</span>'
        if "believer_saved" in requested and f.get("believer_saved") == "Yes":
            flags += '<span class="fl fls">Saved</span>'
        flags = f'<div class="cfl">{flags}</div>' if flags else ""

        cards.append(f"""<div class="card">{img_html}<div class="cb"><div class="ch"><h2 class="cn">{name}</h2>{meta_block}</div>{role}{denom}{desc}{tags}{flags}</div></div>""")

    return HTMLResponse(content=_build_report_html(today, total, "\n".join(cards)))


def _build_report_html(today: str, total: int, cards_html: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>The Archivist — Ecclesia Report</title>
<link rel="preconnect" href="https://fonts.googleapis.com" />
<link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,400;0,600;0,700;1,400;1,600&family=EB+Garamond:ital,wght@0,400;0,500;1,400&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet" />
<style>
:root{{--bg:#08080d;--s:#13131f;--s2:#1a1a2a;--bdr:#2a2a42;--bdr2:#1e1e32;--ink:#e8e4f0;--ink2:#b0aac8;--ink3:#706a88;--acc:#d4903a;--red:#c0392b;--grn:#5a9a6a;--blu:#6db3d4;}}
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0;}}
html{{background:var(--bg);color:var(--ink);font-family:'EB Garamond',Georgia,serif;font-size:15px;}}
.topbar{{background:var(--s);border-bottom:1px solid var(--bdr);padding:.55rem 1.5rem;display:flex;align-items:center;gap:.75rem;position:sticky;top:0;z-index:10;}}
.topbar-note{{font-family:'DM Mono',monospace;font-size:.58rem;text-transform:uppercase;letter-spacing:.1em;color:var(--ink3);margin-right:auto;}}
.btn{{font-family:'DM Mono',monospace;font-size:.65rem;text-transform:uppercase;letter-spacing:.08em;padding:.42rem 1rem;border:none;cursor:pointer;border-radius:2px;}}
.btn-print{{background:var(--acc);color:#1a0e00;}}
.btn-print:hover{{background:#e8a050;}}
.hdr{{padding:2rem 2.5rem 1.75rem;border-bottom:1px solid var(--bdr);display:flex;align-items:flex-end;justify-content:space-between;flex-wrap:wrap;gap:1rem;background:linear-gradient(180deg,#0a0a18,var(--bg));}}
.hdr-logo{{font-family:'Cormorant Garamond',serif;font-size:2.1rem;font-style:italic;font-weight:600;color:var(--ink);line-height:1;}}
.hdr-div{{color:var(--acc);font-size:.65rem;letter-spacing:.25em;display:block;margin:.25rem 0;}}
.hdr-sub{{font-family:'DM Mono',monospace;font-size:.55rem;text-transform:uppercase;letter-spacing:.2em;color:var(--ink3);}}
.hdr-meta{{font-family:'DM Mono',monospace;font-size:.58rem;text-align:right;color:var(--ink3);line-height:1.9;}}
.hdr-meta strong{{color:var(--acc);}}
.body{{padding:1.75rem 2rem;}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(290px,1fr));gap:1rem;}}
.card{{background:var(--s);border:1px solid var(--bdr);display:flex;overflow:hidden;transition:border-color .18s;}}
.card:hover{{border-color:var(--acc);}}
.ci{{width:86px;flex-shrink:0;background:var(--s2);overflow:hidden;display:flex;align-items:center;justify-content:center;align-self:stretch;}}
.ci img{{width:100%;height:100%;object-fit:cover;display:block;}}
.cin{{font-family:'Cormorant Garamond',serif;font-size:2.2rem;font-style:italic;font-weight:600;color:var(--ink3);}}
.cb{{flex:1;padding:.8rem .95rem;display:flex;flex-direction:column;gap:.28rem;min-width:0;}}
.ch{{display:flex;flex-direction:column;gap:.18rem;}}
.cn{{font-family:'Cormorant Garamond',serif;font-size:1.08rem;font-weight:600;color:var(--ink);line-height:1.25;}}
.cm{{display:flex;align-items:center;flex-wrap:wrap;gap:.35rem;}}
.b{{font-family:'DM Mono',monospace;font-size:.52rem;padding:.1rem .42rem;letter-spacing:.06em;text-transform:uppercase;}}
.b-person{{background:rgba(109,179,212,.13);color:var(--blu);}}
.b-event{{background:rgba(212,144,58,.12);color:var(--acc);}}
.b-group{{background:rgba(90,154,106,.13);color:var(--grn);}}
.b-x{{background:var(--s2);color:var(--ink3);}}
.cd{{font-family:'DM Mono',monospace;font-size:.56rem;color:var(--ink3);}}
.cr{{font-family:'DM Mono',monospace;font-size:.58rem;color:var(--acc);text-transform:uppercase;letter-spacing:.06em;}}
.cde{{font-family:'DM Mono',monospace;font-size:.56rem;color:var(--ink3);}}
.cdesc{{font-size:.87rem;color:var(--ink2);line-height:1.55;flex:1;}}
.ctags{{display:flex;flex-wrap:wrap;gap:.22rem;margin-top:auto;padding-top:.2rem;}}
.tag{{background:var(--s2);border:1px solid var(--bdr2);color:var(--ink3);font-family:'DM Mono',monospace;font-size:.52rem;padding:.08rem .38rem;letter-spacing:.04em;}}
.cfl{{display:flex;gap:.28rem;flex-wrap:wrap;}}
.fl{{font-family:'DM Mono',monospace;font-size:.52rem;padding:.1rem .42rem;text-transform:uppercase;letter-spacing:.06em;}}
.flm{{background:rgba(192,57,43,.14);color:var(--red);}}
.fls{{background:rgba(90,154,106,.14);color:var(--grn);}}
.ftr{{text-align:center;padding:1.75rem;border-top:1px solid var(--bdr);font-family:'DM Mono',monospace;font-size:.55rem;color:var(--ink3);letter-spacing:.1em;text-transform:uppercase;margin-top:1rem;}}

@media(max-width:600px){{.grid{{grid-template-columns:1fr;}}.hdr{{padding:1.25rem 1rem;}}.body{{padding:1rem;}}}}

@media print{{
  .topbar{{display:none!important;}}
  html,body{{background:#faf8f3;color:#1e1810;font-size:13px;}}
  .hdr{{background:#faf8f3;border-bottom:2px solid #c9b898;padding:1.2rem 1.5rem .9rem;}}
  .hdr-logo{{color:#1e1810;font-size:1.75rem;}}
  .hdr-div{{color:#a07840;}}
  .hdr-sub,.hdr-meta{{color:#7a6850;}}
  .hdr-meta strong{{color:#8a5c20;}}
  .body{{padding:1rem 1.25rem;}}
  .grid{{grid-template-columns:repeat(2,1fr);gap:.65rem;}}
  .card{{background:#fff;border:1px solid #d8cfc0;break-inside:avoid;}}
  .ci{{background:#f0ece4;}}
  .cin{{color:#c8b89a;}}
  .cn{{color:#1e1810;}}
  .cd{{color:#7a6850;}}
  .cr{{color:#8a5c20;}}
  .cde{{color:#7a6850;}}
  .cdesc{{color:#3a3028;}}
  .b-person{{background:#daeef7;color:#3a7a98;}}
  .b-event{{background:#fbecd8;color:#8a5c20;}}
  .b-group{{background:#dff0e4;color:#3a6a48;}}
  .b-x{{background:#f0ece4;color:#7a6850;}}
  .tag{{background:#f0ece4;border-color:#d8cfc0;color:#7a6850;}}
  .flm{{background:#fde8e5;color:#8a2018;}}
  .fls{{background:#dff0e4;color:#3a6a48;}}
  .ftr{{color:#7a6850;border-color:#d8cfc0;}}
  @page{{margin:1.25cm 1.5cm;}}
}}
</style>
</head>
<body>
<div class="topbar">
  <span class="topbar-note">The Archivist &mdash; Visual Report &mdash; {total} records</span>
  <button class="btn btn-print" onclick="window.print()">&#128438; Print &frasl; Save as PDF</button>
</div>
<div class="hdr">
  <div>
    <div class="hdr-logo">The Archivist</div>
    <span class="hdr-div">&mdash; &#10022; &mdash;</span>
    <div class="hdr-sub">Ecclesia Visual Report</div>
  </div>
  <div class="hdr-meta">
    <div>Generated <strong>{today}</strong></div>
    <div><strong>{total}</strong> records</div>
  </div>
</div>
<div class="body">
  <div class="grid">
    {cards_html}
  </div>
</div>
<div class="ftr">The Archivist &mdash; Ecclesiastical Archive &mdash; {today}</div>
</body>
</html>"""