from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional


def get_figures(
    db: Session,
    search: Optional[str] = None,
    type_filter: Optional[str] = None,
    century: Optional[str] = None,
    era_centuries: Optional[list] = None,
    gender: Optional[str] = None,
    denomination: Optional[str] = None,
    role_office: Optional[str] = None,
    belief_id: Optional[int] = None,
    is_martyr: Optional[bool] = None,
    sort: Optional[str] = None,
    page: int = 1,
    page_size: int = 24,
):
    offset = (page - 1) * page_size
    where_clauses = ["1=1"]
    params = {"limit": page_size, "offset": offset}

    if search:
        where_clauses.append(
            '(ch."Name_Event" ILIKE :search '
            'OR ch."Alternative_Names___Titles" ILIKE :search '
            'OR ch."Short_Description" ILIKE :search '
            'OR ch."Denomination___Tradition" ILIKE :search '
            'OR ch."Long_Biography_Notes" ILIKE :search '
            'OR ch."Famous_Quotes" ILIKE :search '
            'OR ch."Key_Life_Events" ILIKE :search '
            'OR ch."Major_Works___Writings" ILIKE :search '
            'OR ch."Primary_Contributions___Accomplishments" ILIKE :search '
            'OR ch."Associated_Movements" ILIKE :search)'
        )
        params["search"] = f"%{search}%"

    if type_filter:
        where_clauses.append('ch."Type" = :type_filter')
        params["type_filter"] = type_filter

    if century:
        where_clauses.append('ch."Century" = :century')
        params["century"] = century

    if era_centuries:
        # Use exact equality via = ANY() to avoid partial matches
        # e.g. ILIKE '%6th%' would wrongly match '16th'
        placeholders = ", ".join(f":ec{i}" for i in range(len(era_centuries)))
        where_clauses.append(f'ch."Century" IN ({placeholders})')
        for i, kw in enumerate(era_centuries):
            params[f"ec{i}"] = kw

    if gender:
        where_clauses.append('ch."Gender" = :gender')
        params["gender"] = gender

    if denomination:
        where_clauses.append('ch."Denomination___Tradition" ILIKE :denomination')
        params["denomination"] = f"%{denomination}%"

    if role_office:
        where_clauses.append('ch."Role___Office" = :role_office')
        params["role_office"] = role_office

    if is_martyr is not None:
        params["martyr"] = "Yes" if is_martyr else "No"
        where_clauses.append('ch."Martyr___Yes_No_" = :martyr')

    if belief_id:
        where_clauses.append(
            'ch.id IN (SELECT "Church History_id" FROM "nc_hxad___nc_m2m_Church History_Beliefs" WHERE "Beliefs_id" = :belief_id)'
        )
        params["belief_id"] = belief_id

    where_sql = " AND ".join(where_clauses)

    total = db.execute(
        text(f'SELECT COUNT(*) FROM "Church History" ch WHERE {where_sql}'),
        params
    ).scalar()

    sort_map = {
        "name_asc":     'ch."Name_Event" ASC NULLS LAST',
        "name_desc":    'ch."Name_Event" DESC NULLS LAST',
        "date_asc":     'COALESCE(ch."Born___Start", ch."Death___End") ASC NULLS LAST',
        "date_desc":    'COALESCE(ch."Born___Start", ch."Death___End") DESC NULLS LAST',
        "century_asc":  'CAST(REGEXP_REPLACE(ch."Century", \'[^0-9]\', \'\', \'g\') AS INTEGER) ASC NULLS LAST',
        "century_desc": 'CAST(REGEXP_REPLACE(ch."Century", \'[^0-9]\', \'\', \'g\') AS INTEGER) DESC NULLS LAST',
        "type":         'ch."Type" ASC NULLS LAST, ch."Name_Event" ASC NULLS LAST',
    }
    order_clause = sort_map.get(sort, 'ch.nc_order ASC NULLS LAST, ch.id ASC')

    rows = db.execute(text(f"""
        SELECT
            ch.id,
            ch."Name_Event"                          AS name,
            ch."Alternative_Names___Titles"          AS alternative_names,
            ch."Gender"                              AS gender,
            ch."Type"                                AS type,
            ch."Role___Office"                       AS role_office,
            ch."Denomination___Tradition"            AS denomination_tradition,
            ch."Born___Start"                        AS born,
            ch."Death___End"                         AS death,
            ch."Era_Type__BC_AD_"                    AS era_type,
            ch."Century"                             AS century,
            ch."Birthplace"                          AS birthplace,
            ch."Region___Location"                    AS primary_region,
            ch."Short_Description"                   AS short_description,
            ch."Martyr___Yes_No_"                    AS is_martyr,
            ch."Believer_Saved"                      AS believer_saved,
            ch."Thumbnail"                           AS thumbnail_json,
            ch."Wikipedia_Name"                      AS wikipedia_name,
            ch."Cached_Image_URL"                     AS cached_image_url
        FROM "Church History" ch
        WHERE {where_sql}
        ORDER BY {order_clause}
        LIMIT :limit OFFSET :offset
    """), params).mappings().all()

    return total, rows


def get_map_figures(db: Session):
    """Fetch all figures that have parseable coordinates in Birthplace (format: lat;lng)."""
    return db.execute(text("""
        SELECT
            ch.id,
            ch."Name_Event"         AS name,
            ch."Type"               AS type,
            ch."Role___Office"      AS role_office,
            ch."Century"            AS century,
            ch."Born___Start"       AS born,
            ch."Death___End"        AS death,
            ch."Era_Type__BC_AD_"   AS era_type,
            ch."Birthplace"         AS birthplace,
            ch."Thumbnail"          AS thumbnail_json,
            ch."Wikipedia_Name"     AS wikipedia_name,
            ch."Cached_Image_URL"   AS cached_image_url,
            SPLIT_PART(ch."Birthplace", ';', 1)::float AS lat,
            SPLIT_PART(ch."Birthplace", ';', 2)::float AS lng
        FROM "Church History" ch
        WHERE ch."Birthplace" IS NOT NULL
        AND ch."Birthplace" ~ '^-?[0-9]+\.?[0-9]*;-?[0-9]+\.?[0-9]*$'
        AND ch."Name_Event" IS NOT NULL
        AND ch."Name_Event" != ''
        ORDER BY ch."Name_Event" ASC
    """)).mappings().all()


def get_related_figures(db: Session, figure_id: int, century: Optional[str], figure_type: Optional[str], limit: int = 6):
    """
    Find related figures by shared beliefs first, then same century, then same type.
    Excludes the current figure.
    """
    rows = db.execute(text("""
        SELECT DISTINCT
            ch.id,
            ch."Name_Event"         AS name,
            ch."Type"               AS type,
            ch."Role___Office"      AS role_office,
            ch."Century"            AS century,
            ch."Born___Start"       AS born,
            ch."Death___End"        AS death,
            ch."Era_Type__BC_AD_"   AS era_type,
            ch."Short_Description"  AS short_description,
            ch."Thumbnail"          AS thumbnail_json,
            ch."Wikipedia_Name"     AS wikipedia_name,
            ch."Cached_Image_URL"          AS cached_image_url,
            -- Score: shared belief = 3pts, same century = 2pts, same type = 1pt
            (
                CASE WHEN EXISTS (
                    SELECT 1 FROM "nc_hxad___nc_m2m_Church History_Beliefs" m1
                    JOIN "nc_hxad___nc_m2m_Church History_Beliefs" m2
                        ON m1."Beliefs_id" = m2."Beliefs_id"
                    WHERE m1."Church History_id" = :id
                    AND m2."Church History_id" = ch.id
                ) THEN 3 ELSE 0 END
                +
                CASE WHEN ch."Century" = :century THEN 2 ELSE 0 END
                +
                CASE WHEN ch."Type" = :type THEN 1 ELSE 0 END
            ) AS score
        FROM "Church History" ch
        WHERE ch.id != :id
        AND ch."Name_Event" IS NOT NULL
        AND ch."Name_Event" != ''
        AND (
            EXISTS (
                SELECT 1 FROM "nc_hxad___nc_m2m_Church History_Beliefs" m1
                JOIN "nc_hxad___nc_m2m_Church History_Beliefs" m2
                    ON m1."Beliefs_id" = m2."Beliefs_id"
                WHERE m1."Church History_id" = :id
                AND m2."Church History_id" = ch.id
            )
            OR ch."Century" = :century
            OR ch."Type" = :type
        )
        ORDER BY score DESC, ch."Name_Event" ASC
        LIMIT :limit
    """), {"id": figure_id, "century": century, "type": figure_type, "limit": limit}).mappings().all()
    return rows


def get_all_figures_for_caching(db: Session):
    """Fetch all figures for image cache population."""
    return db.execute(text("""
        SELECT id, "Name_Event" AS name, "Thumbnail" AS thumbnail_json,
               "Wikipedia_Name" AS wikipedia_name, "Cached_Image_URL" AS cached_image_url
        FROM "Church History"
        WHERE "Name_Event" IS NOT NULL AND "Name_Event" != ''
        ORDER BY id ASC
    """)).mappings().all()


def save_cached_image_url(db: Session, figure_id: int, url: str):
    """Write a resolved image URL back to the DB cache."""
    db.execute(text("""
        UPDATE "Church History" SET "Cached_Image_URL" = :url WHERE id = :id
    """), {"url": url, "id": figure_id})
    db.commit()


def get_random_figure_id(db: Session) -> Optional[int]:
    """Return a random figure id."""
    row = db.execute(text("""
        SELECT id FROM "Church History"
        ORDER BY RANDOM()
        LIMIT 1
    """)).scalar()
    return row


def get_figure_by_id(db: Session, figure_id: int):
    return db.execute(text("""
        SELECT
            ch.id,
            ch."Name_Event"                              AS name,
            ch."Alternative_Names___Titles"              AS alternative_names,
            ch."Gender"                                  AS gender,
            ch."Type"                                    AS type,
            ch."Role___Office"                           AS role_office,
            ch."Denomination___Tradition"                AS denomination_tradition,
            ch."Born___Start"                            AS born,
            ch."Death___End"                             AS death,
            ch."Era_Type__BC_AD_"                        AS era_type,
            ch."Century"                                 AS century,
            ch."Birthplace"                              AS birthplace,
            ch."Deathplace"                              AS deathplace,
            ch."Region___Location"                        AS primary_region,
            ch."Short_Description"                       AS short_description,
            ch."Long_Biography_Notes"                    AS long_biography,
            ch."Famous_Quotes"                           AS famous_quotes,
            ch."Major_Works___Writings"                  AS major_works,
            ch."Key_Life_Events"                         AS key_life_events,
            ch."Primary_Contributions___Accomplishments" AS primary_contributions,
            ch."Scripture_References"                    AS scripture_references,
            ch."Biblical_Books_Mentioned_In"             AS biblical_books,
            ch."Associated_Movements"                    AS associated_movements,
            ch."External_References___Sources"           AS external_references,
            ch."Notes"                                   AS notes,
            ch."Martyr___Yes_No_"                        AS is_martyr,
            ch."Believer_Saved"                          AS believer_saved,
            ch."Thumbnail"                               AS thumbnail_json,
            ch."Wikipedia_Name"                          AS wikipedia_name
        FROM "Church History" ch
        WHERE ch.id = :id
    """), {"id": figure_id}).mappings().first()


def get_figure_beliefs(db: Session, figure_id: int):
    return db.execute(text("""
        SELECT b.id, b."Belief_Name" AS belief_name, b."Description" AS description
        FROM "Beliefs" b
        JOIN "nc_hxad___nc_m2m_Church History_Beliefs" m ON m."Beliefs_id" = b.id
        WHERE m."Church History_id" = :id
        ORDER BY b.nc_order ASC NULLS LAST
    """), {"id": figure_id}).mappings().all()


def get_figure_eras(db: Session, figure_id: int):
    return db.execute(text("""
        SELECT e.id, e."Name" AS name, e."Time_Span" AS time_span
        FROM "Era" e
        JOIN "nc_hxad___nc_m2m_Church History_Era" m ON m."Era_id" = e.id
        WHERE m."Church History_id" = :id
    """), {"id": figure_id}).mappings().all()


def get_all_beliefs(db: Session):
    return db.execute(text(
        'SELECT id, "Belief_Name" AS belief_name, "Description" AS description '
        'FROM "Beliefs" ORDER BY nc_order ASC NULLS LAST'
    )).mappings().all()


def get_all_eras(db: Session):
    return db.execute(text(
        'SELECT id, "Name" AS name, "Time_Span" AS time_span '
        'FROM "Era" ORDER BY nc_order ASC NULLS LAST'
    )).mappings().all()


def get_era_range_counts(db: Session):
    """
    Count figures per historical era.
    Uses Century field as the primary source of truth when available,
    falling back to date ranges for records without a century.
    """
    ranges = [
        ("Apostolic Age",         30,   100,  ["1st"]),
        ("Early Church",         100,   313,  ["2nd", "3rd"]),
        ("Medieval Church",      313,  1517,  ["4th","5th","6th","7th","8th","9th","10th","11th","12th","13th","14th","15th"]),
        ("Reformation & Beyond", 1517, 1900,  ["16th","17th","18th","19th","20th","21st"]),
    ]
    results = []
    for label, start, end, century_keywords in ranges:
        placeholders = ", ".join(f":kw{i}" for i in range(len(century_keywords)))
        exact_params = {f"kw{i}": kw for i, kw in enumerate(century_keywords)}

        row = db.execute(text(f"""
            SELECT COUNT(DISTINCT id) FROM "Church History"
            WHERE "Century" IN ({placeholders})
        """), exact_params).scalar()
        results.append({"label": label, "start": start, "end": end, "count": row or 0})
    return results


def get_filter_options(db: Session):
    types = db.execute(text(
        'SELECT DISTINCT "Type" FROM "Church History" '
        'WHERE "Type" IS NOT NULL AND "Type" != \'\' ORDER BY "Type"'
    )).scalars().all()

    centuries = db.execute(text(
        'SELECT DISTINCT "Century" FROM "Church History" '
        'WHERE "Century" IS NOT NULL AND "Century" != \'\' ORDER BY "Century"'
    )).scalars().all()

    genders = db.execute(text(
        'SELECT DISTINCT "Gender" FROM "Church History" '
        'WHERE "Gender" IS NOT NULL AND "Gender" != \'\' ORDER BY "Gender"'
    )).scalars().all()

    denominations = db.execute(text(
        'SELECT DISTINCT "Denomination___Tradition" FROM "Church History" '
        'WHERE "Denomination___Tradition" IS NOT NULL AND "Denomination___Tradition" != \'\' '
        'ORDER BY "Denomination___Tradition"'
    )).scalars().all()

    role_offices = db.execute(text(
        'SELECT DISTINCT "Role___Office" FROM "Church History" '
        'WHERE "Role___Office" IS NOT NULL AND "Role___Office" != \'\' '
        'ORDER BY "Role___Office"'
    )).scalars().all()

    return {
        "types": list(types),
        "centuries": list(centuries),
        "genders": list(genders),
        "denominations": list(denominations),
        "role_offices": list(role_offices),
    }


# ── ADMIN QUERIES ─────────────────────────────────────────────────────────────

def admin_get_all_figures(db: Session):
    """Full list of figures for admin table — all fields."""
    return db.execute(text("""
        SELECT
            id, "Name_Event" AS name, "Type" AS type,
            "Gender" AS gender, "Century" AS century,
            "Born___Start" AS born, "Death___End" AS death,
            "Era_Type__BC_AD_" AS era_type,
            "Role___Office" AS role_office,
            "Denomination___Tradition" AS denomination_tradition,
            "Short_Description" AS short_description,
            "Wikipedia_Name" AS wikipedia_name,
            "Cached_Image_URL" AS cached_image_url,
            "Martyr___Yes_No_" AS is_martyr,
            "Believer_Saved" AS believer_saved,
            "Birthplace" AS birthplace,
            "Region___Location" AS primary_region,
            "Alternative_Names___Titles" AS alternative_names,
            nc_order
        FROM "Church History"
        ORDER BY nc_order ASC NULLS LAST, id ASC
    """)).mappings().all()


def admin_get_figure(db: Session, figure_id: int):
    """Full single figure for editing."""
    return db.execute(text("""
        SELECT
            id, "Name_Event" AS name, "Type" AS type,
            "Gender" AS gender, "Century" AS century,
            "Born___Start" AS born, "Death___End" AS death,
            "Era_Type__BC_AD_" AS era_type,
            "Role___Office" AS role_office,
            "Denomination___Tradition" AS denomination_tradition,
            "Alternative_Names___Titles" AS alternative_names,
            "Short_Description" AS short_description,
            "Long_Biography_Notes" AS long_biography,
            "Famous_Quotes" AS famous_quotes,
            "Major_Works___Writings" AS major_works,
            "Key_Life_Events" AS key_life_events,
            "Primary_Contributions___Accomplishments" AS primary_contributions,
            "Scripture_References" AS scripture_references,
            "Biblical_Books_Mentioned_In" AS biblical_books,
            "Associated_Movements" AS associated_movements,
            "External_References___Sources" AS external_references,
            "Notes" AS notes,
            "Birthplace" AS birthplace,
            "Deathplace" AS deathplace,
            "Region___Location" AS primary_region,
            "Wikipedia_Name" AS wikipedia_name,
            "Cached_Image_URL" AS cached_image_url,
            "Martyr___Yes_No_" AS is_martyr,
            "Believer_Saved" AS believer_saved,
            nc_order
        FROM "Church History"
        WHERE id = :id
    """), {"id": figure_id}).mappings().first()


def admin_create_figure(db: Session, data: dict) -> int:
    """Insert a new figure and return its id."""
    result = db.execute(text("""
        INSERT INTO "Church History" (
            "Name_Event", "Type", "Gender", "Century",
            "Born___Start", "Death___End", "Era_Type__BC_AD_",
            "Role___Office", "Denomination___Tradition",
            "Alternative_Names___Titles", "Short_Description",
            "Long_Biography_Notes", "Famous_Quotes",
            "Major_Works___Writings", "Key_Life_Events",
            "Primary_Contributions___Accomplishments",
            "Scripture_References", "Biblical_Books_Mentioned_In",
            "Associated_Movements", "External_References___Sources",
            "Notes", "Birthplace", "Deathplace", "Region___Location",
            "Wikipedia_Name", "Cached_Image_URL",
            "Martyr___Yes_No_", "Believer_Saved",
            created_at, updated_at
        ) VALUES (
            :name, :type, :gender, :century,
            :born, :death, :era_type,
            :role_office, :denomination,
            :alternative_names, :short_description,
            :long_biography, :famous_quotes,
            :major_works, :key_life_events,
            :primary_contributions,
            :scripture_references, :biblical_books,
            :associated_movements, :external_references,
            :notes, :birthplace, :deathplace, :primary_region,
            :wikipedia_name, :cached_image_url,
            :is_martyr, :believer_saved,
            NOW(), NOW()
        )
        RETURNING id
    """), data)
    db.commit()
    return result.scalar()


def admin_update_figure(db: Session, figure_id: int, data: dict):
    """Update an existing figure."""
    data["id"] = figure_id
    db.execute(text("""
        UPDATE "Church History" SET
            "Name_Event" = :name,
            "Type" = :type,
            "Gender" = :gender,
            "Century" = :century,
            "Born___Start" = :born,
            "Death___End" = :death,
            "Era_Type__BC_AD_" = :era_type,
            "Role___Office" = :role_office,
            "Denomination___Tradition" = :denomination,
            "Alternative_Names___Titles" = :alternative_names,
            "Short_Description" = :short_description,
            "Long_Biography_Notes" = :long_biography,
            "Famous_Quotes" = :famous_quotes,
            "Major_Works___Writings" = :major_works,
            "Key_Life_Events" = :key_life_events,
            "Primary_Contributions___Accomplishments" = :primary_contributions,
            "Scripture_References" = :scripture_references,
            "Biblical_Books_Mentioned_In" = :biblical_books,
            "Associated_Movements" = :associated_movements,
            "External_References___Sources" = :external_references,
            "Notes" = :notes,
            "Birthplace" = :birthplace,
            "Deathplace" = :deathplace,
            "Region___Location" = :primary_region,
            "Wikipedia_Name" = :wikipedia_name,
            "Cached_Image_URL" = :cached_image_url,
            "Martyr___Yes_No_" = :is_martyr,
            "Believer_Saved" = :believer_saved,
            updated_at = NOW()
        WHERE id = :id
    """), data)
    db.commit()


def admin_delete_figure(db: Session, figure_id: int):
    """Delete a figure and its belief/era links."""
    db.execute(text('DELETE FROM "nc_hxad___nc_m2m_Church History_Beliefs" WHERE "Church History_id" = :id'), {"id": figure_id})
    db.execute(text('DELETE FROM "nc_hxad___nc_m2m_Church History_Era" WHERE "Church History_id" = :id'), {"id": figure_id})
    db.execute(text('DELETE FROM "Church History" WHERE id = :id'), {"id": figure_id})
    db.commit()


def admin_get_stats(db: Session) -> dict:
    """Dashboard stats for admin panel."""
    total         = db.execute(text('SELECT COUNT(*) FROM "Church History"')).scalar()
    persons       = db.execute(text('SELECT COUNT(*) FROM "Church History" WHERE "Type" = \'Person\'')).scalar()
    events        = db.execute(text('SELECT COUNT(*) FROM "Church History" WHERE "Type" = \'Event\'')).scalar()
    groups        = db.execute(text('SELECT COUNT(*) FROM "Church History" WHERE "Type" = \'Group\'')).scalar()
    martyrs       = db.execute(text('SELECT COUNT(*) FROM "Church History" WHERE "Martyr___Yes_No_" = \'Yes\'')).scalar()
    no_desc       = db.execute(text('SELECT COUNT(*) FROM "Church History" WHERE "Short_Description" IS NULL OR "Short_Description" = \'\'')).scalar()
    no_dates      = db.execute(text('SELECT COUNT(*) FROM "Church History" WHERE "Born___Start" IS NULL AND "Death___End" IS NULL')).scalar()
    no_image      = db.execute(text('SELECT COUNT(*) FROM "Church History" WHERE ("Thumbnail" IS NULL OR "Thumbnail" = \'\') AND ("Cached_Image_URL" IS NULL OR "Cached_Image_URL" = \'\') AND ("Wikipedia_Name" IS NULL OR "Wikipedia_Name" = \'\')')).scalar()
    no_century    = db.execute(text('SELECT COUNT(*) FROM "Church History" WHERE "Century" IS NULL OR "Century" = \'\'')).scalar()
    has_coords    = db.execute(text('SELECT COUNT(*) FROM "Church History" WHERE "Birthplace" ~ \'^-?[0-9]+\\.?[0-9]*;-?[0-9]+\\.?[0-9]*$\'')).scalar()
    cached_images = db.execute(text('SELECT COUNT(*) FROM "Church History" WHERE "Cached_Image_URL" IS NOT NULL AND "Cached_Image_URL" != \'\'')).scalar()

    by_century = db.execute(text("""
        SELECT "Century", COUNT(*) AS cnt
        FROM "Church History"
        WHERE "Century" IS NOT NULL AND "Century" != ''
        GROUP BY "Century"
        ORDER BY "Century"
    """)).mappings().all()

    return {
        "total": total, "persons": persons, "events": events,
        "groups": groups, "martyrs": martyrs,
        "missing_description": no_desc, "missing_dates": no_dates,
        "missing_image": no_image, "missing_century": no_century,
        "has_coordinates": has_coords, "cached_images": cached_images,
        "by_century": [{"century": r["Century"], "count": r["cnt"]} for r in by_century],
    }


def admin_get_beliefs(db: Session):
    return db.execute(text(
        'SELECT id, "Belief_Name" AS belief_name, "Description" AS description '
        'FROM "Beliefs" ORDER BY nc_order ASC NULLS LAST, id ASC'
    )).mappings().all()


def admin_get_figure_belief_ids(db: Session, figure_id: int):
    rows = db.execute(text(
        'SELECT "Beliefs_id" FROM "nc_hxad___nc_m2m_Church History_Beliefs" '
        'WHERE "Church History_id" = :id'
    ), {"id": figure_id}).scalars().all()
    return list(rows)


def admin_set_figure_beliefs(db: Session, figure_id: int, belief_ids: list):
    """Replace all belief links for a figure."""
    db.execute(text(
        'DELETE FROM "nc_hxad___nc_m2m_Church History_Beliefs" '
        'WHERE "Church History_id" = :id'
    ), {"id": figure_id})
    for bid in belief_ids:
        db.execute(text(
            'INSERT INTO "nc_hxad___nc_m2m_Church History_Beliefs" '
            '("Church History_id", "Beliefs_id") VALUES (:fid, :bid)'
        ), {"fid": figure_id, "bid": bid})
    db.commit()


def admin_create_belief(db: Session, name: str, description: str) -> int:
    result = db.execute(text(
        'INSERT INTO "Beliefs" ("Belief_Name", "Description", created_at, updated_at) '
        'VALUES (:name, :desc, NOW(), NOW()) RETURNING id'
    ), {"name": name, "desc": description})
    db.commit()
    return result.scalar()


def admin_update_belief(db: Session, belief_id: int, name: str, description: str):
    db.execute(text(
        'UPDATE "Beliefs" SET "Belief_Name" = :name, "Description" = :desc, updated_at = NOW() WHERE id = :id'
    ), {"name": name, "desc": description, "id": belief_id})
    db.commit()


def admin_delete_belief(db: Session, belief_id: int):
    db.execute(text(
        'DELETE FROM "nc_hxad___nc_m2m_Church History_Beliefs" WHERE "Beliefs_id" = :id'
    ), {"id": belief_id})
    db.execute(text('DELETE FROM "Beliefs" WHERE id = :id'), {"id": belief_id})
    db.commit()