from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional


def get_figures(
    db: Session,
    search: Optional[str] = None,
    type_filter: Optional[str] = None,
    century: Optional[str] = None,
    gender: Optional[str] = None,
    denomination: Optional[str] = None,
    belief_id: Optional[int] = None,
    is_martyr: Optional[bool] = None,
    page: int = 1,
    page_size: int = 24,
):
    offset = (page - 1) * page_size
    where_clauses = ["1=1"]
    params = {"limit": page_size, "offset": offset}

    if search:
        where_clauses.append(
            '(ch."Name_Event" ILIKE :search OR ch."Alternative_Names___Titles" ILIKE :search '
            'OR ch."Short_Description" ILIKE :search OR ch."Denomination___Tradition" ILIKE :search)'
        )
        params["search"] = f"%{search}%"

    if type_filter:
        where_clauses.append('ch."Type" = :type_filter')
        params["type_filter"] = type_filter

    if century:
        where_clauses.append('ch."Century" = :century')
        params["century"] = century

    if gender:
        where_clauses.append('ch."Gender" = :gender')
        params["gender"] = gender

    if denomination:
        where_clauses.append('ch."Denomination___Tradition" ILIKE :denomination')
        params["denomination"] = f"%{denomination}%"

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
            ch."Primary_Region___Area"               AS primary_region,
            ch."Short_Description"                   AS short_description,
            ch."Martyr___Yes_No_"                    AS is_martyr,
            ch."Believer_Saved"                      AS believer_saved,
            ch."Thumbnail"                           AS thumbnail_json
        FROM "Church History" ch
        WHERE {where_sql}
        ORDER BY ch.nc_order ASC NULLS LAST, ch.id ASC
        LIMIT :limit OFFSET :offset
    """), params).mappings().all()

    return total, rows


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
            ch."Primary_Region___Area"                   AS primary_region,
            ch."Short_Description"                       AS short_description,
            ch."Long_Biography_Notes"                    AS long_biography,
            ch."Famous_Quotes"                           AS famous_quotes,
            ch."Major_Works___Writings"                  AS major_works,
            ch."Key_Life_Events"                         AS key_life_events,
            ch."Primary_Contributions___Accomplishments" AS primary_contributions,
            ch."Scripture_References"                    AS scripture_references,
            ch."Biblical_Books_Mentioned_In"             AS biblical_books,
            ch."Associated_Movements"                    AS associated_movements,
            ch."Father"                                  AS father,
            ch."Mother"                                  AS mother,
            ch."Spouse"                                  AS spouse,
            ch."Children"                                AS children,
            ch."Genealogy_Notes"                         AS genealogy_notes,
            ch."Burial___Traditional_Site"               AS burial_site,
            ch."External_References___Sources"           AS external_references,
            ch."Notes"                                   AS notes,
            ch."Martyr___Yes_No_"                        AS is_martyr,
            ch."Believer_Saved"                          AS believer_saved,
            ch."Thumbnail"                               AS thumbnail_json
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
    Count figures per historical era using dates first, Century field as fallback.
    Handles compound century values like '1st & 2nd', '4th - 7th'.
    """
    # Each era: (label, start_year, end_year, century_keywords)
    # Keywords are checked with ILIKE so '1st & 2nd' matches '1st'
    ranges = [
        ("Apostolic Age",         30,   100,  ["1st"]),
        ("Early Church",         100,   313,  ["2nd", "3rd"]),
        ("Medieval Church",      313,  1517,  ["4th","5th","6th","7th","8th","9th","10th","11th","12th","13th","14th","15th"]),
        ("Reformation & Beyond", 1517, 1900,  ["16th"]),
    ]
    results = []
    for label, start, end, century_keywords in ranges:
        # Build ILIKE conditions for each keyword e.g. "Century" ILIKE '%1st%'
        ilike_clauses = " OR ".join(
            f'"Century" ILIKE :kw{i}' for i in range(len(century_keywords))
        )
        ilike_params = {f"kw{i}": f"%{kw}%" for i, kw in enumerate(century_keywords)}

        row = db.execute(text(f"""
            SELECT COUNT(*) FROM "Church History"
            WHERE (
                -- Primary: known date falls in range
                ("Born___Start" >= :start AND "Born___Start" < :end)
                OR ("Death___End" >= :start AND "Death___End" < :end)
            )
            OR (
                -- Fallback: no dates, but Century keyword matches this era
                "Born___Start" IS NULL
                AND "Death___End" IS NULL
                AND ({ilike_clauses})
            )
        """), {"start": start, "end": end, **ilike_params}).scalar()
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

    return {
        "types": list(types),
        "centuries": list(centuries),
        "genders": list(genders),
        "denominations": list(denominations),
    }