from pydantic import BaseModel
from typing import Optional, List


class BeliefBase(BaseModel):
    id: int
    belief_name: Optional[str] = None
    description: Optional[str] = None

    class Config:
        from_attributes = True


class EraBase(BaseModel):
    id: int
    name: Optional[str] = None
    time_span: Optional[str] = None

    class Config:
        from_attributes = True


class FigureCard(BaseModel):
    id: int
    name: Optional[str] = None
    alternative_names: Optional[str] = None
    gender: Optional[str] = None
    type: Optional[str] = None
    role_office: Optional[str] = None
    denomination_tradition: Optional[str] = None
    born: Optional[int] = None
    death: Optional[int] = None
    era_type: Optional[str] = None
    century: Optional[str] = None
    birthplace: Optional[str] = None
    primary_region: Optional[str] = None
    short_description: Optional[str] = None
    image_url: Optional[str] = None
    is_martyr: Optional[str] = None
    believer_saved: Optional[str] = None
    beliefs: List[BeliefBase] = []

    class Config:
        from_attributes = True


class FigureDetail(FigureCard):
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
    deathplace: Optional[str] = None
    eras: List[EraBase] = []

    class Config:
        from_attributes = True


class FigureListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    results: List[FigureCard]


class FilterOptions(BaseModel):
    types: List[str]
    centuries: List[str]
    genders: List[str]
    denominations: List[str]
    role_offices: List[str]
    beliefs: List[BeliefBase]
    eras: List[EraBase]