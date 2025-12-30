from typing import List, Literal

from pydantic import BaseModel, Field, field_validator


class BaseRuleParams(BaseModel):
    pass


class KeywordParams(BaseRuleParams):
    keywords: List[str]
    operator: Literal["AND", "OR"] = "OR"

    @field_validator("keywords")
    def check_keywords(cls, v):
        if not v:
            raise ValueError("keywords list cannot be empty.")
        return v


class TagMatchParams(BaseRuleParams):
    tags: List[str]
    operator: Literal["AND", "OR"] = "OR"

    @field_validator("tags")
    def check_tags_not_empty(cls, v):
        if not v:
            raise ValueError("tags list cannot be empty.")
        return v


class VectorParams(BaseRuleParams):
    threshold: float = Field(..., ge=0.0, le=1.0)
    ref_content_id: str
