from typing import Any, Dict, List, Literal

from pydantic import BaseModel, field_validator


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


class UrlMatchParams(BaseRuleParams):
    url_patterns: List[str]
    operator: Literal["AND", "OR"] = "OR"


class SubRuleConfig(BaseModel):
    rule_type: str
    rule_params: Dict[str, Any]


class CompositeParams(BaseModel):
    """
    operator: "AND" (모든 조건 만족) 또는 "OR" (하나라도 만족)
    rules: 적용할 하위 규칙 리스트
    """

    operator: Literal["AND", "OR"] = "AND"
    rules: List[SubRuleConfig]
