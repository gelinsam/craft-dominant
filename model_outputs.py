"""Craft's existing content contracts, enforced by SDK structured output."""
from typing import List, Literal
from pydantic import BaseModel, ConfigDict, Field


class StrictOutput(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, str_strip_whitespace=True)


class CampaignOutput(StrictOutput):
    subject_line: str = Field(min_length=1, max_length=200)
    preview_text: str = Field(max_length=200)
    body_html: str = Field(min_length=1, max_length=100000)
    cta_text: str = Field(min_length=1, max_length=200)
    cta_url: str = Field(min_length=1, max_length=2048)
    barrier_addressed: Literal['availability','social','concept','value','urgency']
    strategic_reasoning: str = Field(min_length=1, max_length=4000)
    predicted_open_rate: float = Field(ge=0, le=1)
    predicted_click_rate: float = Field(ge=0, le=1)
    confidence_score: float = Field(ge=0, le=1)
    segment_priority: str = Field(min_length=1, max_length=2000)


class Learning(StrictOutput):
    category: Literal['copy','timing','segment']
    learning: str = Field(min_length=1, max_length=2000)
    confidence: float = Field(ge=0, le=1)


class LearningOutput(StrictOutput):
    learnings: List[Learning] = Field(min_length=2, max_length=3)
    what_worked: str = Field(min_length=1, max_length=2000)
    what_to_improve: str = Field(min_length=1, max_length=2000)
