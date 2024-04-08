from datetime import datetime
from typing import Dict, List, Union, Optional
from pydantic import BaseModel, Field

from .shared import SpokenLanguage, Location, Topic, Endpoint, Identifier, Organization, Software

class DataCatalog(BaseModel):
    id: str = Field(..., examples=["catalogdatagov...."])
    uid: str = Field(..., examples=["cdi00001616"])
    name: str = Field(..., examples=["Data.gov portal"])
    link: str = Field(..., examples=["https://catalog.data.gov"])
    catalog_type: str = Field(..., examples=["Open data portal"])
    properties: Optional[Dict] = Field(None, examples=[{"transferable_topics" : True}])
    api: bool = Field(False)
    api_status: str = Field(..., examples=["uncertain"])
    access_mode: List[str] = Field([], examples=[["dataset"]])
    langs: List[SpokenLanguage] = Field([], examples=[])
    tags: List[str] = Field([], examples=[["biota", "life", "geology"]])
    content_types: List[str] = Field([], examples=[["dataset"]])
    coverage: List[Location] = Field(..., examples=[{"location" : ["value"]}])
    endpoints: Optional[List[Endpoint]] = Field([], examples=[])
    identifiers: List[Identifier] = Field([], examples=[])
    owner: Organization = Field(..., examples=[])
    software: Software = Field(..., examples=[{"id" : "ckan", "name" : "CKAN"}])
    status: str = Field(..., examples=["uncertain"])
    topics: Optional[List[Topic]] = Field([], examples=[])
