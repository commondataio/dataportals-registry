from datetime import datetime
from typing import Dict, List, Union, Optional
from pydantic import BaseModel, Field


class Country(BaseModel):
    id: str = Field(..., examples=["ZM"])
    name: str = Field(..., examples=["Zambia"])

class SubRegion(BaseModel):
    id: str = Field(..., examples=["US-TX"])
    name: Optional[str] = Field(None, examples=["Texas"])


class LocationBase(BaseModel):
    country: Country = Field(..., examples=[{"location" : ["value"]}])
    level: int = Field(1, examples=[1])
    subregion: Optional[SubRegion] = Field(None, examples=[{"id" : "US-TX"}])

class Location(BaseModel):
    location: LocationBase = Field(..., examples=[{"location" : ["value"]}])


class Endpoint(BaseModel):
    type: str = Field(..., examples=["ckan:package-search"])
    url: str = Field(..., examples=["https://catalog.data.gov/api/3action/package_search"])
    version: Optional[str] = Field(None, examples=["3.0"])

class Identifier(BaseModel):
    id: str = Field(..., examples=["wikidata"])
    url: Optional[str] = Field(..., examples=["https://www.wikidata.org/wiki/Q5227102"])
    value: str = Field(..., examples=["Q5227102"])


class Topic(BaseModel):
    id: str = Field(..., examples=["SOCI"])
    name: Optional[str] = Field(..., examples=["Population and society"])
    type: str = Field(..., examples=["eudatatheme"])

class Software(BaseModel):
    id: str = Field(..., examples=["ckan"])
    name: str = Field(..., examples=["CKAN"])


class Organization(BaseModel):
    name: str = Field(..., examples=["USA Government"])
    link: str = Field(..., examples=["https://www.whitehouse.gov"])
    type: str = Field(..., examples=["Federal government"])
    location: Location = Field(..., examples=[])
    

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
    langs: List[str] = Field([], examples=[["EN", "ES", "FR"]])
    tags: List[str] = Field([], examples=[["biota", "life", "geology"]])
    content_types: List[str] = Field([], examples=[["dataset"]])
    coverage: List[Location] = Field(..., examples=[{"location" : ["value"]}])
    endpoints: Optional[List[Endpoint]] = Field([], examples=[])
    identifiers: List[Identifier] = Field([], examples=[])
    software: Software = Field(..., examples=[{"id" : "ckan", "name" : "CKAN"}])
    status: str = Field(..., examples=["uncertain"])
    topics: Optional[List[Topic]] = Field([], examples=[])
