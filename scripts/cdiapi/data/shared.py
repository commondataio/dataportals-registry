from datetime import datetime
from typing import Dict, List, Union, Optional
from pydantic import BaseModel, Field


class Country(BaseModel):
    """Country code and name from ISO 3166-1"""
    id: str = Field(..., examples=["ZM"])
    name: str = Field(..., examples=["Zambia"])

class SubRegion(BaseModel):
    """Country subregion code and name from ISO 3166-2"""
    id: str = Field(..., examples=["US-TX"])
    name: Optional[str] = Field(None, examples=["Texas"])

class MacroRegion(BaseModel):
    """Macro region code and name from UN49 classification"""
    id: str = Field(..., examples=["145"])
    name: Optional[str] = Field(None, examples=["Western Asia"])


class LocationBase(BaseModel):
    country: Country = Field(..., examples=[{"location" : ["value"]}])
    level: int = Field(1, examples=[1, 2, 3])
    subregion: Optional[SubRegion] = Field(None, examples=[{"id" : "US-TX", "name" : "Texas"}])
    macroregion: Optional[MacroRegion] = Field(None, examples=[{"id" : "145", "name" : "Western Asia"}])

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
    link: Optional[str] = Field(None, examples=["https://www.whitehouse.gov"])
    type: str = Field(..., examples=["Federal government"])
    location: LocationBase = Field(..., examples=[])


class SpokenLanguage(BaseModel):
    id: str = Field(..., examples=["EN"])
    name: str = Field(..., examples=["English"])


class Rights:
    license_id: str = Field(..., examples=["cc-by"])
    license_name: str = Field(..., examples=["Creative Commons BY 4.0"])
    license_url: str = Field(..., examples=['https://creativecommons.org/licenses/by/4.0/'])
    privacy_policy_url: str = Field(..., examples=['https://somewebsite/privacy'])
    rights_type: str = Field(..., examples=['granular', 'global', 'unknown', 'inapplicable'])
    tos_url: str = Field(..., examples=['https://somewebsite/terms'])

