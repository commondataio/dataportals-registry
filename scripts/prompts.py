import re
from dataclasses import dataclass
from typing import Optional
import json
import requests
import csv
import sys
import os
from io import StringIO

PERPLEXITY_API_KEY = os.getenv('PERPLEXITY_API_KEY', )


# This is the prompt we use for getting chat gpt 4o to convert documents into our silver training data
def build_openai_silver_data_prompt(base_text: str) -> str:
    return (
        f"Use following vocabularies\n"
        f"1. First vocabulary owner_type_reference should be used to identity type of the owner of the data catalogs and could have following values: Central government, Regional government, Local government, Business, Civil society, Academy, International."
        f"2. Another vocabulary data_theme should be used to assign topic according to EU data theme vocabulary associated with Data Catalog Vocabulary application profile (DCAT-AP)."
        f"3. Another vocabulary iso19115_theme should be used to assign geo topic category to ISO19115 Topic Categories from ISO19115:2003 standard."
        f"Do not hallucinate.\n"
        f"Below is URL of the website which is data catalog."
        f"Extract from this url following information strictly according to schema provided"
        f"RAW_TEXT_START\n{base_text}\nRAW_TEXT_END"
    )


@dataclass(frozen=True)
class EnrichmentResponse:
    primary_language: Optional[str]
    natural_text: Optional[str]



def openai_response_format_schema() -> dict:
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "enrich_response",
            "schema": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": ["string", "null"],
                        "description": "Name of the data catalog in English",
                    },
                    "description": {
                        "type": ["string", "null"],
                        "description": "Description in English of the data catalog in English, up to 150 characters",
                    },
                    "owner_name": {
                        "type": ["string", "null"],
                        "description": "Name of the organization that owns this data catalog",
                    },
                    "owner_website": {
                        "type": ["string", "null"],
                        "description": "URL of the website of the organization that owns this data catalog",
                    },
                    "owner_type": {
                        "type": ["string", "null"],
                        "description": "Type of the owner according owner_type_reference",
                        "enum": ["Central government", "Regional government", "Local government", "Business", "Academy", "Civil society", "Community", "International"]
                    }, 
                    "owner_country_iso2": {
                        "type": ["string", "null"],
                        "description": "ISO2 code of the country of the owner of this data catalog if ISO2 code known overwise empty",
                    },
                    "owner_country": {
                        "type": ["string", "null"],
                        "description": "Name of the country of the owner of this data catalog if ISO2 code known",
                    },
                    "owner_subregion_iso3166_2": {
                        "type": ["string", "null"],
                        "description": "ISO 3166-2 code of the subregion of the owner of this data catalog if ISO2 code known overwise empty",
                    },
                    "owner_subregion_name": {
                        "type": ["string", "null"],
                        "description": "Name of the country of the owner of this data catalog from ISO3166-2 vocbulary",
                    },

                    "data_themes": {
                        "type": ["array", "null"],
                        "description": "Data catalog primary themes according to EU data theme vocabulary associated with Data Catalog Vocabulary application profile (DCAT-AP)",
                        "maxItems": 10,
                        "items": {
                             "type": "string"
                        }
                    },
                    "geotopics": {
                        "type": ["array", "null"],
                        "description": "Data catalog ISO19115 Topic Categories",
                        "maxItems": 10,
                        "items": {
                             "type": "string"
                        }
                    }, 
                    "tags":  {
                        "type": ["array", "null"],
                        "description" : "Tags associated with data catalogs",
                        "maxItems": 10,
                        "items": {
                             "type": "string"
                        }
                    }
                },
                "additionalProperties": False,
                "required": [
                    "name",
                    "description",
                    "owner_name", 
                    "owner_website",
                    "owner_type"
                    "owner_country",
                    "topic",
                    "geotopic",
                    "tags"
                ],
            },
            "strict": True,
        },
    }


# This is a base prompt that will be used for training and running the fine tuned model
# It's simplified from the prompt which was used to generate the silver data, and can change from dataset to dataset
def build_finetuning_prompt(base_text: str) -> str:
    return (
        f"Below is the URL of the data catalog website "
        f"Return result of data catalog parameters from website"
        f"Do not hallucinate.\n"
        f"RAW_TEXT_START\n{base_text}\nRAW_TEXT_END"
    )


# Extracts the anchor text component from an existing prompt string
def extract_raw_text(prompt: str) -> str:
    pattern = r"RAW_TEXT_START\s*\n(.*?)\nRAW_TEXT_END"

    # Use re.DOTALL to ensure that the dot matches newline characters
    match = re.search(pattern, prompt, re.DOTALL)

    if match:
        return match.group(1).strip()
    else:
        raise ValueError("Prompt does not contain raw text")

def get_profile(url):
    api_url = "https://api.perplexity.ai/chat/completions"
    headers = {"Authorization": f"Bearer {PERPLEXITY_API_KEY}"}
    payload = {
        "model": "sonar",
        "messages": [
            {"role": "system", "content": "Be precise and concise, provide data output only CSV or JSON, accrording to request"},
            {"role": "user", "content": (
                build_openai_silver_data_prompt(url))},
        ],
        "response_format": openai_response_format_schema()
    }
    resp = requests.post(api_url, headers=headers, json=payload)
    response = resp.json()     
    return response["choices"][0]["message"]["content"]

if __name__ == "__main__":
    get_profile(sys.argv[1])