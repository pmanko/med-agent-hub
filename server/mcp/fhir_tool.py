"""
MCP Tool for FHIR endpoint queries.
Simple wrapper around OpenMRS or other FHIR servers.
"""

from typing import Dict, Any, Optional, List
from .base import MCPTool
import httpx
import logging
import os
import base64

logger = logging.getLogger(__name__)


class FHIRSearchTool(MCPTool):
    """MCP tool for querying FHIR endpoints."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize FHIR client.
        
        Args:
            config: Dict with 'base_url', 'username', 'password' keys
        """
        self.config = config or {
            'base_url': os.getenv('OPENMRS_FHIR_BASE_URL', ''),
            'username': os.getenv('OPENMRS_USERNAME', ''),
            'password': os.getenv('OPENMRS_PASSWORD', '')
        }
        
        # Create HTTP client with auth if credentials provided
        self.client = httpx.AsyncClient(timeout=30.0)
        
        # Setup basic auth header if credentials available
        self.headers = {}
        if self.config.get('username') and self.config.get('password'):
            credentials = f"{self.config['username']}:{self.config['password']}"
            encoded = base64.b64encode(credentials.encode()).decode()
            self.headers['Authorization'] = f"Basic {encoded}"
        
        self.base_url = self.config.get('base_url', '').rstrip('/')
        
        if self.base_url:
            logger.info(f"FHIR client configured for: {self.base_url}")
        else:
            logger.warning("No FHIR base URL configured")
    
    @property
    def name(self) -> str:
        return "fhir_search"
    
    @property
    def schema(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": "Search and retrieve FHIR resources from the server",
            "input_schema": {
                "type": "object",
                "properties": {
                    "resource_type": {
                        "type": "string",
                        "enum": ["Patient", "Observation", "Condition", "MedicationRequest", 
                                "Encounter", "Procedure", "DiagnosticReport", "AllergyIntolerance"],
                        "description": "FHIR resource type to search"
                    },
                    "patient_id": {
                        "type": "string",
                        "description": "Patient ID to filter results"
                    },
                    "search_params": {
                        "type": "object",
                        "description": "Additional FHIR search parameters",
                        "properties": {
                            "code": {"type": "string"},
                            "date": {"type": "string"},
                            "_count": {"type": "integer"},
                            "_sort": {"type": "string"},
                            "status": {"type": "string"},
                            "category": {"type": "string"}
                        }
                    },
                    "operation": {
                        "type": "string",
                        "enum": ["search", "read", "$everything"],
                        "default": "search",
                        "description": "FHIR operation to perform"
                    }
                },
                "required": ["resource_type"]
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "resource_type": {"type": "string"},
                    "total": {"type": "integer"},
                    "entries": {"type": "array"},
                    "url": {"type": "string"}
                }
            }
        }
    
    async def invoke(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute FHIR search."""
        self.validate_input(params)
        
        if not self.base_url:
            return self._get_mock_results(params)
        
        resource_type = params["resource_type"]
        operation = params.get("operation", "search")
        
        try:
            if operation == "read" and params.get("patient_id"):
                # Direct resource read
                url = f"{self.base_url}/{resource_type}/{params['patient_id']}"
                response = await self.client.get(url, headers=self.headers)
                
                if response.status_code == 200:
                    return {
                        "resource_type": resource_type,
                        "total": 1,
                        "entries": [response.json()],
                        "url": url
                    }
                else:
                    return {
                        "error": f"HTTP {response.status_code}",
                        "url": url
                    }
            
            elif operation == "$everything" and params.get("patient_id"):
                # Patient $everything operation
                url = f"{self.base_url}/Patient/{params['patient_id']}/$everything"
                response = await self.client.get(url, headers=self.headers)
                
                if response.status_code == 200:
                    bundle = response.json()
                    return {
                        "resource_type": "Bundle",
                        "total": bundle.get("total", 0),
                        "entries": [e.get("resource") for e in bundle.get("entry", [])],
                        "url": url
                    }
            
            else:
                # Standard search
                url = f"{self.base_url}/{resource_type}"
                
                # Build search parameters
                search_params = params.get("search_params", {}).copy()
                
                # Add patient filter if provided
                if params.get("patient_id"):
                    if resource_type == "Patient":
                        search_params["_id"] = params["patient_id"]
                    else:
                        search_params["patient"] = params["patient_id"]
                
                # Default to limiting results
                if "_count" not in search_params:
                    search_params["_count"] = 10
                
                response = await self.client.get(
                    url,
                    params=search_params,
                    headers=self.headers
                )
                
                if response.status_code == 200:
                    bundle = response.json()
                    
                    # Extract resources from bundle
                    entries = []
                    if bundle.get("entry"):
                        entries = [e.get("resource") for e in bundle["entry"]]
                    
                    return {
                        "resource_type": resource_type,
                        "total": bundle.get("total", len(entries)),
                        "entries": entries,
                        "url": str(response.url)
                    }
                else:
                    return {
                        "error": f"HTTP {response.status_code}: {response.text[:200]}",
                        "url": str(response.url)
                    }
        
        except Exception as e:
            logger.error(f"FHIR search failed: {e}")
            return {
                "error": str(e),
                "resource_type": resource_type
            }
    
    def _get_mock_results(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Return mock FHIR results for testing."""
        resource_type = params["resource_type"]
        
        mock_data = {
            "Patient": [
                {
                    "resourceType": "Patient",
                    "id": "example-123",
                    "name": [{"family": "Doe", "given": ["John"]}],
                    "gender": "male",
                    "birthDate": "1980-01-15"
                }
            ],
            "Observation": [
                {
                    "resourceType": "Observation",
                    "id": "obs-1",
                    "code": {
                        "coding": [{
                            "system": "http://loinc.org",
                            "code": "4548-4",
                            "display": "Hemoglobin A1c"
                        }]
                    },
                    "valueQuantity": {"value": 7.2, "unit": "%"},
                    "effectiveDateTime": "2024-03-01"
                }
            ],
            "Condition": [
                {
                    "resourceType": "Condition",
                    "id": "cond-1",
                    "code": {
                        "coding": [{
                            "system": "http://snomed.info/sct",
                            "code": "44054006",
                            "display": "Type 2 diabetes mellitus"
                        }]
                    },
                    "clinicalStatus": {
                        "coding": [{"code": "active"}]
                    }
                }
            ]
        }
        
        return {
            "resource_type": resource_type,
            "total": 1,
            "entries": mock_data.get(resource_type, []),
            "url": "mock://fhir"
        }
    
    async def cleanup(self):
        """Clean up HTTP client."""
        await self.client.aclose()
