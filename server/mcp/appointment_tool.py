"""
MCP Tool for appointment management via OpenMRS REST API.
"""

from typing import Dict, Any, Optional, List
from .base import MCPTool
import httpx
import logging
import os
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class AppointmentTool(MCPTool):
    """MCP tool for managing appointments via OpenMRS REST API."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize appointment management client.
        
        Args:
            config: Dict with 'rest_base_url', 'username', 'password' keys
        """
        self.config = config or {
            'rest_base_url': os.getenv('OPENMRS_REST_BASE_URL', 
                                      os.getenv('OPENMRS_FHIR_BASE_URL', '').replace('/ws/fhir2/R4', '/ws/rest/v1')),
            'username': os.getenv('OPENMRS_USERNAME', ''),
            'password': os.getenv('OPENMRS_PASSWORD', '')
        }
        
        self.rest_base = self.config.get('rest_base_url', '').rstrip('/')
        
        # Create HTTP client with auth
        auth = None
        if self.config.get('username') and self.config.get('password'):
            auth = (self.config['username'], self.config['password'])
        
        self.client = httpx.AsyncClient(auth=auth, timeout=30.0)
        
        if self.rest_base:
            logger.info(f"Appointment tool configured for: {self.rest_base}")
        else:
            logger.warning("No OpenMRS REST URL configured")
    
    @property
    def name(self) -> str:
        return "appointment_manager"
    
    @property
    def schema(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": "Review and schedule appointments in OpenMRS",
            "input_schema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["review", "schedule"],
                        "description": "Action to perform"
                    },
                    "patient_id": {
                        "type": "string",
                        "description": "Patient UUID for filtering or scheduling"
                    },
                    "appointment_details": {
                        "type": "object",
                        "description": "Details for scheduling new appointment",
                        "properties": {
                            "date": {
                                "type": "string",
                                "format": "date",
                                "description": "Appointment date (YYYY-MM-DD)"
                            },
                            "time": {
                                "type": "string",
                                "pattern": "^([01]?[0-9]|2[0-3]):[0-5][0-9]$",
                                "description": "Appointment time (HH:MM)"
                            },
                            "duration_minutes": {
                                "type": "integer",
                                "default": 30
                            },
                            "provider_uuid": {
                                "type": "string",
                                "description": "Provider UUID"
                            },
                            "service": {
                                "type": "string",
                                "description": "Service or appointment type"
                            },
                            "location_uuid": {
                                "type": "string",
                                "description": "Location UUID"
                            },
                            "reason": {
                                "type": "string",
                                "description": "Reason for appointment"
                            }
                        },
                        "required": ["date", "time"]
                    },
                    "filters": {
                        "type": "object",
                        "description": "Filters for reviewing appointments",
                        "properties": {
                            "start_date": {"type": "string", "format": "date"},
                            "end_date": {"type": "string", "format": "date"},
                            "provider_uuid": {"type": "string"},
                            "status": {
                                "type": "string",
                                "enum": ["scheduled", "checked_in", "completed", "cancelled", "missed"]
                            }
                        }
                    }
                },
                "required": ["action"]
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "action": {"type": "string"},
                    "success": {"type": "boolean"},
                    "appointments": {"type": "array"},
                    "appointment_id": {"type": "string"},
                    "message": {"type": "string"}
                }
            }
        }
    
    async def invoke(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute appointment action."""
        self.validate_input(params)
        
        action = params["action"]
        
        if action == "review":
            return await self._review_appointments(params)
        elif action == "schedule":
            return await self._schedule_appointment(params)
        else:
            return {
                "action": action,
                "success": False,
                "message": f"Unknown action: {action}"
            }
    
    async def _review_appointments(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Review existing appointments."""
        
        if not self.rest_base:
            # Return mock data
            return self._get_mock_appointments(params)
        
        try:
            # Build query parameters
            query_params = {}
            
            if params.get("patient_id"):
                query_params["patient"] = params["patient_id"]
            
            filters = params.get("filters", {})
            if filters.get("start_date"):
                query_params["fromDate"] = filters["start_date"]
            if filters.get("end_date"):
                query_params["toDate"] = filters["end_date"]
            if filters.get("status"):
                query_params["status"] = filters["status"]
            
            # OpenMRS appointment endpoint (this may vary by installation)
            # Standard endpoint might be /appointmentscheduling/appointment
            url = f"{self.rest_base}/appointmentscheduling/appointment"
            
            response = await self.client.get(url, params=query_params)
            
            if response.status_code == 200:
                data = response.json()
                appointments = data.get("results", [])
                
                # Format appointments for output
                formatted = []
                for apt in appointments:
                    formatted.append({
                        "id": apt.get("uuid"),
                        "patient": apt.get("patient", {}).get("display"),
                        "date": apt.get("timeSlot", {}).get("startDate"),
                        "provider": apt.get("provider", {}).get("display"),
                        "service": apt.get("appointmentType", {}).get("display"),
                        "status": apt.get("status"),
                        "reason": apt.get("reason")
                    })
                
                return {
                    "action": "review",
                    "success": True,
                    "appointments": formatted,
                    "total": len(formatted),
                    "message": f"Found {len(formatted)} appointments"
                }
            else:
                return {
                    "action": "review",
                    "success": False,
                    "message": f"Failed to retrieve appointments: HTTP {response.status_code}"
                }
        
        except Exception as e:
            logger.error(f"Error reviewing appointments: {e}")
            return {
                "action": "review",
                "success": False,
                "message": str(e)
            }
    
    async def _schedule_appointment(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Schedule a new appointment."""
        
        if not self.rest_base:
            # Return mock success
            return {
                "action": "schedule",
                "success": True,
                "appointment_id": "mock-apt-123",
                "message": "Mock appointment scheduled (no OpenMRS connection)"
            }
        
        try:
            details = params.get("appointment_details", {})
            
            # Combine date and time
            apt_datetime = f"{details['date']}T{details['time']}:00"
            
            # Calculate end time based on duration
            duration = details.get("duration_minutes", 30)
            
            # Build appointment request object
            # Note: Actual OpenMRS structure may vary
            appointment_data = {
                "patient": params.get("patient_id"),
                "appointmentType": details.get("service", "General Consultation"),
                "startDateTime": apt_datetime,
                "endDateTime": apt_datetime,  # Would calculate based on duration
                "provider": details.get("provider_uuid"),
                "location": details.get("location_uuid"),
                "reason": details.get("reason", ""),
                "status": "Scheduled"
            }
            
            url = f"{self.rest_base}/appointmentscheduling/appointment"
            
            response = await self.client.post(url, json=appointment_data)
            
            if response.status_code in [200, 201]:
                created = response.json()
                return {
                    "action": "schedule",
                    "success": True,
                    "appointment_id": created.get("uuid"),
                    "message": f"Appointment scheduled for {details['date']} at {details['time']}"
                }
            else:
                return {
                    "action": "schedule",
                    "success": False,
                    "message": f"Failed to schedule: HTTP {response.status_code}"
                }
        
        except Exception as e:
            logger.error(f"Error scheduling appointment: {e}")
            return {
                "action": "schedule",
                "success": False,
                "message": str(e)
            }
    
    def _get_mock_appointments(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Return mock appointments for testing."""
        
        # Generate some mock appointments
        today = datetime.now()
        mock_appointments = [
            {
                "id": "apt-001",
                "patient": "John Doe",
                "date": (today + timedelta(days=1)).strftime("%Y-%m-%d 09:00"),
                "provider": "Dr. Smith",
                "service": "Follow-up",
                "status": "scheduled",
                "reason": "Diabetes follow-up"
            },
            {
                "id": "apt-002",
                "patient": "Jane Smith",
                "date": (today + timedelta(days=3)).strftime("%Y-%m-%d 14:30"),
                "provider": "Dr. Jones",
                "service": "Consultation",
                "status": "scheduled",
                "reason": "Hypertension management"
            }
        ]
        
        # Filter by patient if specified
        if params.get("patient_id"):
            mock_appointments = [a for a in mock_appointments 
                                if params["patient_id"] in a["patient"]]
        
        return {
            "action": "review",
            "success": True,
            "appointments": mock_appointments,
            "total": len(mock_appointments),
            "message": f"Mock data - {len(mock_appointments)} appointments"
        }
    
    async def cleanup(self):
        """Clean up HTTP client."""
        await self.client.aclose()
