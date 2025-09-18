"""
MCP Tools for Spark-based analytics on Parquet-on-FHIR data.
"""

from typing import Dict, Any, Optional, List
from .base import MCPTool
import logging
import os

logger = logging.getLogger(__name__)

# Check if PyHive is available
try:
    from pyhive import hive
    PYHIVE_AVAILABLE = True
except ImportError:
    PYHIVE_AVAILABLE = False
    logger.warning("PyHive not installed - Spark tools will use mock data")


class SparkPopulationAnalyticsTool(MCPTool):
    """MCP tool for population-level health analytics via Spark SQL."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Spark connection.
        
        Args:
            config: Dictionary with 'host', 'port', 'database' keys
        """
        self.config = config or {
            'host': os.getenv('SPARK_THRIFT_HOST', 'localhost'),
            'port': int(os.getenv('SPARK_THRIFT_PORT', '10001')),
            'database': os.getenv('SPARK_THRIFT_DATABASE', 'default')
        }
        
        self.connection = None
        if PYHIVE_AVAILABLE and self.config.get('host'):
            try:
                self.connection = hive.Connection(
                    host=self.config['host'],
                    port=self.config['port'],
                    database=self.config['database']
                )
                logger.info(f"Connected to Spark at {self.config['host']}:{self.config['port']}")
            except Exception as e:
                logger.error(f"Failed to connect to Spark: {e}")
    
    @property
    def name(self) -> str:
        return "spark_population_analytics"
    
    @property
    def schema(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": "Query population-level health statistics from Spark",
            "input_schema": {
                "type": "object",
                "properties": {
                    "analysis_type": {
                        "type": "string",
                        "enum": ["prevalence", "trends", "demographics", "comorbidities", "custom"],
                        "description": "Type of population analysis"
                    },
                    "condition": {
                        "type": "string",
                        "description": "Condition name or ICD code to analyze"
                    },
                    "timeframe": {
                        "type": "string",
                        "enum": ["all_time", "last_year", "last_month", "last_week", "custom"],
                        "default": "all_time"
                    },
                    "filters": {
                        "type": "object",
                        "properties": {
                            "age_min": {"type": "integer"},
                            "age_max": {"type": "integer"},
                            "gender": {"type": "string", "enum": ["male", "female", "other"]},
                            "facility_id": {"type": "string"}
                        }
                    },
                    "custom_sql": {
                        "type": "string",
                        "description": "Custom SQL query (only for analysis_type='custom')"
                    }
                },
                "required": ["analysis_type"]
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "results": {"type": "array"},
                    "summary": {"type": "string"},
                    "row_count": {"type": "integer"},
                    "query_executed": {"type": "string"}
                }
            }
        }
    
    async def invoke(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute population analytics query."""
        self.validate_input(params)
        
        # Build SQL query based on analysis type
        sql = self._build_query(params)
        
        # Execute query
        if self.connection:
            try:
                cursor = self.connection.cursor()
                cursor.execute(sql)
                results = cursor.fetchall()
                
                return {
                    "results": results,
                    "summary": self._generate_summary(params, results),
                    "row_count": len(results),
                    "query_executed": sql
                }
            except Exception as e:
                logger.error(f"Query execution failed: {e}")
                return {
                    "error": str(e),
                    "query_attempted": sql
                }
        else:
            # Return mock data for testing
            return self._get_mock_results(params)
    
    def _build_query(self, params: Dict[str, Any]) -> str:
        """Build SQL query based on analysis type."""
        analysis_type = params["analysis_type"]
        
        if analysis_type == "custom":
            return params.get("custom_sql", "SELECT 1")
        
        # Base query components
        condition_filter = ""
        if params.get("condition"):
            condition_filter = f"WHERE code LIKE '%{params['condition']}%'"
        
        # Time filter
        time_filter = self._build_time_filter(params.get("timeframe", "all_time"))
        
        # Build query based on type
        if analysis_type == "prevalence":
            return f"""
                SELECT 
                    code,
                    COUNT(DISTINCT patient_id) as patient_count,
                    COUNT(*) as condition_instances
                FROM condition
                {condition_filter}
                GROUP BY code
                ORDER BY patient_count DESC
                LIMIT 20
            """
        
        elif analysis_type == "trends":
            return f"""
                SELECT 
                    DATE_TRUNC('month', onsetDateTime) as month,
                    COUNT(DISTINCT patient_id) as patient_count,
                    COUNT(*) as total_cases
                FROM condition
                {condition_filter}
                {time_filter}
                GROUP BY month
                ORDER BY month DESC
                LIMIT 12
            """
        
        elif analysis_type == "demographics":
            return f"""
                SELECT 
                    p.gender,
                    FLOOR(DATEDIFF(CURRENT_DATE, p.birthDate) / 365) as age,
                    COUNT(DISTINCT c.patient_id) as patient_count
                FROM condition c
                JOIN patient p ON c.patient_id = p.id
                {condition_filter}
                GROUP BY p.gender, age
                ORDER BY patient_count DESC
            """
        
        elif analysis_type == "comorbidities":
            condition = params.get("condition", "diabetes")
            return f"""
                WITH target_patients AS (
                    SELECT DISTINCT patient_id
                    FROM condition
                    WHERE code LIKE '%{condition}%'
                )
                SELECT 
                    c.code,
                    COUNT(DISTINCT c.patient_id) as patient_count
                FROM condition c
                WHERE c.patient_id IN (SELECT patient_id FROM target_patients)
                  AND c.code NOT LIKE '%{condition}%'
                GROUP BY c.code
                ORDER BY patient_count DESC
                LIMIT 10
            """
        
        return "SELECT 1"
    
    def _build_time_filter(self, timeframe: str) -> str:
        """Build SQL time filter clause."""
        if timeframe == "last_year":
            return "WHERE onsetDateTime >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)"
        elif timeframe == "last_month":
            return "WHERE onsetDateTime >= DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)"
        elif timeframe == "last_week":
            return "WHERE onsetDateTime >= DATE_SUB(CURRENT_DATE, INTERVAL 1 WEEK)"
        return ""
    
    def _generate_summary(self, params: Dict[str, Any], results: List) -> str:
        """Generate human-readable summary of results."""
        analysis_type = params["analysis_type"]
        condition = params.get("condition", "specified condition")
        
        if not results:
            return f"No data found for {condition}"
        
        if analysis_type == "prevalence":
            total = sum(r[1] for r in results if len(r) > 1)
            return f"Found {total} patients with {condition} across {len(results)} condition codes"
        elif analysis_type == "trends":
            return f"Retrieved {len(results)} months of trend data for {condition}"
        elif analysis_type == "demographics":
            return f"Demographic breakdown for {condition} across {len(results)} groups"
        
        return f"Analysis completed with {len(results)} results"
    
    def _get_mock_results(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Return mock results for testing."""
        analysis_type = params["analysis_type"]
        
        if analysis_type == "prevalence":
            mock_data = [
                ("E11.9", 1250, 3500),  # Type 2 diabetes
                ("I10", 2100, 5200),    # Hypertension
                ("J45.909", 450, 890)    # Asthma
            ]
        elif analysis_type == "trends":
            mock_data = [
                ("2024-01", 120),
                ("2024-02", 135),
                ("2024-03", 128)
            ]
        else:
            mock_data = [("Mock", "Data", 100)]
        
        return {
            "results": mock_data,
            "summary": "Mock data - Spark not connected",
            "row_count": len(mock_data),
            "query_executed": "MOCK QUERY"
        }


class SparkPatientLongitudinalTool(MCPTool):
    """MCP tool for retrieving complete patient longitudinal health records."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize with Spark connection config."""
        self.config = config or {
            'host': os.getenv('SPARK_THRIFT_HOST', 'localhost'),
            'port': int(os.getenv('SPARK_THRIFT_PORT', '10001')),
            'database': os.getenv('SPARK_THRIFT_DATABASE', 'default')
        }
        
        self.connection = None
        if PYHIVE_AVAILABLE and self.config.get('host'):
            try:
                self.connection = hive.Connection(
                    host=self.config['host'],
                    port=self.config['port'],
                    database=self.config['database']
                )
            except Exception as e:
                logger.error(f"Failed to connect to Spark: {e}")
    
    @property
    def name(self) -> str:
        return "spark_patient_longitudinal"
    
    @property
    def schema(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": "Retrieve comprehensive longitudinal health record for a patient",
            "input_schema": {
                "type": "object",
                "properties": {
                    "patient_id": {
                        "type": "string",
                        "description": "Patient identifier"
                    },
                    "format": {
                        "type": "string",
                        "enum": ["ips", "timeline", "summary", "full"],
                        "default": "summary",
                        "description": "Output format for the health record"
                    },
                    "sections": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["demographics", "conditions", "medications", "observations", "encounters", "procedures"]
                        },
                        "description": "Sections to include (default: all)"
                    },
                    "date_range": {
                        "type": "object",
                        "properties": {
                            "start": {"type": "string", "format": "date"},
                            "end": {"type": "string", "format": "date"}
                        }
                    }
                },
                "required": ["patient_id"]
            }
        }
    
    async def invoke(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve patient longitudinal record."""
        self.validate_input(params)
        
        patient_id = params["patient_id"]
        format_type = params.get("format", "summary")
        sections = params.get("sections", ["demographics", "conditions", "medications", "observations", "encounters"])
        
        # Execute queries for each section
        record = {}
        
        for section in sections:
            sql = self._build_section_query(section, patient_id, params.get("date_range"))
            
            if self.connection:
                try:
                    cursor = self.connection.cursor()
                    cursor.execute(sql)
                    record[section] = cursor.fetchall()
                except Exception as e:
                    logger.error(f"Failed to fetch {section}: {e}")
                    record[section] = []
            else:
                # Mock data
                record[section] = self._get_mock_section_data(section)
        
        # Format the record based on requested format
        formatted = self._format_record(record, format_type)
        
        return {
            "patient_id": patient_id,
            "format": format_type,
            "record": formatted,
            "sections_included": list(record.keys()),
            "record_count": sum(len(v) for v in record.values())
        }
    
    def _build_section_query(self, section: str, patient_id: str, date_range: Optional[Dict]) -> str:
        """Build query for specific record section."""
        date_filter = ""
        if date_range:
            if date_range.get("start"):
                date_filter += f" AND effectiveDateTime >= '{date_range['start']}'"
            if date_range.get("end"):
                date_filter += f" AND effectiveDateTime <= '{date_range['end']}'"
        
        queries = {
            "demographics": f"""
                SELECT id, gender, birthDate, deceased, address_city, address_state
                FROM patient
                WHERE id = '{patient_id}'
            """,
            
            "conditions": f"""
                SELECT code, clinicalStatus, onsetDateTime, abatementDateTime
                FROM condition
                WHERE patient_id = '{patient_id}'
                ORDER BY onsetDateTime DESC
            """,
            
            "medications": f"""
                SELECT medication, dosage, status, authoredOn
                FROM medication_request
                WHERE patient_id = '{patient_id}'
                ORDER BY authoredOn DESC
            """,
            
            "observations": f"""
                SELECT code, value, unit, effectiveDateTime
                FROM observation
                WHERE patient_id = '{patient_id}' {date_filter}
                ORDER BY effectiveDateTime DESC
                LIMIT 100
            """,
            
            "encounters": f"""
                SELECT type, period_start, period_end, reasonCode
                FROM encounter
                WHERE patient_id = '{patient_id}'
                ORDER BY period_start DESC
                LIMIT 50
            """,
            
            "procedures": f"""
                SELECT code, performedDateTime, outcome
                FROM procedure
                WHERE patient_id = '{patient_id}'
                ORDER BY performedDateTime DESC
            """
        }
        
        return queries.get(section, f"SELECT 1")
    
    def _format_record(self, record: Dict, format_type: str) -> Any:
        """Format the patient record based on requested format."""
        if format_type == "full":
            return record
        
        elif format_type == "summary":
            # Create a summarized view
            summary = {
                "patient_info": record.get("demographics", []),
                "active_conditions": [c for c in record.get("conditions", []) if len(c) > 1 and c[1] == "active"],
                "current_medications": record.get("medications", [])[:5],
                "recent_observations": record.get("observations", [])[:10],
                "recent_encounters": record.get("encounters", [])[:3]
            }
            return summary
        
        elif format_type == "timeline":
            # Create chronological timeline
            timeline = []
            
            # Add conditions with dates
            for condition in record.get("conditions", []):
                if len(condition) > 2:
                    timeline.append({
                        "date": condition[2],
                        "type": "condition",
                        "data": condition
                    })
            
            # Add observations
            for obs in record.get("observations", []):
                if len(obs) > 3:
                    timeline.append({
                        "date": obs[3],
                        "type": "observation",
                        "data": obs
                    })
            
            # Sort by date
            timeline.sort(key=lambda x: x["date"], reverse=True)
            return timeline[:50]  # Return most recent 50 events
        
        elif format_type == "ips":
            # International Patient Summary format
            return {
                "patient": record.get("demographics", []),
                "problems": record.get("conditions", []),
                "medications": record.get("medications", []),
                "allergies": [],  # Would need allergy table
                "immunizations": [],  # Would need immunization table
                "results": record.get("observations", [])[:20]
            }
        
        return record
    
    def _get_mock_section_data(self, section: str) -> List:
        """Return mock data for testing."""
        mock_data = {
            "demographics": [("pat-123", "male", "1980-05-15", False, "Boston", "MA")],
            "conditions": [
                ("E11.9", "active", "2020-03-15", None),
                ("I10", "active", "2019-01-20", None)
            ],
            "medications": [
                ("Metformin 500mg", "Twice daily", "active", "2024-01-15"),
                ("Lisinopril 10mg", "Once daily", "active", "2024-01-15")
            ],
            "observations": [
                ("HbA1c", "7.2", "%", "2024-03-01"),
                ("Blood Pressure", "130/85", "mmHg", "2024-03-01")
            ],
            "encounters": [
                ("office-visit", "2024-03-01", "2024-03-01", "Routine checkup")
            ]
        }
        return mock_data.get(section, [])
