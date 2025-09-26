#!/usr/bin/env python3
"""
Direct tests for MCP tools without agent layer.
Useful for debugging and verifying tool functionality.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.mcp.base import MCPToolRegistry
from server.mcp.spark_tools import SparkPopulationAnalyticsTool, SparkPatientLongitudinalTool
from server.mcp.spark_tools import SparkCapabilitiesTool
from server.mcp.fhir_tool import FHIRSearchTool
from server.mcp.medical_search_tool import MedicalSearchTool
from server.mcp.appointment_tool import AppointmentTool

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def test_spark_capabilities_tool():
    """Test Spark capabilities tool."""
    logger.info("\n" + "="*60)
    logger.info("Testing Spark Capabilities Tool")
    logger.info("="*60)

    tool = SparkCapabilitiesTool()
    result = await tool.safe_invoke({})

    if result["success"]:
        data = result["result"]
        logger.info("✅ Success!")
        logger.info(f"  - Profile: {data.get('profile')}")
        views = data.get("views", {})
        feats = data.get("features", {})
        logger.info(f"  - Views defined: {list(views.keys())}")
        logger.info(f"  - Features: {feats}")
        # Basic assertions: profile exists; features dict present
        ok = isinstance(data.get("profile"), str) and isinstance(feats, dict)
        return ok
    else:
        logger.error(f"❌ Failed: {result['error']}")
        return False


async def test_spark_population_tool():
    """Test Spark population analytics tool."""
    logger.info("\n" + "="*60)
    logger.info("Testing Spark Population Analytics Tool")
    logger.info("="*60)
    
    tool = SparkPopulationAnalyticsTool()
    
    # Test prevalence query
    params = {
        "analysis_type": "prevalence",
        "condition": "diabetes"
    }
    
    logger.info(f"Query: {params}")
    result = await tool.safe_invoke(params)
    
    if result["success"]:
        data = result["result"]
        logger.info(f"✅ Success!")
        logger.info(f"  - Row count: {data.get('row_count', 0)}")
        logger.info(f"  - Summary: {data.get('summary', 'N/A')}")
        logger.info(f"  - Query: {data.get('query_executed', 'N/A')[:100]}...")
        if data.get("results"):
            logger.info(f"  - Sample results: {data['results'][:2]}")
    else:
        logger.error(f"❌ Failed: {result['error']}")
    
    return result["success"]


async def test_spark_longitudinal_tool():
    """Test Spark patient longitudinal tool."""
    logger.info("\n" + "="*60)
    logger.info("Testing Spark Patient Longitudinal Tool")
    logger.info("="*60)
    
    tool = SparkPatientLongitudinalTool()
    
    params = {
        "patient_id": "pat-123",
        "format": "summary",
        "sections": ["demographics", "conditions", "medications"]
    }
    
    logger.info(f"Query: {params}")
    result = await tool.safe_invoke(params)
    
    if result["success"]:
        data = result["result"]
        logger.info(f"✅ Success!")
        logger.info(f"  - Patient: {data.get('patient_id')}")
        logger.info(f"  - Format: {data.get('format')}")
        logger.info(f"  - Sections: {data.get('sections_included', [])}")
        logger.info(f"  - Total records: {data.get('record_count', 0)}")
    else:
        logger.error(f"❌ Failed: {result['error']}")
    
    return result["success"]


async def test_fhir_search_tool():
    """Test FHIR search tool."""
    logger.info("\n" + "="*60)
    logger.info("Testing FHIR Search Tool")
    logger.info("="*60)
    
    tool = FHIRSearchTool()
    
    # Test patient search
    params = {
        "resource_type": "Patient",
        "patient_id": "example-123",
        "operation": "read"
    }
    
    logger.info(f"Query: {params}")
    result = await tool.safe_invoke(params)
    
    if result["success"]:
        data = result["result"]
        logger.info(f"✅ Success!")
        logger.info(f"  - Resource type: {data.get('resource_type')}")
        logger.info(f"  - Total results: {data.get('total', 0)}")
        if data.get("entries"):
            logger.info(f"  - First entry: {data['entries'][0]}")
    else:
        logger.error(f"❌ Failed: {result['error']}")
    
    # Test observation search
    logger.info("\n--- Testing Observation search ---")
    params2 = {
        "resource_type": "Observation",
        "patient_id": "example-123",
        "search_params": {"_count": 5}
    }
    
    logger.info(f"Query: {params2}")
    result2 = await tool.safe_invoke(params2)
    
    if result2["success"]:
        logger.info(f"✅ Observation search succeeded")
    else:
        logger.error(f"❌ Observation search failed")
    
    # Cleanup
    await tool.cleanup()
    
    return result["success"]


async def test_medical_search_tool():
    """Test medical search tool."""
    logger.info("\n" + "="*60)
    logger.info("Testing Medical Search Tool")
    logger.info("="*60)
    
    tool = MedicalSearchTool()
    
    params = {
        "query": "metformin diabetes treatment",
        "search_type": "literature",
        "max_results": 5
    }
    
    logger.info(f"Query: {params}")
    result = await tool.safe_invoke(params)
    
    if result["success"]:
        data = result["result"]
        logger.info(f"✅ Success!")
        logger.info(f"  - Search type: {data.get('search_type')}")
        logger.info(f"  - Total found: {data.get('total_found', 0)}")
        logger.info(f"  - Message: {data.get('message', 'N/A')}")
        if data.get("results"):
            logger.info(f"  - First result: {data['results'][0].get('title', 'N/A')}")
    else:
        logger.error(f"❌ Failed: {result['error']}")
    
    return result["success"]


async def test_appointment_tool():
    """Test appointment management tool."""
    logger.info("\n" + "="*60)
    logger.info("Testing Appointment Tool")
    logger.info("="*60)
    
    tool = AppointmentTool()
    
    # Test review appointments
    logger.info("\n--- Testing Review Appointments ---")
    params = {
        "action": "review",
        "filters": {
            "start_date": datetime.now().strftime("%Y-%m-%d"),
            "end_date": (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        }
    }
    
    logger.info(f"Query: {params}")
    result = await tool.safe_invoke(params)
    
    if result["success"]:
        data = result["result"]
        logger.info(f"✅ Success!")
        logger.info(f"  - Action: {data.get('action')}")
        logger.info(f"  - Total appointments: {data.get('total', 0)}")
        if data.get("appointments"):
            logger.info(f"  - First appointment: {data['appointments'][0]}")
    else:
        logger.error(f"❌ Failed: {result['error']}")
    
    # Test schedule appointment
    logger.info("\n--- Testing Schedule Appointment ---")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    params2 = {
        "action": "schedule",
        "patient_id": "pat-789",
        "appointment_details": {
            "date": tomorrow,
            "time": "14:30",
            "service": "Follow-up Consultation",
            "reason": "Diabetes management review"
        }
    }
    
    logger.info(f"Query: {params2}")
    result2 = await tool.safe_invoke(params2)
    
    if result2["success"]:
        data = result2["result"]
        logger.info(f"✅ Success!")
        logger.info(f"  - Appointment ID: {data.get('appointment_id', 'N/A')}")
        logger.info(f"  - Message: {data.get('message', 'N/A')}")
    else:
        logger.error(f"❌ Failed: {result2['error']}")
    
    # Cleanup
    await tool.cleanup()
    
    return result["success"] and result2["success"]


async def test_tool_registry():
    """Test the MCP tool registry."""
    logger.info("\n" + "="*60)
    logger.info("Testing MCP Tool Registry")
    logger.info("="*60)
    
    registry = MCPToolRegistry()
    
    # Register all tools
    registry.register(SparkCapabilitiesTool())
    registry.register(SparkPopulationAnalyticsTool())
    registry.register(SparkPatientLongitudinalTool())
    registry.register(FHIRSearchTool())
    registry.register(MedicalSearchTool())
    registry.register(AppointmentTool())
    
    # List all tools
    tools = registry.list_tools()
    logger.info(f"Registered {len(tools)} tools:")
    for name, schema in tools.items():
        logger.info(f"  - {name}: {schema.get('description', 'No description')}")
    
    # Test retrieval
    spark_tool = registry.get_tool("spark_population_analytics")
    if spark_tool:
        logger.info(f"✅ Successfully retrieved spark_population_analytics tool")
    else:
        logger.error(f"❌ Failed to retrieve spark_population_analytics tool")
    
    return len(tools) == 6


async def main():
    """Run all direct tool tests."""
    logger.info("\n" + "="*80)
    logger.info(" DIRECT MCP TOOL TESTS")
    logger.info("="*80)
    logger.info("\nThese tests verify MCP tools work independently of agents.")
    logger.info("Mock data will be returned if external services are not configured.\n")
    
    results = []
    
    # Run each test
    tests = [
        ("Tool Registry", test_tool_registry),
        ("Spark Capabilities", test_spark_capabilities_tool),
        ("Spark Population Analytics", test_spark_population_tool),
        ("Spark Patient Longitudinal", test_spark_longitudinal_tool),
        ("FHIR Search", test_fhir_search_tool),
        ("Medical Search", test_medical_search_tool),
        ("Appointment Management", test_appointment_tool)
    ]
    
    for test_name, test_func in tests:
        try:
            success = await test_func()
            results.append((test_name, success))
        except Exception as e:
            logger.error(f"Test {test_name} failed with exception: {e}")
            results.append((test_name, False))
        
        await asyncio.sleep(0.5)  # Brief pause between tests
    
    # Print summary
    logger.info("\n" + "="*80)
    logger.info(" TEST SUMMARY")
    logger.info("="*80)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        logger.info(f"{status} - {test_name}")
    
    logger.info(f"\nResults: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("\n🎉 All MCP tools are working correctly!")
    else:
        logger.warning(f"\n⚠️ {total - passed} tools need attention.")
    
    return 0 if passed == total else 1


if __name__ == "__main__":
    # Load environment
    env_file = os.getenv("ENV_FILE", ".env")
    if os.path.exists(env_file):
        from dotenv import load_dotenv
        load_dotenv(env_file)
        logger.info(f"Loaded environment from {env_file}")
    # Default to test-data mode unless explicitly disabled
    if os.getenv("MCP_USE_TEST_DATA") is None:
        os.environ["MCP_USE_TEST_DATA"] = "1"
        logger.info("MCP_USE_TEST_DATA not set; enabling test-data mode for deterministic runs")
    # Log active Spark profile for visibility
    logger.info(f"Active SPARK_PROFILE: {os.getenv('SPARK_PROFILE', 'parquet_on_fhir_flat')}")
    
    # Run tests
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
