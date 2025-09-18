#!/usr/bin/env python3
"""
Configuration test for the Medical Multi-Agent Chat System.
Tests minimal setup and optional Gemini orchestration.

Usage:
  python test_config.py                    # Test current configuration (.env)
  python test_config.py minimal            # Test with minimal config
  python test_config.py gemini             # Test with Gemini orchestration
  python test_config.py --env-file <path>  # Test with custom .env file
"""

import os
import sys
import socket

def check_env_file(env_file=".env"):
    """Check if .env file exists."""
    if not os.path.exists(env_file):
        print(f"❌ No {env_file} file found!")
        if env_file == ".env":
            print("\n📝 To create one:")
            print("  cp env.example .env")
            print("  echo 'LLM_BASE_URL=http://localhost:1234' >> .env")
        else:
            print(f"\n📝 Make sure {env_file} exists and is accessible")
        return False
    return True

def test_current_config(env_file=".env"):
    """Test the current configuration from .env file."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        print("❌ python-dotenv not installed!")
        print("\n📝 Install dependencies with Poetry:")
        print("  poetry install")
        print("\nThen run this script with:")
        print("  poetry run python test_config.py")
        return False
    
    # Load environment variables from specified file
    load_dotenv(env_file)
    
    print("=" * 50)
    
    errors = []
    warnings = []
    success = []
    
    # Test required settings
    llm_base_url = os.getenv("LLM_BASE_URL")
    if llm_base_url:
        success.append(f"✅ LLM endpoint: {llm_base_url}")
    else:
        errors.append("❌ LLM_BASE_URL not set - this is required!")
    
    # Test models (with defaults)
    general_model = os.getenv("GENERAL_MODEL", "llama-3-8b-instruct")
    med_model = os.getenv("MED_MODEL", general_model)
    success.append(f"✅ General model: {general_model}")
    if med_model == general_model:
        success.append(f"✅ Medical model: {med_model} (same as general)")
    else:
        success.append(f"✅ Medical model: {med_model}")
    
    # Test orchestrator
    orchestrator = os.getenv("ORCHESTRATOR_PROVIDER", "openai")
    if orchestrator == "gemini":
        gemini_key = os.getenv("GEMINI_API_KEY")
        if gemini_key:
            success.append(f"✅ Gemini orchestrator configured")
            success.append(f"   Model: {os.getenv('ORCHESTRATOR_MODEL', 'gemini-1.5-flash')}")
        else:
            errors.append("❌ GEMINI_API_KEY required when using Gemini orchestrator")
    else:
        success.append(f"✅ Using local LLM for orchestration")
    
    # Test optional data sources presence
    fhir_url = os.getenv("OPENMRS_FHIR_BASE_URL")
    if fhir_url:
        success.append(f"✅ FHIR server configured: {fhir_url}")
    
    parquet_dir = os.getenv("FHIR_PARQUET_DIR")
    if parquet_dir:
        if os.path.exists(parquet_dir):
            success.append(f"✅ Local FHIR data: {parquet_dir}")
        else:
            warnings.append(f"⚠️  FHIR_PARQUET_DIR set but path doesn't exist")
    
    # Test Spark Thrift connectivity (TCP; optional PyHive validation)
    spark_host = os.getenv("SPARK_THRIFT_HOST")
    spark_port = int(os.getenv("SPARK_THRIFT_PORT", "10001"))
    if spark_host:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        tcp_ok = sock.connect_ex((spark_host, spark_port)) == 0
        sock.close()
        if tcp_ok:
            # Try PyHive if available
            try:
                from pyhive import hive  # type: ignore
                conn = hive.Connection(host=spark_host, port=spark_port, username="test", auth="NONE")
                cur = conn.cursor()
                cur.execute("SHOW DATABASES")
                cur.fetchall()
                conn.close()
                success.append(f"✅ Spark Thrift reachable via PyHive at {spark_host}:{spark_port}")
            except Exception:
                success.append(f"✅ Spark Thrift TCP open at {spark_host}:{spark_port} (PyHive check skipped/failed)")
        else:
            errors.append(f"❌ Spark Thrift not reachable at {spark_host}:{spark_port}")
    else:
        warnings.append("⚠️  SPARK_THRIFT_HOST not set; skipping Spark connectivity check")

    # Test FHIR HTTP reachability
    if fhir_url:
        try:
            import requests  # type: ignore
            # Try base URL; if fails, try metadata
            resp = requests.get(fhir_url, timeout=5)
            if not (200 <= resp.status_code < 400):
                meta_url = fhir_url.rstrip('/') + '/metadata'
                resp2 = requests.get(meta_url, timeout=5)
                if 200 <= resp2.status_code < 400:
                    success.append(f"✅ FHIR reachable at {meta_url} (status {resp2.status_code})")
                else:
                    errors.append(f"❌ FHIR endpoint not reachable (status {resp.status_code}, metadata {resp2.status_code})")
            else:
                success.append(f"✅ FHIR reachable at {fhir_url} (status {resp.status_code})")
        except Exception as e:
            errors.append(f"❌ FHIR connectivity check failed: {e}")

    # Test agent ports availability (optional when using honcho)
    ports_to_check = [
        ("Router", 9100),
        ("MedGemma", 9101),
        ("Clinical", 9102)
    ]
    
    for name, port in ports_to_check:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('localhost', port))
        sock.close()
        
        if result == 0:
            warnings.append(f"⚠️  Port {port} ({name}) already in use")
        else:
            success.append(f"✅ Port {port} available for {name}")
    
    # Print results
    print("\n📋 Results:")
    print("=" * 50)
    
    if success:
        for msg in success:
            print(f"  {msg}")
    
    if warnings:
        print("\n⚠️  Warnings:")
        for msg in warnings:
            print(f"  {msg}")
    
    if errors:
        print("\n❌ Errors:")
        for msg in errors:
            print(f"  {msg}")
        return False
    
    # Test LLM connectivity (informational)
    if llm_base_url:
        print("\n🔗 Testing LLM connectivity...")
        try:
            import requests
            response = requests.get(f"{llm_base_url}/v1/models", timeout=5)
            if response.status_code == 200:
                print(f"  ✅ Connected to LLM at {llm_base_url}")
            else:
                print(f"  ⚠️  LLM responded with status {response.status_code}")
        except ImportError:
            print("  ℹ️  Install 'requests' to test LLM connectivity")
        except requests.exceptions.ConnectionError:
            print(f"  ❌ Cannot connect to {llm_base_url}")
            print(f"     Make sure LM Studio is running")
        except Exception as e:
            print(f"  ⚠️  Error: {e}")
    
    print("\n" + "=" * 50)
    if not errors:
        print("✅ Configuration valid! Run: python launch_a2a_agents.py")
    else:
        print("❌ Fix errors above before launching agents")
    
    return not bool(errors)

def test_minimal_config():
    """Test with absolute minimal configuration."""
    print("🧪 Testing Minimal Configuration")
    print("=" * 50)
    print("\nSimulating minimal .env with just:")
    print("  LLM_BASE_URL=http://localhost:1234\n")
    
    # Clear environment and set minimal config
    for key in list(os.environ.keys()):
        if key.startswith(('LLM_', 'GENERAL_', 'MED_', 'ORCHESTRATOR_', 'GEMINI_')):
            del os.environ[key]
    
    os.environ["LLM_BASE_URL"] = "http://localhost:1234"
    
    # Import config to test defaults
    try:
        from server import config
        print("✅ Config module loaded with minimal settings")
        print("\n📋 Smart defaults applied:")
        print(f"  - GENERAL_MODEL: {config.GENERAL_MODEL}")
        print(f"  - MED_MODEL: {config.MED_MODEL}")
        print(f"  - ORCHESTRATOR: {config.ORCHESTRATOR_PROVIDER} (local)")
        print(f"  - TEMPERATURE: {config.LLM_TEMPERATURE}")
        print(f"  - TIMEOUT: {config.CHAT_TIMEOUT_SECONDS}s")
        print(f"  - LOG_LEVEL: {config.LOG_LEVEL}")
        print("\n✅ System works with just 1 line of configuration!")
        return True
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        return False

def test_gemini_config():
    """Test configuration with Gemini orchestrator."""
    print("\n🧪 Testing Gemini Orchestrator Configuration")
    print("=" * 50)
    print("\nSimulating .env with Gemini settings:\n")
    
    # Set Gemini configuration
    os.environ["LLM_BASE_URL"] = "http://localhost:1234"
    os.environ["ORCHESTRATOR_PROVIDER"] = "gemini"
    os.environ["GEMINI_API_KEY"] = "test-key-abc123"
    os.environ["ORCHESTRATOR_MODEL"] = "gemini-1.5-flash"
    
    try:
        # Reload config module
        import importlib
        from server import config
        importlib.reload(config)
        
        if config.ORCHESTRATOR_PROVIDER == "gemini":
            print("✅ Gemini orchestrator configured")
            print(f"  - Model: {config.ORCHESTRATOR_MODEL}")
            print(f"  - Local agents still use: {config.GENERAL_MODEL}")
            print("\n📌 Benefits:")
            print("  - Fast, intelligent routing via Gemini")
            print("  - Private, local processing via LM Studio")
            print("  - Best of both worlds!")
            return True
        else:
            print("❌ Gemini configuration failed")
            return False
    except ValueError as e:
        if "GEMINI_API_KEY required" in str(e):
            print("✅ Validation working: API key required for Gemini")
            return True
        raise
    except Exception as e:
        print(f"❌ Failed: {e}")
        return False

def main():
    """Main test function."""
    env_file = ".env"
    mode = None
    
    # Parse command line arguments
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--env-file":
            if i + 1 >= len(args):
                print("❌ --env-file requires a path argument")
                return False
            env_file = args[i + 1]
            i += 2
        elif arg.lower() in ["minimal", "gemini"]:
            mode = arg.lower()
            i += 1
        else:
            print(f"Unknown argument: {arg}")
            print("Usage: python test_config.py [minimal|gemini] [--env-file <path>]")
            return False
    
    if mode == "minimal":
        success = test_minimal_config()
    elif mode == "gemini":
        success = test_gemini_config()
    else:
        # Test current configuration from specified file
        if not check_env_file(env_file):
            return False
        success = test_current_config(env_file)
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)