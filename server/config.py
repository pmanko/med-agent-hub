"""
Configuration management for the Medical Multi-Agent Chat System.
Provides smart defaults for all settings to minimize configuration burden.
"""

import os
from dotenv import load_dotenv

# Load environment variables from the appropriate file
# Check if uvicorn specified a custom env file, otherwise use default .env
env_file = os.getenv("UVICORN_ENV_FILE", ".env")
load_dotenv(dotenv_path=env_file)

# ==============================================================================
# LLM Configuration
# ==============================================================================

# LM Studio endpoint (required)
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://localhost:1234")
LLM_API_KEY = os.getenv("LLM_API_KEY", "")  # Empty for local LM Studio

# Model selection (swappable via env). Defaults: a
# gemma-4 e4b orchestrator+synthesizer and a medgemma-1.5-4b medical expert.
# Use the official `google/` gemma-4 build — some community GGUFs (unsloth,
# e2b) ship a broken jinja tool template that 400s on tool-calling.
ORCHESTRATOR_MODEL = os.getenv("ORCHESTRATOR_MODEL", "google/gemma-4-e4b")
MED_MODEL = os.getenv("MED_MODEL", "medgemma-1.5-4b-it")
# Synthesizer composes the final {answer,citations,blocks} envelope. Defaults to the
# orchestrator model; set SYNTHESIZER_MODEL to a larger model so the
# single synthesis call gets more capability while a small/fast orchestrator runs the
# multi-call tool loop.
SYNTHESIZER_MODEL = os.getenv("SYNTHESIZER_MODEL", ORCHESTRATOR_MODEL)
CLINICAL_RESEARCH_MODEL = os.getenv("CLINICAL_RESEARCH_MODEL", "gemma-3-1b-it")

# Per-tier models (orchestrator / expert / synthesizer per level) now live in
# server/levels.yaml, resolved by levels_loader and passed to run_team per request.
# ORCHESTRATOR_MODEL / MED_MODEL / SYNTHESIZER_MODEL above are only the fallback
# defaults for direct run_team calls (tests) and the raw-passthrough path.

# Legacy alias for backward compatibility
GENERAL_MODEL = ORCHESTRATOR_MODEL

# LLM parameters (smart defaults, not exposed in env.example)
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.2"))
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "2000"))  # Increased default for longer responses

# Synthesis anti-repetition lever. LM Studio's OpenAI API silently DROPS
# frequency_penalty and the DRY sampler on its MLX engine — only `repeat_penalty`
# is honored — so the synthesis call sends this. ~1.15; keep <=1.3 (higher distorts
# output / drives language drift on small models).
SYNTH_REPEAT_PENALTY = float(os.getenv("SYNTH_REPEAT_PENALTY", "1.15"))

# Per-role DRY (n-gram anti-repetition) multiplier, sent on each role's chat call. Honored by
# a real llama.cpp server (the router); LM Studio MLX silently drops it (no-op), so it's safe
# to always send. ORCHESTRATOR runs at 0 (DRY OFF): it emits tool-call JSON in a loop, and DRY's
# sequence penalty can distort the repeated call structure. EXPERT (free text) + SYNTH (the
# loop-prone JSON envelope) keep DRY on. Per-ROLE, so the orchestrator's gemma differs from the
# same gemma serving the single-LLM parity arm (which keeps DRY).
ORCHESTRATOR_DRY_MULTIPLIER = float(os.getenv("ORCHESTRATOR_DRY_MULTIPLIER", "0.0"))
EXPERT_DRY_MULTIPLIER = float(os.getenv("EXPERT_DRY_MULTIPLIER", "0.8"))
SYNTH_DRY_MULTIPLIER = float(os.getenv("SYNTH_DRY_MULTIPLIER", "0.8"))

# Create a config object for cleaner imports
class LLMConfig:
    base_url = LLM_BASE_URL
    api_key = LLM_API_KEY
    orchestrator_model = ORCHESTRATOR_MODEL
    synthesizer_model = SYNTHESIZER_MODEL
    med_model = MED_MODEL
    clinical_research_model = CLINICAL_RESEARCH_MODEL
    general_model = GENERAL_MODEL  # backward compat
    temperature = LLM_TEMPERATURE
    max_tokens = LLM_MAX_TOKENS

llm_config = LLMConfig()

# ==============================================================================
# Orchestrator Configuration (Optional Gemini)
# ==============================================================================

# Provider can be "openai" (for LM Studio) or "gemini"
ORCHESTRATOR_PROVIDER = os.getenv("ORCHESTRATOR_PROVIDER", "openai")

if ORCHESTRATOR_PROVIDER == "gemini":
    # Use Google Gemini for orchestration
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY required when ORCHESTRATOR_PROVIDER=gemini")
    ORCHESTRATOR_MODEL = os.getenv("ORCHESTRATOR_MODEL", "gemini-1.5-flash")
else:
    # Use local LM Studio for orchestration
    ORCHESTRATOR_MODEL = GENERAL_MODEL
    GEMINI_API_KEY = None

# ==============================================================================
# A2A Service Configuration
# ==============================================================================

# Agent service URLs (defaults work for standard setup)
AGENT_HOST_IP = os.getenv("AGENT_HOST_IP", "127.0.0.1")
A2A_ROUTER_PORT = os.getenv("A2A_ROUTER_PORT", "9100")
A2A_MEDGEMMA_PORT = os.getenv("A2A_MEDGEMMA_PORT", "9101")
A2A_CLINICAL_PORT = os.getenv("A2A_CLINICAL_PORT", "9102")

A2A_ROUTER_URL = f"http://{AGENT_HOST_IP}:{A2A_ROUTER_PORT}"
A2A_MEDGEMMA_URL = f"http://{AGENT_HOST_IP}:{A2A_MEDGEMMA_PORT}"
# Backwards-compat: prefer role-based 'medical' URL while retaining legacy 'medgemma'
A2A_MEDICAL_URL = os.getenv("A2A_MEDICAL_URL", A2A_MEDGEMMA_URL)
A2A_CLINICAL_URL = f"http://{AGENT_HOST_IP}:{A2A_CLINICAL_PORT}"

# A2A is always enabled in SDK version
ENABLE_A2A = True
ENABLE_A2A_NATIVE = True

# ==============================================================================
# Clinical Data Sources (Optional)
# ==============================================================================

# OpenMRS FHIR configuration
OPENMRS_FHIR_BASE_URL = os.getenv("OPENMRS_FHIR_BASE_URL", "")
# Use default OpenMRS credentials if FHIR is configured but credentials not provided
OPENMRS_USERNAME = os.getenv("OPENMRS_USERNAME", "admin" if OPENMRS_FHIR_BASE_URL else "")
OPENMRS_PASSWORD = os.getenv("OPENMRS_PASSWORD", "Admin123" if OPENMRS_FHIR_BASE_URL else "")

# Local FHIR Parquet files
FHIR_PARQUET_DIR = os.getenv("FHIR_PARQUET_DIR", "")

# Spark SQL configuration (advanced users only)
SPARK_THRIFT_HOST = os.getenv("SPARK_THRIFT_HOST", "")
SPARK_THRIFT_PORT = int(os.getenv("SPARK_THRIFT_PORT", "10000"))
SPARK_THRIFT_DATABASE = os.getenv("SPARK_THRIFT_DATABASE", "default")

# ==============================================================================
# Application Settings (Smart Defaults)
# ==============================================================================

# Timeouts
CHAT_TIMEOUT_SECONDS = int(os.getenv("CHAT_TIMEOUT_SECONDS", "90"))
LLM_TIMEOUT_SECONDS = int(os.getenv("LLM_TIMEOUT_SECONDS", "60"))
AGENT_STARTUP_TIMEOUT = int(os.getenv("AGENT_STARTUP_TIMEOUT", "10"))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Performance
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "10"))
CONNECTION_POOL_SIZE = int(os.getenv("CONNECTION_POOL_SIZE", "20"))

# ==============================================================================
# Security Settings (Defaults for Local Development)
# ==============================================================================

# CORS - Allow common local development origins
CORS_ORIGINS = [
    "http://localhost:3000",  # React dev server
    "http://localhost:8080",  # Production UI
    "http://localhost:7860",  # Alternative port
]

# SSL/TLS - Disabled for local development
USE_HTTPS = os.getenv("USE_HTTPS", "false").lower() == "true"

# API Authentication - Disabled by default for ease of use
API_KEY_REQUIRED = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
API_KEY = os.getenv("API_KEY", "")

# ==============================================================================
# Development/Debug Settings
# ==============================================================================

# Environment mode
ENV = os.getenv("ENV", "development")
DEBUG = ENV == "development"

# Auto-reload for development
RELOAD = os.getenv("RELOAD", str(DEBUG)).lower() == "true"

# ==============================================================================
# Config Objects for cleaner imports
# ==============================================================================

class AgentConfig:
    enable_a2a = ENABLE_A2A
    enable_a2a_native = ENABLE_A2A_NATIVE
    chat_timeout_seconds = CHAT_TIMEOUT_SECONDS
    startup_timeout = AGENT_STARTUP_TIMEOUT

class A2AEndpoints:
    router_url = A2A_ROUTER_URL
    # Prefer role-based alias
    medical_url = A2A_MEDICAL_URL
    # Legacy alias for compatibility
    medgemma_url = A2A_MEDGEMMA_URL
    clinical_url = A2A_CLINICAL_URL

class OrchestatorConfig:
    provider = ORCHESTRATOR_PROVIDER
    model = ORCHESTRATOR_MODEL
    gemini_api_key = GEMINI_API_KEY
    mode = os.getenv("ORCHESTRATOR_MODE", "simple")  # "simple" or "react"

class OpenMRSConfig:
    fhir_base_url = OPENMRS_FHIR_BASE_URL
    auth_username = OPENMRS_USERNAME
    auth_password = OPENMRS_PASSWORD

class SparkConfig:
    host = SPARK_THRIFT_HOST
    port = SPARK_THRIFT_PORT
    database = SPARK_THRIFT_DATABASE
    username = None
    password = None

class LocalConfig:
    parquet_dir = FHIR_PARQUET_DIR

agent_config = AgentConfig()
a2a_endpoints = A2AEndpoints()
orchestrator_config = OrchestatorConfig()
openmrs_config = OpenMRSConfig()
spark_config = SparkConfig()
local_config = LocalConfig()

# ==============================================================================
# Validation
# ==============================================================================

def validate_config():
    """Validate configuration and provide helpful error messages."""
    errors = []
    
    # Check required settings
    if not LLM_BASE_URL:
        errors.append("LLM_BASE_URL is required. Set it to your LM Studio endpoint (e.g., http://localhost:1234)")
    
    if not GENERAL_MODEL:
        errors.append("GENERAL_MODEL is required. Set it to your model name in LM Studio")
    
    # Check optional features - credentials are auto-set with defaults when FHIR URL is provided
    
    if errors:
        print("Configuration errors:")
        for error in errors:
            print(f"  - {error}")
        raise ValueError("Invalid configuration. Please check your .env file.")
    
    # Log configuration summary (without secrets)
    print("Configuration loaded:")
    print(f"  - LLM: {LLM_BASE_URL}")
    print(f"  - Orchestrator Model: {ORCHESTRATOR_MODEL}")
    print(f"  - Medical Model: {MED_MODEL}")
    print(f"  - Clinical Research Model: {CLINICAL_RESEARCH_MODEL}")
    print(f"  - Orchestrator Provider: {ORCHESTRATOR_PROVIDER}")
    print(f"  - Orchestrator Mode: {orchestrator_config.mode}")
    if OPENMRS_FHIR_BASE_URL:
        print(f"  - FHIR: Connected to {OPENMRS_FHIR_BASE_URL}")
    if FHIR_PARQUET_DIR:
        print(f"  - Local Data: {FHIR_PARQUET_DIR}")
    print(f"  - Environment: {ENV}")

# Run validation on import
if __name__ != "__main__":  # Only validate when imported as module
    try:
        validate_config()
    except ValueError as e:
        print(f"Warning: {e}")