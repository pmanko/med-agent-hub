import logging
import time
import psutil
import uuid
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .config import agent_config, llm_config, a2a_endpoints
from .llm_clients import llm_client
from .schemas import ChatRequest, ChatResponse

import requests
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

server_start_time = time.time()

app = FastAPI(
    title="A2A-Enabled Multi-Agent Medical Chat API",
    description="Direct chat endpoints + A2A-simulated /chat with agent threads",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatMessage(BaseModel):
    role: str = Field(...)
    content: str = Field(...)
    timestamp: Optional[str] = Field(default=None)


class PromptRequest(BaseModel):
    prompt: str = Field(..., min_length=1, max_length=4000)
    system_prompt: str = Field(default="", max_length=2000)
    max_new_tokens: int = Field(default=512, ge=1, le=2048)
    conversation_history: List[ChatMessage] = Field(default=[])
    conversation_id: Optional[str] = None


class PromptResponse(BaseModel):
    response: str


@app.get("/")
def read_root():
    return {
        "status": "Server is running",
        "uptime_seconds": round(time.time() - server_start_time, 2),
        "a2a_enabled": agent_config.enable_a2a,
        "direct_models": {
            "orchestrator": llm_config.orchestrator_model, 
            "medical": llm_config.med_model,
            "generalist": llm_config.clinical_research_model
        },
    }


@app.get("/manifest")
def get_manifest():
    # In a native A2A architecture, the manifest is decentralized.
    # Each agent provides its own card. This endpoint could be
    # enhanced to query each agent and aggregate the results.
    return {
        "router_agent": a2a_endpoints.router_url,
        "medical_agent": a2a_endpoints.medgemma_url,
        "generalist_agent": a2a_endpoints.clinical_url
    }


@app.get("/health")
def health_check():
    uptime = time.time() - server_start_time
    memory_info = {}
    try:
        process = psutil.Process()
        memory_info["process_memory_gb"] = round(process.memory_info().rss / 1024**3, 2)
        memory_info["process_memory_percent"] = round(process.memory_percent(), 1)
    except Exception:
        pass
    return {"status": "healthy", "uptime_seconds": round(uptime, 2), "memory": memory_info, "timestamp": time.time()}


@app.post("/generate/orchestrator", response_model=PromptResponse)
def generate_orchestrator(request: PromptRequest):
    try:
        messages = []
        if request.system_prompt:
            messages.append({"role": "system", "content": request.system_prompt})
        
        # Handle conversation history - keep last 10 exchanges (20 messages)
        history = request.conversation_history[-20:] if request.conversation_history else []
        
        # If history is too long, summarize older messages
        if len(request.conversation_history) > 20:
            # Create a summary of older messages
            summary_prompt = "Summarize the key points from this conversation so far in 2-3 sentences:\n"
            for m in request.conversation_history[:-20]:
                summary_prompt += f"{m.role}: {m.content[:200]}...\n" if len(m.content) > 200 else f"{m.role}: {m.content}\n"
            
            summary = llm_client.generate_chat(
                model=llm_config.orchestrator_model,
                messages=[{"role": "user", "content": summary_prompt}],
                temperature=0.1,
                max_tokens=200,
            )
            messages.append({"role": "system", "content": f"Previous conversation summary: {summary}"})
        
        # Add recent history
        for m in history:
            role = m.role if m.role in ("user", "assistant", "system") else "user"
            # Map 'assistant' to 'assistant' for the LLM API
            if role == "assistant":
                messages.append({"role": "assistant", "content": m.content})
            else:
                messages.append({"role": role, "content": m.content})
        
        # Add current prompt
        messages.append({"role": "user", "content": request.prompt})
        
        text = llm_client.generate_chat(
            model=llm_config.orchestrator_model,
            messages=messages,
            temperature=llm_config.temperature,
            max_tokens=min(request.max_new_tokens, 2000),
        )
        return {"response": text}
    except Exception as e:
        logger.warning(f"Orchestrator generation unavailable: {e}")
        fallback = (
            "The orchestrator LLM backend is currently unavailable. "
            "Please try again shortly or use Agents (A2A) mode if configured."
        )
        return {"response": fallback}


@app.post("/generate/medical", response_model=PromptResponse)
def generate_medical(request: PromptRequest):
    try:
        system = request.system_prompt or (
            "You are a medical AI assistant. Provide accurate, evidence-based information. Include a brief disclaimer."
        )
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        
        # Handle conversation history - keep last 10 exchanges for medical context
        history = request.conversation_history[-20:] if request.conversation_history else []
        
        # If history is too long, summarize medical context
        if len(request.conversation_history) > 20:
            # Create a medical-focused summary
            summary_prompt = "Summarize the key medical topics and patient information discussed so far in 2-3 sentences:\n"
            for m in request.conversation_history[:-20]:
                summary_prompt += f"{m.role}: {m.content[:200]}...\n" if len(m.content) > 200 else f"{m.role}: {m.content}\n"
            
            summary = llm_client.generate_chat(
                model=llm_config.med_model,
                messages=[{"role": "user", "content": summary_prompt}],
                temperature=0.1,
                max_tokens=200,
            )
            messages.append({"role": "system", "content": f"Previous medical discussion summary: {summary}"})
        
        # Add recent history
        for m in history:
            role = m.role if m.role in ("user", "assistant", "system") else "user"
            if role == "assistant":
                messages.append({"role": "assistant", "content": m.content})
            else:
                messages.append({"role": role, "content": m.content})
        
        # Add current prompt
        messages.append({"role": "user", "content": request.prompt})
        
        text = llm_client.generate_chat(
            model=llm_config.med_model,
            messages=messages,
            temperature=0.1,
            max_tokens=min(request.max_new_tokens, 1500),
        )
        return {"response": text}
    except Exception as e:
        logger.warning(f"Medical LLM unavailable: {e}")
        fallback = (
            "The medical LLM backend is currently unavailable. "
            "Please try again or use Agents (A2A) mode if configured."
        )
        return {"response": fallback}


@app.post("/generate/clinical", response_model=PromptResponse)
def generate_clinical(request: PromptRequest):
    try:
        system = request.system_prompt or (
            "You are a clinical research assistant. Provide evidence-based analysis and insights."
        )
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        
        # Handle conversation history - keep last 10 exchanges for clinical context
        history = request.conversation_history[-20:] if request.conversation_history else []
        
        # If history is too long, summarize clinical research context
        if len(request.conversation_history) > 20:
            # Create a clinical research-focused summary
            summary_prompt = "Summarize the key clinical research topics, data points, and findings discussed so far in 2-3 sentences:\n"
            for m in request.conversation_history[:-20]:
                summary_prompt += f"{m.role}: {m.content[:200]}...\n" if len(m.content) > 200 else f"{m.role}: {m.content}\n"
            
            summary = llm_client.generate_chat(
                model=llm_config.clinical_research_model,
                messages=[{"role": "user", "content": summary_prompt}],
                temperature=0.1,
                max_tokens=200,
            )
            messages.append({"role": "system", "content": f"Previous clinical research discussion summary: {summary}"})
        
        # Add recent history
        for m in history:
            role = m.role if m.role in ("user", "assistant", "system") else "user"
            if role == "assistant":
                messages.append({"role": "assistant", "content": m.content})
            else:
                messages.append({"role": role, "content": m.content})
        
        # Add current prompt
        messages.append({"role": "user", "content": request.prompt})
        
        text = llm_client.generate_chat(
            model=llm_config.clinical_research_model,
            messages=messages,
            temperature=0.2,
            max_tokens=min(request.max_new_tokens, 2000),
        )
        return {"response": text}
    except Exception as e:
        logger.warning(f"Clinical research LLM unavailable: {e}")
        fallback = (
            "The clinical research LLM backend is currently unavailable. "
            "Please try again or use Agents (A2A) mode if configured."
        )
        return {"response": fallback}


@app.post("/chat", response_model=ChatResponse)
def chat(request: ChatRequest):
    if not agent_config.enable_a2a:
        raise HTTPException(status_code=503, detail="A2A layer is disabled in the configuration.")

    if not a2a_endpoints.router_url:
        raise HTTPException(status_code=500, detail="A2A router URL is not configured.")
    
    try:
        # This is the native A2A path. The web server acts as a client
        # to the independently running router agent.
        
        # The A2A SDK's router_executor expects a full message object,
        # not just a payload. We construct one here.
        a2a_message = {
            "jsonrpc": "2.0",
            "method": "message/send",
            "params": {
                "message": {
                    "message_id": str(uuid.uuid4()),
                    "role": "user",
                    "parts": [
                        {
                            "type": "text",
                            "text": request.prompt,
                        }
                    ],
                    "metadata": {
                        "conversation_id": request.conversation_id,
                        "scope": request.scope,
                        "facility_id": request.facility_id,
                        "org_ids": request.org_ids,
                        "orchestrator_mode": request.orchestrator_mode,
                    },
                }
            },
            "id": str(uuid.uuid4()),
        }

        resp = requests.post(
            a2a_endpoints.router_url.rstrip("/") + "/",
            json=a2a_message,
            timeout=agent_config.chat_timeout_seconds,
        )
        resp.raise_for_status()
        data = resp.json()
        
        # Extract the final text response from the A2A task structure
        final_text = "(No content was returned from the agent network)"
        responding_agent_name: Optional[str] = None
        task_result = data.get("result", {})
        
        # The result from an SDK agent is a Task object. We need to parse it.
        if task_result:
            # The final answer is usually in the last artifact
            artifacts = task_result.get("artifacts", [])
            if artifacts:
                last_artifact = artifacts[-1]
                
                # Determine agent name from the artifact's name
                artifact_name = last_artifact.get("name")
                if artifact_name == "medical_response":
                    responding_agent_name = "MedGemma Agent"
                elif artifact_name in ("clinical_analysis", "general_knowledge_response"):
                    responding_agent_name = "Clinical Research Agent"
                elif artifact_name == "router_summary":
                    responding_agent_name = "Router Agent"
                
                parts = last_artifact.get("parts", [])
                if parts:
                    text_part = parts[0]
                    if text_part.get("kind") == "text":
                        final_text = text_part.get("text", final_text)

        return ChatResponse(
            response=final_text, 
            correlation_id=task_result.get("id", "native") if task_result else "native",
            responding_agent=responding_agent_name,
        )

    except requests.exceptions.RequestException as e:
        logger.error(f"A2A router connection error: {e}")
        raise HTTPException(status_code=503, detail=f"Could not connect to the A2A Router Agent at {a2a_endpoints.router_url}.")
    except Exception as e:
        logger.error(f"/chat endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
