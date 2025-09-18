from typing import List, Dict, Any, Optional
import requests

from .config import llm_config, orchestrator_config


class LLMClient:
    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None) -> None:
        self.base_url = (base_url or llm_config.base_url).rstrip("/")
        self.api_key = api_key or llm_config.api_key
        self._session = requests.Session()

    def generate_chat(
        self,
        model: str,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        stream: bool = False,
    ) -> str:
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        payload: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature if temperature is not None else llm_config.temperature,
            "stream": stream,
        }
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        # Helpful log so we can see effective URL and model used
        try:
            print(f"LLM call: base={self.base_url} model={model} temp={payload['temperature']} max_tokens={payload.get('max_tokens','-')} stream={stream}")
        except Exception:
            pass
        resp = self._session.post(url, json=payload, headers=headers, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"].strip()


class GeminiClient:
    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key = api_key or orchestrator_config.gemini_api_key
        self._session = requests.Session()

    def generate_chat(
        self,
        model: str,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> str:
        # Google Generative Language API v1beta style
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={self.api_key}"
        contents = []
        for m in messages:
            role = m.get("role", "user")
            text = m.get("content", "")
            contents.append({"role": role, "parts": [{"text": text}]})
        payload: Dict[str, Any] = {
            "contents": contents,
            "generationConfig": {"temperature": temperature or 0.2, **({"maxOutputTokens": max_tokens} if max_tokens else {})},
        }
        resp = self._session.post(url, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        return data["candidates"][0]["content"]["parts"][0]["text"].strip()


class OrchestratorClient:
    def __init__(self) -> None:
        self.provider = orchestrator_config.provider
        self.model = orchestrator_config.model
        self._openai_like = LLMClient()
        self._gemini = GeminiClient()

    def route(self, messages: List[Dict[str, str]], temperature: Optional[float] = None, max_tokens: Optional[int] = None) -> str:
        if self.provider == "gemini":
            return self._gemini.generate_chat(self.model, messages, temperature=temperature, max_tokens=max_tokens)
        return self._openai_like.generate_chat(self.model, messages, temperature=temperature, max_tokens=max_tokens)


llm_client = LLMClient()
orchestrator_client = OrchestratorClient() 