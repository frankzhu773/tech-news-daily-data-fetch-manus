"""
LLM Client — Gemini 2.5 Flash via OpenAI-compatible API

Provides a unified call_llm() function that replaces direct Gemini API calls.
Uses the pre-configured OpenAI client with model 'gemini-2.5-flash'.

Search grounding is enabled by default via the google_search tool.
"""

import time
import logging
from openai import OpenAI

log = logging.getLogger(__name__)

# ─── Configuration ──────────────────────────────────────────────────────────

MODEL = "gemini-2.5-flash"
LLM_MAX_RETRIES = 3

# Initialize the OpenAI client (API key and base URL are pre-configured)
_client = OpenAI()


# ─── Main LLM call function ────────────────────────────────────────────────

def call_llm(prompt: str, system: str = "", max_tokens: int = 2000, use_search: bool = True) -> str:
    """Call Gemini 2.5 Flash via OpenAI-compatible API and return the response text.

    Retries up to LLM_MAX_RETRIES times with exponential backoff for transient errors.

    Args:
        prompt: The user prompt text.
        system: Optional system instruction.
        max_tokens: Maximum output tokens.
        use_search: If True, enable Google Search grounding tool.

    Returns:
        The response text, or empty string on failure.
    """
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    # Build kwargs
    kwargs = {
        "model": MODEL,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0.3,
    }

    # Enable Google Search grounding if requested
    if use_search:
        kwargs["tools"] = [{"type": "function", "function": {"name": "google_search", "parameters": {}}}]

    for attempt in range(LLM_MAX_RETRIES + 1):
        try:
            response = _client.chat.completions.create(**kwargs)

            if response.choices and response.choices[0].message:
                content = response.choices[0].message.content
                if content:
                    return content.strip()

            log.warning(f"LLM returned empty content (attempt {attempt + 1})")
            return ""

        except Exception as e:
            error_str = str(e)
            # Check for retryable errors
            is_retryable = any(code in error_str for code in ["429", "500", "502", "503", "504", "rate_limit", "overloaded"])

            if is_retryable and attempt < LLM_MAX_RETRIES:
                wait = 3 * (2 ** attempt)
                log.warning(f"LLM error (attempt {attempt + 1}/{LLM_MAX_RETRIES + 1}), retrying in {wait}s: {error_str[:200]}")
                time.sleep(wait)
                continue

            log.error(f"LLM call failed: {error_str[:300]}")
            return ""

    log.error(f"LLM failed after {LLM_MAX_RETRIES + 1} attempts")
    return ""


# ─── Convenience alias for sensortower script ──────────────────────────────

def call_gemini(prompt: str, system_instruction: str, max_tokens: int = 2000,
                use_search: bool = False, retries: int = 3) -> str | None:
    """Alias matching the original call_gemini() signature in fetch_sensortower.py.

    Returns None on failure (matching original behavior).
    """
    result = call_llm(prompt, system=system_instruction, max_tokens=max_tokens, use_search=use_search)
    return result if result else None
