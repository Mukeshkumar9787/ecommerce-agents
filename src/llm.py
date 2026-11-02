"""LLM Provider Factory supporting Google Gemini models."""

import logging
import os
from typing import Optional
from langchain_core.language_models import BaseChatModel
from src.config import get_settings

logger = logging.getLogger(__name__)


def get_gemini_llm(
    model: Optional[str] = None,
    temperature: float = 0.0,
) -> Optional[BaseChatModel]:
    """
    Instantiate and return a ChatGoogleGenerativeAI model configured with GEMINI_API_KEY.

    Args:
        model: Model name override (defaults to GEMINI_MODEL setting, e.g. gemini-2.5-flash).
        temperature: Sampling temperature.

    Returns:
        ChatGoogleGenerativeAI instance if API key is provided, or None if in offline/mock mode.
    """
    settings = get_settings()
    api_key = (
        settings.effective_gemini_api_key
        or os.environ.get("GEMINI_API_KEY", "")
        or os.environ.get("GOOGLE_API_KEY", "")
    )
    model_name = model or settings.GEMINI_MODEL or "gemini-2.5-flash"

    if not api_key or api_key.startswith("your-") or api_key == "mock-key":
        logger.debug("No active GEMINI_API_KEY configured. Running in offline/deterministic mode.")
        return None

    try:
        from langchain_google_genai import ChatGoogleGenerativeAI

        llm = ChatGoogleGenerativeAI(
            model=model_name,
            google_api_key=api_key,
            temperature=temperature,
        )
        logger.info("Initialized Gemini LLM with model='%s'", model_name)
        return llm
    except Exception as exc:
        logger.warning("Could not initialize ChatGoogleGenerativeAI: %s", exc)
        return None
