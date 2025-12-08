import asyncio
import logging
from typing import Optional, List, Dict, Any
import json

import httpx

from app.celery_app import celery_app
from app.configs.settings import settings
from app.services.chatbot_service import ChatbotService


logger = logging.getLogger(__name__)


def _get_loop() -> asyncio.AbstractEventLoop:
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


async def _async_chat(
    thread_id: str,
    query: str,
    customer_id: str,
    llm_provider: str,
    api_key: str,
    access: Optional[int] = None,
    scopes: Optional[List[str]] = None,
    image_url: Optional[str] = None,
    image_urls: Optional[List[str]] = None,
    image_base64: Optional[str] = None,
    history: Optional[list] = None,
) -> Dict[str, Any]:
    res = await ChatbotService.chat_with_bot(
        thread_id=thread_id,
        query=query,
        customer_id=customer_id,
        llm_provider=llm_provider,
        api_key=api_key,
        access=access,
        scopes=scopes,
        image_url=image_url,
        image_urls=image_urls,
        image_base64=image_base64,
        history=history,
    )
    return res


@celery_app.task(name="chatbot_chat", bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 2})
def chatbot_chat(
    self,
    thread_id: str,
    query: str,
    customer_id: str,
    llm_provider: str,
    api_key: str,
    access: Optional[int] = None,
    scopes: Optional[List[str]] = None,
    image_url: Optional[str] = None,
    image_urls: Optional[List[str]] = None,
    image_base64: Optional[str] = None,
    history: Optional[list] = None,
):
    loop = _get_loop()
    return loop.run_until_complete(
        _async_chat(
            thread_id=thread_id,
            query=query,
            customer_id=customer_id,
            llm_provider=llm_provider,
            api_key=api_key,
            access=access,
            scopes=scopes,
            image_url=image_url,
            image_urls=image_urls,
            image_base64=image_base64,
            history=history,
        )
    )


async def _async_custom_chat(
    customer_id: str,
    message: str,
    model_choice: str,
    api_key: str,
    session_id: str,
    image_url: Optional[str] = None,
    image_urls: Optional[List[str]] = None,
) -> Dict[str, Any]:
    url = f"{settings.CHATBOT_CUSTOM_API_BASE_URL}/chat/{customer_id}"
    form_data = {
        "message": message,
        "model_choice": model_choice,
        "api_key": api_key,
        "session_id": session_id,
    }
    if image_url:
        form_data["image_url"] = image_url
    if image_urls:
        try:
            form_data["image_urls"] = json.dumps(list(image_urls))
        except Exception:
            pass

    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(url, data=form_data)
        resp.raise_for_status()
        return resp.json()


@celery_app.task(name="chatbot_custom_chat", bind=True, autoretry_for=(httpx.HTTPError,), retry_backoff=True, retry_kwargs={"max_retries": 2})
def chatbot_custom_chat(
    self,
    customer_id: str,
    message: str,
    model_choice: str,
    api_key: str,
    session_id: str,
    image_url: Optional[str] = None,
    image_urls: Optional[List[str]] = None,
):
    loop = _get_loop()
    return loop.run_until_complete(
        _async_custom_chat(
            customer_id=customer_id,
            message=message,
            model_choice=model_choice,
            api_key=api_key,
            session_id=session_id,
            image_url=image_url,
            image_urls=image_urls,
        )
    )
