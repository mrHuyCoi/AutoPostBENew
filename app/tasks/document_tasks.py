"""
Celery tasks for Document operations.

These tasks provide async processing with automatic retry for document operations,
ensuring data is not lost if ChatbotMobileStore is temporarily unavailable.
"""

import asyncio
import logging
import os
from typing import Dict, Any, Optional

import httpx

from app.celery_app import celery_app
from app.configs.settings import settings

logger = logging.getLogger(__name__)

CHATBOT_API_BASE_URL = os.getenv("CHATBOT_API_BASE_URL", "http://localhost:8001")


def _get_loop() -> asyncio.AbstractEventLoop:
    """Get or create an event loop for running async code in Celery tasks."""
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


async def _async_upload_text(
    user_id: str,
    document_input: Dict[str, Any],
) -> Dict[str, Any]:
    """Internal async function to upload text document."""
    async with httpx.AsyncClient(timeout=120.0) as client:
        url = f"{CHATBOT_API_BASE_URL}/upload-text/{user_id}"
        headers = {"Content-Type": "application/json"}
        response = await client.post(url, json=document_input, headers=headers)
        response.raise_for_status()
        return response.json()


async def _async_upload_url(
    user_id: str,
    document_input: Dict[str, Any],
) -> Dict[str, Any]:
    """Internal async function to upload URL document."""
    async with httpx.AsyncClient(timeout=120.0) as client:
        url = f"{CHATBOT_API_BASE_URL}/upload-url/{user_id}"
        headers = {"Content-Type": "application/json"}
        response = await client.post(url, json=document_input, headers=headers)
        response.raise_for_status()
        return response.json()


async def _async_upload_file_from_url(
    user_id: str,
    file_url: str,
    filename: str,
    content_type: str,
) -> Dict[str, Any]:
    """Internal async function to upload file from URL (file already uploaded to storage)."""
    async with httpx.AsyncClient(timeout=120.0) as client:
        # Download file from storage URL
        file_response = await client.get(file_url)
        file_response.raise_for_status()
        file_content = file_response.content
        
        # Upload to ChatbotMobileStore
        url = f"{CHATBOT_API_BASE_URL}/upload-file/{user_id}"
        files = {"file": (filename, file_content, content_type)}
        response = await client.post(url, files=files)
        response.raise_for_status()
        return response.json()


async def _async_delete_all_documents(user_id: str) -> Dict[str, Any]:
    """Internal async function to delete all documents."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        url = f"{CHATBOT_API_BASE_URL}/documents/{user_id}"
        response = await client.delete(url)
        response.raise_for_status()
        return response.json()


async def _async_delete_documents_by_source(user_id: str, source: str) -> Dict[str, Any]:
    """Internal async function to delete documents by source."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        url = f"{CHATBOT_API_BASE_URL}/sources/{user_id}?source={source}"
        response = await client.delete(url)
        response.raise_for_status()
        return response.json()


async def _async_trigger_graphrag_reindex(
    user_id: str,
    api_key: str,
    provider: str = "gemini",
    method: str = "fast",
    persist_to_db: bool = True,
    overwrite: bool = True,
    chat_model: Optional[str] = None,
    embedding_model: Optional[str] = None,
) -> Dict[str, Any]:
    """Internal async function to trigger GraphRAG reindex."""
    payload: Dict[str, Any] = {
        "api_key": api_key,
        "method": method,
        "persist_to_db": persist_to_db,
        "overwrite": overwrite,
        "provider": provider,
    }
    if provider == "gemini":
        if chat_model:
            payload["chat_model"] = chat_model
        if embedding_model:
            payload["embedding_model"] = embedding_model

    async with httpx.AsyncClient(timeout=None) as client:
        url = f"{CHATBOT_API_BASE_URL}/graphrag/reindex/{user_id}"
        response = await client.post(url, json=payload)
        response.raise_for_status()
        return response.json()


# Celery Tasks for Documents

@celery_app.task(
    name="doc_upload_text",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
    reject_on_worker_lost=True,
)
def doc_upload_text(
    self,
    user_id: str,
    document_input: Dict[str, Any],
    trigger_reindex: bool = False,
    reindex_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Celery task to upload text document with retry."""
    loop = _get_loop()
    result = loop.run_until_complete(_async_upload_text(user_id, document_input))
    
    # Optionally trigger reindex after upload
    if trigger_reindex and reindex_config:
        try:
            loop.run_until_complete(_async_trigger_graphrag_reindex(
                user_id=user_id,
                api_key=reindex_config.get("api_key", ""),
                provider=reindex_config.get("provider", "gemini"),
            ))
        except Exception as e:
            logger.warning(f"Failed to trigger reindex after upload: {e}")
    
    return result


@celery_app.task(
    name="doc_upload_url",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
    reject_on_worker_lost=True,
)
def doc_upload_url(
    self,
    user_id: str,
    document_input: Dict[str, Any],
    trigger_reindex: bool = False,
    reindex_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Celery task to upload URL document with retry."""
    loop = _get_loop()
    result = loop.run_until_complete(_async_upload_url(user_id, document_input))
    
    # Optionally trigger reindex after upload
    if trigger_reindex and reindex_config:
        try:
            loop.run_until_complete(_async_trigger_graphrag_reindex(
                user_id=user_id,
                api_key=reindex_config.get("api_key", ""),
                provider=reindex_config.get("provider", "gemini"),
            ))
        except Exception as e:
            logger.warning(f"Failed to trigger reindex after upload: {e}")
    
    return result


@celery_app.task(
    name="doc_upload_file",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
    reject_on_worker_lost=True,
    time_limit=120,
)
def doc_upload_file(
    self,
    user_id: str,
    file_url: str,
    filename: str,
    content_type: str,
    trigger_reindex: bool = False,
    reindex_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Celery task to upload file document with retry.
    
    Note: The file must be already uploaded to storage and accessible via file_url.
    """
    loop = _get_loop()
    result = loop.run_until_complete(
        _async_upload_file_from_url(user_id, file_url, filename, content_type)
    )
    
    # Optionally trigger reindex after upload
    if trigger_reindex and reindex_config:
        try:
            loop.run_until_complete(_async_trigger_graphrag_reindex(
                user_id=user_id,
                api_key=reindex_config.get("api_key", ""),
                provider=reindex_config.get("provider", "gemini"),
            ))
        except Exception as e:
            logger.warning(f"Failed to trigger reindex after upload: {e}")
    
    return result


@celery_app.task(
    name="doc_delete_all",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
    reject_on_worker_lost=True,
)
def doc_delete_all(self, user_id: str) -> Dict[str, Any]:
    """Celery task to delete all documents with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_delete_all_documents(user_id))


@celery_app.task(
    name="doc_delete_by_source",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
    reject_on_worker_lost=True,
)
def doc_delete_by_source(self, user_id: str, source: str) -> Dict[str, Any]:
    """Celery task to delete documents by source with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_delete_documents_by_source(user_id, source))


@celery_app.task(
    name="doc_trigger_reindex",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
    reject_on_worker_lost=True,
    time_limit=600,  # 10 minutes for reindex
)
def doc_trigger_reindex(
    self,
    user_id: str,
    api_key: str,
    provider: str = "gemini",
    method: str = "fast",
    persist_to_db: bool = True,
    overwrite: bool = True,
    chat_model: Optional[str] = None,
    embedding_model: Optional[str] = None,
) -> Dict[str, Any]:
    """Celery task to trigger GraphRAG reindex with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_trigger_graphrag_reindex(
        user_id=user_id,
        api_key=api_key,
        provider=provider,
        method=method,
        persist_to_db=persist_to_db,
        overwrite=overwrite,
        chat_model=chat_model,
        embedding_model=embedding_model,
    ))
