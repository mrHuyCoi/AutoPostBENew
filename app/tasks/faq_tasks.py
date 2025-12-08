"""
Celery tasks for FAQ operations.

These tasks provide async processing with automatic retry for FAQ operations,
ensuring data is not lost if ChatbotMobileStore is temporarily unavailable.
"""

import asyncio
import logging
import os
from typing import Dict, Any, List, Optional

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


async def _async_create_faq(
    customer_id: str,
    faq_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Internal async function to create a FAQ."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        url = f"{CHATBOT_API_BASE_URL}/faq/{customer_id}"
        headers = {"Content-Type": "application/json"}
        response = await client.post(url, json=faq_data, headers=headers)
        response.raise_for_status()
        return response.json()


async def _async_update_faq(
    customer_id: str,
    faq_id: str,
    faq_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Internal async function to update a FAQ."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        url = f"{CHATBOT_API_BASE_URL}/faq/{customer_id}/{faq_id}"
        headers = {"Content-Type": "application/json"}
        response = await client.put(url, json=faq_data, headers=headers)
        response.raise_for_status()
        return response.json()


async def _async_delete_faq(
    customer_id: str,
    faq_id: str,
) -> Dict[str, Any]:
    """Internal async function to delete a FAQ."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        url = f"{CHATBOT_API_BASE_URL}/faq/{customer_id}/{faq_id}"
        response = await client.delete(url)
        response.raise_for_status()
        return response.json()


async def _async_delete_all_faqs(customer_id: str) -> Dict[str, Any]:
    """Internal async function to delete all FAQs."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        url = f"{CHATBOT_API_BASE_URL}/faqs/{customer_id}"
        response = await client.delete(url)
        response.raise_for_status()
        return response.json()


async def _async_import_faqs(
    customer_id: str,
    file_url: str,
    filename: str,
    content_type: str,
) -> Dict[str, Any]:
    """Internal async function to import FAQs from file."""
    async with httpx.AsyncClient(timeout=120.0) as client:
        # Download file from storage URL
        file_response = await client.get(file_url)
        file_response.raise_for_status()
        file_content = file_response.content
        
        # Upload to ChatbotMobileStore
        url = f"{CHATBOT_API_BASE_URL}/insert-faq/{customer_id}"
        files = {"file": (filename, file_content, content_type)}
        response = await client.post(url, files=files)
        response.raise_for_status()
        return response.json()


async def _async_bulk_create_faqs(
    customer_id: str,
    faqs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Internal async function to bulk create FAQs."""
    results = {"success": 0, "failed": 0, "errors": []}
    
    for faq_data in faqs:
        try:
            await _async_create_faq(customer_id, faq_data)
            results["success"] += 1
        except Exception as e:
            results["failed"] += 1
            results["errors"].append({"faq": faq_data.get("question", "unknown"), "error": str(e)})
    
    return results


# Celery Tasks for FAQs

@celery_app.task(
    name="faq_create",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
    reject_on_worker_lost=True,
)
def faq_create(
    self,
    customer_id: str,
    faq_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Celery task to create a FAQ with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_create_faq(customer_id, faq_data))


@celery_app.task(
    name="faq_update",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
    reject_on_worker_lost=True,
)
def faq_update(
    self,
    customer_id: str,
    faq_id: str,
    faq_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Celery task to update a FAQ with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_update_faq(customer_id, faq_id, faq_data))


@celery_app.task(
    name="faq_delete",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
    reject_on_worker_lost=True,
)
def faq_delete(
    self,
    customer_id: str,
    faq_id: str,
) -> Dict[str, Any]:
    """Celery task to delete a FAQ with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_delete_faq(customer_id, faq_id))


@celery_app.task(
    name="faq_delete_all",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
    reject_on_worker_lost=True,
)
def faq_delete_all(self, customer_id: str) -> Dict[str, Any]:
    """Celery task to delete all FAQs with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_delete_all_faqs(customer_id))


@celery_app.task(
    name="faq_import",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
    reject_on_worker_lost=True,
    time_limit=120,
)
def faq_import(
    self,
    customer_id: str,
    file_url: str,
    filename: str,
    content_type: str,
) -> Dict[str, Any]:
    """Celery task to import FAQs from file with retry.
    
    Note: The file must be already uploaded to storage and accessible via file_url.
    """
    loop = _get_loop()
    return loop.run_until_complete(
        _async_import_faqs(customer_id, file_url, filename, content_type)
    )


@celery_app.task(
    name="faq_bulk_create",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
    reject_on_worker_lost=True,
    time_limit=300,
)
def faq_bulk_create(
    self,
    customer_id: str,
    faqs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Celery task to bulk create FAQs with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_bulk_create_faqs(customer_id, faqs))
