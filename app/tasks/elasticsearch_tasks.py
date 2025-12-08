"""
Celery tasks for Elasticsearch operations.

These tasks provide async processing with automatic retry for ES operations,
ensuring data is not lost if Elasticsearch is temporarily unavailable.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional

from app.celery_app import celery_app
from app.services.elasticsearch_service import (
    ElasticsearchService,
    get_es_client,
    close_es_client,
    sanitize_for_es,
    _embed_name_with_google,
    PRODUCTS_INDEX,
    SERVICES_INDEX,
    ACCESSORIES_INDEX,
)
from elasticsearch import AsyncElasticsearch, NotFoundError
from elasticsearch.helpers import async_bulk
from app.utils.crypto import token_encryption

logger = logging.getLogger(__name__)


def _get_loop() -> asyncio.AbstractEventLoop:
    """Get or create an event loop for running async code in Celery tasks."""
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


# ========================= PRODUCTS =========================

async def _async_upsert_product(
    customer_id: str,
    product_data: Dict[str, Any],
    with_embedding: bool = True,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Internal async function to upsert a product."""
    es = await get_es_client()
    sanitized_customer_id = sanitize_for_es(customer_id)
    doc_id = product_data.get("ma_san_pham")
    
    if not doc_id:
        raise ValueError("Missing 'ma_san_pham' in product data")
    
    sanitized_doc_id = sanitize_for_es(str(doc_id))
    composite_id = f"{sanitized_customer_id}_{sanitized_doc_id}"
    
    product_data["customer_id"] = sanitized_customer_id
    
    if with_embedding:
        name_field = "model"
        new_name = product_data.get(name_field)
        
        old_embedding = None
        old_name = None
        try:
            existing = await es.get(
                index=PRODUCTS_INDEX,
                id=composite_id,
                routing=sanitized_customer_id,
            )
            if existing and existing.get("found"):
                existing_source = existing.get("_source", {})
                old_name = existing_source.get(name_field)
                old_embedding = existing_source.get(f"{name_field}_embedding")
        except NotFoundError:
            pass
        except Exception as e:
            logger.warning(f"Error checking existing product: {e}")
        
        embedding = old_embedding
        if (new_name or "").strip() != (old_name or "").strip():
            try:
                embedding = await _embed_name_with_google(str(new_name) if new_name else "", api_key)
            except Exception as e:
                logger.warning(f"Error embedding product: {e}")
        
        if embedding:
            product_data[f"{name_field}_embedding"] = embedding
    
    response = await es.index(
        index=PRODUCTS_INDEX,
        id=composite_id,
        document=product_data,
        routing=sanitized_customer_id,
        refresh=True,
    )
    logger.info(f"Upserted product to Elasticsearch: {doc_id}")
    return response.body


async def _async_delete_product(customer_id: str, product_code: str) -> Dict[str, Any]:
    """Internal async function to delete a product."""
    es = await get_es_client()
    sanitized_customer_id = sanitize_for_es(customer_id)
    sanitized_doc_id = sanitize_for_es(str(product_code))
    composite_id = f"{sanitized_customer_id}_{sanitized_doc_id}"
    
    try:
        response = await es.delete(
            index=PRODUCTS_INDEX,
            id=composite_id,
            routing=sanitized_customer_id,
            refresh=True,
        )
        logger.info(f"Deleted product from Elasticsearch: {product_code}")
        return response.body
    except NotFoundError:
        logger.warning(f"Product not found in Elasticsearch: {product_code}")
        return {"result": "not_found"}


async def _async_bulk_upsert_products(
    customer_id: str,
    products: List[Dict[str, Any]],
    with_embedding: bool = True,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Internal async function to bulk upsert products."""
    es = await get_es_client()
    sanitized_customer_id = sanitize_for_es(customer_id)
    
    # Decrypt api_key if provided
    decrypted_api_key = None
    if api_key:
        try:
            decrypted_api_key = token_encryption.decrypt(api_key)
        except Exception as e:
            logger.warning(f"Failed to decrypt api_key: {e}")
    
    name_field = "model"
    
    # Step 1: Bulk upsert all docs with metadata (use script to preserve existing embedding)
    actions = []
    for product_data in products:
        doc_id = product_data.get("ma_san_pham")
        if not doc_id:
            continue
        
        sanitized_doc_id = sanitize_for_es(str(doc_id))
        composite_id = f"{sanitized_customer_id}_{sanitized_doc_id}"
        product_data["customer_id"] = sanitized_customer_id
        
        # Use scripted upsert to preserve existing embedding
        actions.append({
            "_op_type": "update",
            "_index": PRODUCTS_INDEX,
            "_id": composite_id,
            "routing": sanitized_customer_id,
            "script": {
                "source": """
                    for (entry in params.data.entrySet()) {
                        if (entry.getKey() != 'model_embedding') {
                            ctx._source[entry.getKey()] = entry.getValue();
                        }
                    }
                    if (ctx._source.model != params.data.model) {
                        ctx._source.remove('model_embedding');
                    }
                """,
                "params": {"data": product_data}
            },
            "upsert": product_data,
        })
    
    if actions:
        success, failed = await async_bulk(es, actions, raise_on_error=False, refresh=True)
        logger.info(f"Products: Bulk upserted metadata: success={success}, failed={len(failed) if failed else 0}")
    
    # Step 2: Query for docs missing embedding
    embedded_count = 0
    if with_embedding:
        try:
            query = {
                "bool": {
                    "filter": [
                        {"term": {"customer_id": sanitized_customer_id}},
                    ],
                    "must_not": [
                        {"exists": {"field": f"{name_field}_embedding"}}
                    ]
                }
            }
            
            result = await es.search(
                index=PRODUCTS_INDEX,
                query=query,
                size=10000,
                _source=[name_field, "ma_san_pham"],
                routing=sanitized_customer_id,
            )
            
            docs_need_embedding = result.get("hits", {}).get("hits", [])
            logger.info(f"Products: Found {len(docs_need_embedding)} docs needing embedding")
            
            # Step 3: Generate embeddings and bulk update
            if docs_need_embedding:
                embed_actions = []
                for hit in docs_need_embedding:
                    doc_id = hit["_id"]
                    source = hit.get("_source", {})
                    name = source.get(name_field)
                    
                    if name:
                        try:
                            embedding = await _embed_name_with_google(str(name), decrypted_api_key)
                            if embedding:
                                embed_actions.append({
                                    "_op_type": "update",
                                    "_index": PRODUCTS_INDEX,
                                    "_id": doc_id,
                                    "routing": sanitized_customer_id,
                                    "doc": {f"{name_field}_embedding": embedding},
                                })
                                embedded_count += 1
                        except Exception as e:
                            logger.warning(f"Error embedding product {doc_id}: {e}")
                
                if embed_actions:
                    success, failed = await async_bulk(es, embed_actions, raise_on_error=False, refresh=True)
                    logger.info(f"Products: Bulk updated embeddings: success={success}, failed={len(failed) if failed else 0}")
        
        except Exception as e:
            logger.warning(f"Error querying docs needing embedding: {e}")
    
    logger.info(f"Products: {embedded_count} newly embedded")
    return {"success": len(actions), "embedded": embedded_count}


async def _async_bulk_delete_products(customer_id: str, product_codes: List[str]) -> Dict[str, Any]:
    """Internal async function to bulk delete products."""
    if not product_codes:
        return {"deleted": 0}
    
    es = await get_es_client()
    sanitized_customer_id = sanitize_for_es(customer_id)
    
    query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"customer_id": sanitized_customer_id}},
                    {"terms": {"ma_san_pham": product_codes}},
                ]
            }
        }
    }
    
    response = await es.delete_by_query(
        index=PRODUCTS_INDEX,
        body=query,
        refresh=True,
        routing=sanitized_customer_id,
    )
    deleted = response.body.get("deleted", 0)
    logger.info(f"Bulk deleted products: {deleted}")
    return response.body


async def _async_delete_all_products(customer_id: str) -> Dict[str, Any]:
    """Internal async function to delete all products for a customer."""
    es = await get_es_client()
    sanitized_customer_id = sanitize_for_es(customer_id)
    
    query = {"query": {"term": {"customer_id": sanitized_customer_id}}}
    
    response = await es.delete_by_query(
        index=PRODUCTS_INDEX,
        body=query,
        refresh=True,
        routing=sanitized_customer_id,
    )
    deleted = response.body.get("deleted", 0)
    logger.info(f"Deleted all products for customer {customer_id}: {deleted}")
    return response.body


# Celery Tasks for Products

@celery_app.task(
    name="es_upsert_product",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
)
def es_upsert_product(
    self,
    customer_id: str,
    product_data: Dict[str, Any],
    with_embedding: bool = True,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Celery task to upsert a product to Elasticsearch with retry."""
    loop = _get_loop()
    return loop.run_until_complete(
        _async_upsert_product(customer_id, product_data, with_embedding, api_key)
    )


@celery_app.task(
    name="es_delete_product",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
)
def es_delete_product(self, customer_id: str, product_code: str) -> Dict[str, Any]:
    """Celery task to delete a product from Elasticsearch with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_delete_product(customer_id, product_code))


@celery_app.task(
    name="es_bulk_upsert_products",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
    time_limit=300,
)
def es_bulk_upsert_products(
    self,
    customer_id: str,
    products: List[Dict[str, Any]],
    with_embedding: bool = True,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Celery task to bulk upsert products to Elasticsearch with retry."""
    loop = _get_loop()
    return loop.run_until_complete(
        _async_bulk_upsert_products(customer_id, products, with_embedding, api_key)
    )


@celery_app.task(
    name="es_bulk_delete_products",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
)
def es_bulk_delete_products(self, customer_id: str, product_codes: List[str]) -> Dict[str, Any]:
    """Celery task to bulk delete products from Elasticsearch with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_bulk_delete_products(customer_id, product_codes))


@celery_app.task(
    name="es_delete_all_products",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
)
def es_delete_all_products(self, customer_id: str) -> Dict[str, Any]:
    """Celery task to delete all products for a customer from Elasticsearch with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_delete_all_products(customer_id))


# ========================= SERVICES =========================

async def _async_upsert_service(
    customer_id: str,
    service_data: Dict[str, Any],
    with_embedding: bool = True,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Internal async function to upsert a service."""
    es = await get_es_client()
    sanitized_customer_id = sanitize_for_es(customer_id)
    doc_id = service_data.get("ma_dich_vu")
    
    if not doc_id:
        raise ValueError("Missing 'ma_dich_vu' in service data")
    
    sanitized_doc_id = sanitize_for_es(str(doc_id))
    composite_id = f"{sanitized_customer_id}_{sanitized_doc_id}"
    
    service_data["customer_id"] = sanitized_customer_id
    
    if with_embedding:
        name_field = "ten_dich_vu"
        new_name = service_data.get(name_field)
        
        old_embedding = None
        old_name = None
        try:
            existing = await es.get(
                index=SERVICES_INDEX,
                id=composite_id,
                routing=sanitized_customer_id,
            )
            if existing and existing.get("found"):
                existing_source = existing.get("_source", {})
                old_name = existing_source.get(name_field)
                old_embedding = existing_source.get(f"{name_field}_embedding")
        except NotFoundError:
            pass
        except Exception as e:
            logger.warning(f"Error checking existing service: {e}")
        
        embedding = old_embedding
        if (new_name or "").strip() != (old_name or "").strip():
            try:
                embedding = await _embed_name_with_google(str(new_name) if new_name else "", api_key)
            except Exception as e:
                logger.warning(f"Error embedding service: {e}")
        
        if embedding:
            service_data[f"{name_field}_embedding"] = embedding
    
    response = await es.index(
        index=SERVICES_INDEX,
        id=composite_id,
        document=service_data,
        routing=sanitized_customer_id,
        refresh=True,
    )
    logger.info(f"Upserted service to Elasticsearch: {doc_id}")
    return response.body


async def _async_delete_service(customer_id: str, service_code: str) -> Dict[str, Any]:
    """Internal async function to delete a service."""
    es = await get_es_client()
    sanitized_customer_id = sanitize_for_es(customer_id)
    sanitized_doc_id = sanitize_for_es(str(service_code))
    composite_id = f"{sanitized_customer_id}_{sanitized_doc_id}"
    
    try:
        response = await es.delete(
            index=SERVICES_INDEX,
            id=composite_id,
            routing=sanitized_customer_id,
            refresh=True,
        )
        logger.info(f"Deleted service from Elasticsearch: {service_code}")
        return response.body
    except NotFoundError:
        logger.warning(f"Service not found in Elasticsearch: {service_code}")
        return {"result": "not_found"}


async def _async_bulk_upsert_services(
    customer_id: str,
    services: List[Dict[str, Any]],
    with_embedding: bool = True,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Internal async function to bulk upsert services."""
    es = await get_es_client()
    sanitized_customer_id = sanitize_for_es(customer_id)
    
    # Decrypt api_key if provided
    decrypted_api_key = None
    if api_key:
        try:
            decrypted_api_key = token_encryption.decrypt(api_key)
        except Exception as e:
            logger.warning(f"Failed to decrypt api_key: {e}")
    
    name_field = "ten_dich_vu"
    
    # Step 1: Bulk upsert all docs with metadata (use script to preserve existing embedding)
    actions = []
    for service_data in services:
        doc_id = service_data.get("ma_dich_vu")
        if not doc_id:
            continue
        
        sanitized_doc_id = sanitize_for_es(str(doc_id))
        composite_id = f"{sanitized_customer_id}_{sanitized_doc_id}"
        service_data["customer_id"] = sanitized_customer_id
        
        # Use scripted upsert to preserve existing embedding
        actions.append({
            "_op_type": "update",
            "_index": SERVICES_INDEX,
            "_id": composite_id,
            "routing": sanitized_customer_id,
            "script": {
                "source": """
                    for (entry in params.data.entrySet()) {
                        if (entry.getKey() != 'ten_dich_vu_embedding') {
                            ctx._source[entry.getKey()] = entry.getValue();
                        }
                    }
                    if (ctx._source.ten_dich_vu != params.data.ten_dich_vu) {
                        ctx._source.remove('ten_dich_vu_embedding');
                    }
                """,
                "params": {"data": service_data}
            },
            "upsert": service_data,
        })
    
    if actions:
        success, failed = await async_bulk(es, actions, raise_on_error=False, refresh=True)
        logger.info(f"Services: Bulk upserted metadata: success={success}, failed={len(failed) if failed else 0}")
    
    # Step 2: Query for docs missing embedding
    embedded_count = 0
    if with_embedding:
        try:
            query = {
                "bool": {
                    "filter": [
                        {"term": {"customer_id": sanitized_customer_id}},
                    ],
                    "must_not": [
                        {"exists": {"field": f"{name_field}_embedding"}}
                    ]
                }
            }
            
            result = await es.search(
                index=SERVICES_INDEX,
                query=query,
                size=10000,
                _source=[name_field, "ma_dich_vu"],
                routing=sanitized_customer_id,
            )
            
            docs_need_embedding = result.get("hits", {}).get("hits", [])
            logger.info(f"Services: Found {len(docs_need_embedding)} docs needing embedding")
            
            # Step 3: Generate embeddings and bulk update
            if docs_need_embedding:
                embed_actions = []
                for hit in docs_need_embedding:
                    doc_id = hit["_id"]
                    source = hit.get("_source", {})
                    name = source.get(name_field)
                    
                    if name:
                        try:
                            embedding = await _embed_name_with_google(str(name), decrypted_api_key)
                            if embedding:
                                embed_actions.append({
                                    "_op_type": "update",
                                    "_index": SERVICES_INDEX,
                                    "_id": doc_id,
                                    "routing": sanitized_customer_id,
                                    "doc": {f"{name_field}_embedding": embedding},
                                })
                                embedded_count += 1
                        except Exception as e:
                            logger.warning(f"Error embedding service {doc_id}: {e}")
                
                if embed_actions:
                    success, failed = await async_bulk(es, embed_actions, raise_on_error=False, refresh=True)
                    logger.info(f"Services: Bulk updated embeddings: success={success}, failed={len(failed) if failed else 0}")
        
        except Exception as e:
            logger.warning(f"Error querying docs needing embedding: {e}")
    
    logger.info(f"Services: {embedded_count} newly embedded")
    return {"success": len(actions), "embedded": embedded_count}


async def _async_bulk_delete_services(customer_id: str, service_codes: List[str]) -> Dict[str, Any]:
    """Internal async function to bulk delete services."""
    if not service_codes:
        return {"deleted": 0}
    
    es = await get_es_client()
    sanitized_customer_id = sanitize_for_es(customer_id)
    
    query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"customer_id": sanitized_customer_id}},
                    {"terms": {"ma_dich_vu": service_codes}},
                ]
            }
        }
    }
    
    response = await es.delete_by_query(
        index=SERVICES_INDEX,
        body=query,
        refresh=True,
        routing=sanitized_customer_id,
    )
    deleted = response.body.get("deleted", 0)
    logger.info(f"Bulk deleted services: {deleted}")
    return response.body


async def _async_delete_all_services(customer_id: str) -> Dict[str, Any]:
    """Internal async function to delete all services for a customer."""
    es = await get_es_client()
    sanitized_customer_id = sanitize_for_es(customer_id)
    
    query = {"query": {"term": {"customer_id": sanitized_customer_id}}}
    
    response = await es.delete_by_query(
        index=SERVICES_INDEX,
        body=query,
        refresh=True,
        routing=sanitized_customer_id,
    )
    deleted = response.body.get("deleted", 0)
    logger.info(f"Deleted all services for customer {customer_id}: {deleted}")
    return response.body


# Celery Tasks for Services

@celery_app.task(
    name="es_upsert_service",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
)
def es_upsert_service(
    self,
    customer_id: str,
    service_data: Dict[str, Any],
    with_embedding: bool = True,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Celery task to upsert a service to Elasticsearch with retry."""
    loop = _get_loop()
    return loop.run_until_complete(
        _async_upsert_service(customer_id, service_data, with_embedding, api_key)
    )


@celery_app.task(
    name="es_delete_service",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
)
def es_delete_service(self, customer_id: str, service_code: str) -> Dict[str, Any]:
    """Celery task to delete a service from Elasticsearch with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_delete_service(customer_id, service_code))


@celery_app.task(
    name="es_bulk_upsert_services",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
    time_limit=300,
)
def es_bulk_upsert_services(
    self,
    customer_id: str,
    services: List[Dict[str, Any]],
    with_embedding: bool = True,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Celery task to bulk upsert services to Elasticsearch with retry."""
    loop = _get_loop()
    return loop.run_until_complete(
        _async_bulk_upsert_services(customer_id, services, with_embedding, api_key)
    )


@celery_app.task(
    name="es_bulk_delete_services",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
)
def es_bulk_delete_services(self, customer_id: str, service_codes: List[str]) -> Dict[str, Any]:
    """Celery task to bulk delete services from Elasticsearch with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_bulk_delete_services(customer_id, service_codes))


@celery_app.task(
    name="es_delete_all_services",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
)
def es_delete_all_services(self, customer_id: str) -> Dict[str, Any]:
    """Celery task to delete all services for a customer from Elasticsearch with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_delete_all_services(customer_id))


# ========================= ACCESSORIES =========================

async def _async_upsert_accessory(
    customer_id: str,
    accessory_data: Dict[str, Any],
    with_embedding: bool = True,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Internal async function to upsert an accessory."""
    es = await get_es_client()
    sanitized_customer_id = sanitize_for_es(customer_id)
    doc_id = accessory_data.get("accessory_code")
    
    if not doc_id:
        raise ValueError("Missing 'accessory_code' in accessory data")
    
    sanitized_doc_id = sanitize_for_es(str(doc_id))
    composite_id = f"{sanitized_customer_id}_{sanitized_doc_id}"
    
    accessory_data["customer_id"] = sanitized_customer_id
    
    if with_embedding:
        name_field = "accessory_name"
        new_name = accessory_data.get(name_field)
        
        old_embedding = None
        old_name = None
        try:
            existing = await es.get(
                index=ACCESSORIES_INDEX,
                id=composite_id,
                routing=sanitized_customer_id,
            )
            if existing and existing.get("found"):
                existing_source = existing.get("_source", {})
                old_name = existing_source.get(name_field)
                old_embedding = existing_source.get(f"{name_field}_embedding")
        except NotFoundError:
            pass
        except Exception as e:
            logger.warning(f"Error checking existing accessory: {e}")
        
        embedding = old_embedding
        if (new_name or "").strip() != (old_name or "").strip():
            try:
                embedding = await _embed_name_with_google(str(new_name) if new_name else "", api_key)
            except Exception as e:
                logger.warning(f"Error embedding accessory: {e}")
        
        if embedding:
            accessory_data[f"{name_field}_embedding"] = embedding
    
    response = await es.index(
        index=ACCESSORIES_INDEX,
        id=composite_id,
        document=accessory_data,
        routing=sanitized_customer_id,
        refresh=True,
    )
    logger.info(f"Upserted accessory to Elasticsearch: {doc_id}")
    return response.body


async def _async_delete_accessory(customer_id: str, accessory_code: str) -> Dict[str, Any]:
    """Internal async function to delete an accessory."""
    es = await get_es_client()
    sanitized_customer_id = sanitize_for_es(customer_id)
    sanitized_doc_id = sanitize_for_es(str(accessory_code))
    composite_id = f"{sanitized_customer_id}_{sanitized_doc_id}"
    
    try:
        response = await es.delete(
            index=ACCESSORIES_INDEX,
            id=composite_id,
            routing=sanitized_customer_id,
            refresh=True,
        )
        logger.info(f"Deleted accessory from Elasticsearch: {accessory_code}")
        return response.body
    except NotFoundError:
        logger.warning(f"Accessory not found in Elasticsearch: {accessory_code}")
        return {"result": "not_found"}


async def _async_bulk_upsert_accessories(
    customer_id: str,
    accessories: List[Dict[str, Any]],
    with_embedding: bool = True,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Internal async function to bulk upsert accessories."""
    es = await get_es_client()
    sanitized_customer_id = sanitize_for_es(customer_id)
    
    # Decrypt api_key if provided
    decrypted_api_key = None
    if api_key:
        try:
            decrypted_api_key = token_encryption.decrypt(api_key)
        except Exception as e:
            logger.warning(f"Failed to decrypt api_key: {e}")
    
    name_field = "accessory_name"
    
    # Step 1: Bulk upsert all docs with metadata (use script to preserve existing embedding)
    actions = []
    for accessory_data in accessories:
        doc_id = accessory_data.get("accessory_code")
        if not doc_id:
            continue
        
        sanitized_doc_id = sanitize_for_es(str(doc_id))
        composite_id = f"{sanitized_customer_id}_{sanitized_doc_id}"
        accessory_data["customer_id"] = sanitized_customer_id
        
        # Use scripted upsert to preserve existing embedding
        actions.append({
            "_op_type": "update",
            "_index": ACCESSORIES_INDEX,
            "_id": composite_id,
            "routing": sanitized_customer_id,
            "script": {
                "source": """
                    for (entry in params.data.entrySet()) {
                        if (entry.getKey() != 'accessory_name_embedding') {
                            ctx._source[entry.getKey()] = entry.getValue();
                        }
                    }
                    if (ctx._source.accessory_name != params.data.accessory_name) {
                        ctx._source.remove('accessory_name_embedding');
                    }
                """,
                "params": {"data": accessory_data}
            },
            "upsert": accessory_data,
        })
    
    if actions:
        success, failed = await async_bulk(es, actions, raise_on_error=False, refresh=True)
        logger.info(f"Accessories: Bulk upserted metadata: success={success}, failed={len(failed) if failed else 0}")
    
    # Step 2: Query for docs missing embedding
    embedded_count = 0
    if with_embedding:
        try:
            query = {
                "bool": {
                    "filter": [
                        {"term": {"customer_id": sanitized_customer_id}},
                    ],
                    "must_not": [
                        {"exists": {"field": f"{name_field}_embedding"}}
                    ]
                }
            }
            
            result = await es.search(
                index=ACCESSORIES_INDEX,
                query=query,
                size=10000,
                _source=[name_field, "accessory_code"],
                routing=sanitized_customer_id,
            )
            
            docs_need_embedding = result.get("hits", {}).get("hits", [])
            logger.info(f"Accessories: Found {len(docs_need_embedding)} docs needing embedding")
            
            # Step 3: Generate embeddings and bulk update
            if docs_need_embedding:
                embed_actions = []
                for hit in docs_need_embedding:
                    doc_id = hit["_id"]
                    source = hit.get("_source", {})
                    name = source.get(name_field)
                    
                    if name:
                        try:
                            embedding = await _embed_name_with_google(str(name), decrypted_api_key)
                            if embedding:
                                embed_actions.append({
                                    "_op_type": "update",
                                    "_index": ACCESSORIES_INDEX,
                                    "_id": doc_id,
                                    "routing": sanitized_customer_id,
                                    "doc": {f"{name_field}_embedding": embedding},
                                })
                                embedded_count += 1
                        except Exception as e:
                            logger.warning(f"Error embedding accessory {doc_id}: {e}")
                
                if embed_actions:
                    success, failed = await async_bulk(es, embed_actions, raise_on_error=False, refresh=True)
                    logger.info(f"Accessories: Bulk updated embeddings: success={success}, failed={len(failed) if failed else 0}")
        
        except Exception as e:
            logger.warning(f"Error querying docs needing embedding: {e}")
    
    logger.info(f"Accessories: {embedded_count} newly embedded")
    return {"success": len(actions), "embedded": embedded_count}


async def _async_bulk_delete_accessories(customer_id: str, accessory_codes: List[str]) -> Dict[str, Any]:
    """Internal async function to bulk delete accessories."""
    if not accessory_codes:
        return {"deleted": 0}
    
    es = await get_es_client()
    sanitized_customer_id = sanitize_for_es(customer_id)
    
    query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"customer_id": sanitized_customer_id}},
                    {"terms": {"accessory_code": accessory_codes}},
                ]
            }
        }
    }
    
    response = await es.delete_by_query(
        index=ACCESSORIES_INDEX,
        body=query,
        refresh=True,
        routing=sanitized_customer_id,
    )
    deleted = response.body.get("deleted", 0)
    logger.info(f"Bulk deleted accessories: {deleted}")
    return response.body


async def _async_delete_all_accessories(customer_id: str) -> Dict[str, Any]:
    """Internal async function to delete all accessories for a customer."""
    es = await get_es_client()
    sanitized_customer_id = sanitize_for_es(customer_id)
    
    query = {"query": {"term": {"customer_id": sanitized_customer_id}}}
    
    response = await es.delete_by_query(
        index=ACCESSORIES_INDEX,
        body=query,
        refresh=True,
        routing=sanitized_customer_id,
    )
    deleted = response.body.get("deleted", 0)
    logger.info(f"Deleted all accessories for customer {customer_id}: {deleted}")
    return response.body


# Celery Tasks for Accessories

@celery_app.task(
    name="es_upsert_accessory",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
)
def es_upsert_accessory(
    self,
    customer_id: str,
    accessory_data: Dict[str, Any],
    with_embedding: bool = True,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Celery task to upsert an accessory to Elasticsearch with retry."""
    loop = _get_loop()
    return loop.run_until_complete(
        _async_upsert_accessory(customer_id, accessory_data, with_embedding, api_key)
    )


@celery_app.task(
    name="es_delete_accessory",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
)
def es_delete_accessory(self, customer_id: str, accessory_code: str) -> Dict[str, Any]:
    """Celery task to delete an accessory from Elasticsearch with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_delete_accessory(customer_id, accessory_code))


@celery_app.task(
    name="es_bulk_upsert_accessories",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
    time_limit=300,
)
def es_bulk_upsert_accessories(
    self,
    customer_id: str,
    accessories: List[Dict[str, Any]],
    with_embedding: bool = True,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Celery task to bulk upsert accessories to Elasticsearch with retry."""
    loop = _get_loop()
    return loop.run_until_complete(
        _async_bulk_upsert_accessories(customer_id, accessories, with_embedding, api_key)
    )


@celery_app.task(
    name="es_bulk_delete_accessories",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
)
def es_bulk_delete_accessories(self, customer_id: str, accessory_codes: List[str]) -> Dict[str, Any]:
    """Celery task to bulk delete accessories from Elasticsearch with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_bulk_delete_accessories(customer_id, accessory_codes))


@celery_app.task(
    name="es_delete_all_accessories",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
)
def es_delete_all_accessories(self, customer_id: str) -> Dict[str, Any]:
    """Celery task to delete all accessories for a customer from Elasticsearch with retry."""
    loop = _get_loop()
    return loop.run_until_complete(_async_delete_all_accessories(customer_id))
