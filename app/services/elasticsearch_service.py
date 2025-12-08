"""
Elasticsearch Service - Direct connection to Elasticsearch.

This service allows dangbaitudong to insert/update/delete data directly into
Elasticsearch without going through the ChatbotMobileStore API.
Both services share the same network (shared_datanet) and Elasticsearch instance.
"""

import re
from typing import Dict, Any, List, Optional
from elasticsearch import AsyncElasticsearch, NotFoundError
from elasticsearch.helpers import async_bulk
from google import genai
from google.genai import types

from app.configs.settings import settings
from app.utils.crypto import token_encryption
from app.logging.logger import log_info, log_error, log_warning, log_debug
from app.logging.decorators import with_log_context

# Index names (must match ChatbotMobileStore)
PRODUCTS_INDEX = "products"
SERVICES_INDEX = "services"
ACCESSORIES_INDEX = "accessories"

# Singleton Elasticsearch client
_es_client: Optional[AsyncElasticsearch] = None
# Cache for genai clients per API key
_genai_clients: Dict[str, genai.Client] = {}


def sanitize_for_es(identifier: str) -> str:
    """Làm sạch một định danh để sử dụng an toàn trong routing và ID của Elasticsearch."""
    if not identifier:
        return ""
    return identifier.replace("-", "")


async def get_es_client() -> AsyncElasticsearch:
    """Get or create the Elasticsearch client."""
    global _es_client
    if _es_client is None:
        _es_client = AsyncElasticsearch(
            hosts=[settings.ELASTICSEARCH_URL],
            verify_certs=False,
            request_timeout=30,
        )
    return _es_client


async def close_es_client():
    """Close the Elasticsearch client."""
    global _es_client
    if _es_client is not None:
        await _es_client.close()
        _es_client = None


def _get_genai_client(api_key: Optional[str] = None) -> genai.Client:
    """Get or create the Google GenAI client for embeddings.
    
    Args:
        api_key: User's Gemini API key (already decrypted). If None, uses global settings.
    """
    global _genai_clients

    # Use provided user's API key if available, otherwise fall back to global settings
    effective_key = (api_key or "").strip() or settings.GEMINI_API_KEY or ""
    
    if effective_key in _genai_clients:
        return _genai_clients[effective_key]
    
    client = genai.Client(api_key=effective_key) if effective_key else genai.Client()
    _genai_clients[effective_key] = client
    return client


async def _embed_name_with_google(text: str, api_key: Optional[str] = None) -> List[float]:
    """Create embedding for a name using Google Gemini.
    
    Args:
        text: Text to embed
        api_key: User's Gemini API key from database. If None, uses global settings.
    
    Returns:
        list[float] to store directly in Elasticsearch dense_vector field.
    """
    if not text or not str(text).strip():
        return []

    try:
        client = _get_genai_client(api_key)
        resp = await client.aio.models.embed_content(
            model="gemini-embedding-001",
            contents=str(text),
            config=types.EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT"),
        )
        if getattr(resp, "embeddings", None):
            emb0 = resp.embeddings[0]
            return getattr(emb0, "values", None) or getattr(emb0, "embedding", None) or []
    except Exception as e:
        log_warning({"message": "Error creating embedding", "text": text, "error": str(e)})
    return []


class ElasticsearchService:
    """Direct Elasticsearch operations for products, services, and accessories.
    
    All methods support `async_mode` parameter:
    - async_mode=True (default): Queue operation via Celery for retry resilience
    - async_mode=False: Execute directly (used by Celery worker or when immediate result needed)
    """

    # ========================= PRODUCTS =========================

    @staticmethod
    @with_log_context(
        event_action="product.upsert",
        event_category="elasticsearch",
        source_layer="service",
        source_controller="elasticsearch_service",
    )
    async def upsert_product(
        customer_id: str,
        product_data: Dict[str, Any],
        with_embedding: bool = True,
        api_key: Optional[str] = None,
        async_mode: bool = False,
    ) -> Dict[str, Any]:
        """Insert or update a product in Elasticsearch.
        
        Args:
            customer_id: The customer/user ID
            product_data: Product data dict with fields like ma_san_pham, model, etc.
            with_embedding: Whether to generate embedding for the model field
            api_key: Gemini API key for embedding
            async_mode: If True, queue via Celery; if False, execute directly
            
        Returns:
            Elasticsearch response dict or Celery task info
        """
        if async_mode:
            from app.tasks.elasticsearch_tasks import es_upsert_product
            task = es_upsert_product.delay(
                customer_id=customer_id,
                product_data=product_data,
                with_embedding=with_embedding,
                api_key=api_key,
            )
            return {"status": "queued", "task_id": task.id}
        
        # Direct execution
        es = await get_es_client()
        sanitized_customer_id = sanitize_for_es(customer_id)
        doc_id = product_data.get("ma_san_pham")
        
        if not doc_id:
            raise ValueError("Missing 'ma_san_pham' in product data")
        
        sanitized_doc_id = sanitize_for_es(str(doc_id))
        composite_id = f"{sanitized_customer_id}_{sanitized_doc_id}"
        
        # Add customer_id to document
        product_data["customer_id"] = sanitized_customer_id
        
        # Handle embedding if requested
        if with_embedding:
            name_field = "model"
            new_name = product_data.get(name_field)
            
            # Check if document exists and if name changed
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
                log_warning({"message": "Error checking existing product", "composite_id": composite_id, "error": str(e)})
            
            # Only re-embed if name changed
            embedding = old_embedding
            if (new_name or "").strip() != (old_name or "").strip():
                try:
                    embedding = await _embed_name_with_google(str(new_name) if new_name else "", api_key)
                except Exception as e:
                    log_warning({"message": "Error embedding product", "doc_id": str(doc_id), "error": str(e)})
            
            if embedding:
                product_data[f"{name_field}_embedding"] = embedding
        
        try:
            response = await es.index(
                index=PRODUCTS_INDEX,
                id=composite_id,
                document=product_data,
                routing=sanitized_customer_id,
                refresh=True,
            )
            log_info({"message": "Upserted product to Elasticsearch", "doc_id": str(doc_id), "customer_id": customer_id})
            return response.body
        except Exception as e:
            log_error({"message": "Error upserting product", "doc_id": str(doc_id), "error": str(e)})
            raise

    @staticmethod
    @with_log_context(
        event_action="product.delete",
        event_category="elasticsearch",
        source_layer="service",
        source_controller="elasticsearch_service",
    )
    async def delete_product(customer_id: str, product_code: str, async_mode: bool = False) -> Dict[str, Any]:
        """Delete a product from Elasticsearch."""
        if async_mode:
            from app.tasks.elasticsearch_tasks import es_delete_product
            task = es_delete_product.delay(customer_id=customer_id, product_code=product_code)
            return {"status": "queued", "task_id": task.id}
        
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
            log_info({"message": "Deleted product from Elasticsearch", "product_code": product_code})
            return response.body
        except NotFoundError:
            log_warning({"message": "Product not found in Elasticsearch", "product_code": product_code})
            return {"result": "not_found"}
        except Exception as e:
            log_error({"message": "Error deleting product", "product_code": product_code, "error": str(e)})
            raise

    @staticmethod
    @with_log_context(
        event_action="product.bulk_upsert",
        event_category="elasticsearch",
        source_layer="service",
        source_controller="elasticsearch_service",
    )
    async def bulk_upsert_products(
        customer_id: str,
        products: List[Dict[str, Any]],
        with_embedding: bool = True,
        api_key: Optional[str] = None,
        async_mode: bool = False,
    ) -> Dict[str, Any]:
        """Bulk insert/update products in Elasticsearch."""
        if async_mode:
            from app.tasks.elasticsearch_tasks import es_bulk_upsert_products
            task = es_bulk_upsert_products.delay(
                customer_id=customer_id, products=products,
                with_embedding=with_embedding, api_key=api_key
            )
            return {"status": "queued", "task_id": task.id}
        
        es = await get_es_client()
        sanitized_customer_id = sanitize_for_es(customer_id)
        
        actions = []
        for product_data in products:
            doc_id = product_data.get("ma_san_pham")
            if not doc_id:
                continue
            
            sanitized_doc_id = sanitize_for_es(str(doc_id))
            composite_id = f"{sanitized_customer_id}_{sanitized_doc_id}"
            product_data["customer_id"] = sanitized_customer_id
            
            # Generate embedding if requested
            if with_embedding:
                name_field = "model"
                new_name = product_data.get(name_field)
                if new_name:
                    try:
                        embedding = await _embed_name_with_google(str(new_name), api_key)
                        if embedding:
                            product_data[f"{name_field}_embedding"] = embedding
                    except Exception as e:
                        log_warning({"message": "Error embedding product", "doc_id": str(doc_id), "error": str(e)})
            
            actions.append({
                "_index": PRODUCTS_INDEX,
                "_id": composite_id,
                "_source": product_data,
                "routing": sanitized_customer_id,
            })
        
        if not actions:
            return {"success": 0, "failed": 0}
        
        try:
            success, failed = await async_bulk(es, actions, raise_on_error=False, refresh=True)
            log_info({"message": "Bulk upserted products to Elasticsearch", "success": success, "failed": len(failed) if failed else 0})
            return {"success": success, "failed": len(failed) if failed else 0}
        except Exception as e:
            log_error({"message": "Error bulk upserting products", "error": str(e)})
            raise

    @staticmethod
    @with_log_context(
        event_action="product.bulk_delete",
        event_category="elasticsearch",
        source_layer="service",
        source_controller="elasticsearch_service",
    )
    async def bulk_delete_products(customer_id: str, product_codes: List[str], async_mode: bool = False) -> Dict[str, Any]:
        """Bulk delete products from Elasticsearch."""
        if async_mode:
            from app.tasks.elasticsearch_tasks import es_bulk_delete_products
            task = es_bulk_delete_products.delay(customer_id=customer_id, product_codes=product_codes)
            return {"status": "queued", "task_id": task.id}
        
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
        
        try:
            response = await es.delete_by_query(
                index=PRODUCTS_INDEX,
                body=query,
                refresh=True,
                routing=sanitized_customer_id,
            )
            deleted = response.body.get("deleted", 0)
            log_info({"message": "Bulk deleted products from Elasticsearch", "deleted": deleted})
            return response.body
        except Exception as e:
            log_error({"message": "Error bulk deleting products", "error": str(e)})
            raise

    @staticmethod
    @with_log_context(
        event_action="product.delete_all",
        event_category="elasticsearch",
        source_layer="service",
        source_controller="elasticsearch_service",
    )
    async def delete_all_products(customer_id: str, async_mode: bool = False) -> Dict[str, Any]:
        """Delete all products for a customer from Elasticsearch."""
        if async_mode:
            from app.tasks.elasticsearch_tasks import es_delete_all_products
            task = es_delete_all_products.delay(customer_id=customer_id)
            return {"status": "queued", "task_id": task.id}
        
        es = await get_es_client()
        sanitized_customer_id = sanitize_for_es(customer_id)
        
        query = {"query": {"term": {"customer_id": sanitized_customer_id}}}
        
        try:
            response = await es.delete_by_query(
                index=PRODUCTS_INDEX,
                body=query,
                refresh=True,
                routing=sanitized_customer_id,
            )
            deleted = response.body.get("deleted", 0)
            log_info({"message": "Deleted all products for customer", "customer_id": customer_id, "deleted": deleted})
            return response.body
        except Exception as e:
            log_error({"message": "Error deleting all products", "error": str(e)})
            raise

    # ========================= ACCESSORIES =========================

    @staticmethod
    @with_log_context(
        event_action="accessory.upsert",
        event_category="elasticsearch",
        source_layer="service",
        source_controller="elasticsearch_service",
    )
    async def upsert_accessory(
        customer_id: str,
        accessory_data: Dict[str, Any],
        with_embedding: bool = True,
        api_key: Optional[str] = None,
        async_mode: bool = False,
    ) -> Dict[str, Any]:
        """Insert or update an accessory in Elasticsearch."""
        if async_mode:
            from app.tasks.elasticsearch_tasks import es_upsert_accessory
            task = es_upsert_accessory.delay(
                customer_id=customer_id, accessory_data=accessory_data,
                with_embedding=with_embedding, api_key=api_key
            )
            return {"status": "queued", "task_id": task.id}
        
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
                log_warning({"message": "Error checking existing accessory", "composite_id": composite_id, "error": str(e)})
            
            embedding = old_embedding
            if (new_name or "").strip() != (old_name or "").strip():
                try:
                    embedding = await _embed_name_with_google(str(new_name) if new_name else "", api_key)
                except Exception as e:
                    log_warning({"message": "Error embedding accessory", "doc_id": str(doc_id), "error": str(e)})
            
            if embedding:
                accessory_data[f"{name_field}_embedding"] = embedding
        
        try:
            response = await es.index(
                index=ACCESSORIES_INDEX,
                id=composite_id,
                document=accessory_data,
                routing=sanitized_customer_id,
                refresh=True,
            )
            log_info({"message": "Upserted accessory to Elasticsearch", "doc_id": str(doc_id)})
            return response.body
        except Exception as e:
            log_error({"message": "Error upserting accessory", "doc_id": str(doc_id), "error": str(e)})
            raise

    @staticmethod
    @with_log_context(
        event_action="accessory.delete",
        event_category="elasticsearch",
        source_layer="service",
        source_controller="elasticsearch_service",
    )
    async def delete_accessory(customer_id: str, accessory_code: str, async_mode: bool = False) -> Dict[str, Any]:
        """Delete an accessory from Elasticsearch."""
        if async_mode:
            from app.tasks.elasticsearch_tasks import es_delete_accessory
            task = es_delete_accessory.delay(customer_id=customer_id, accessory_code=accessory_code)
            return {"status": "queued", "task_id": task.id}
        
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
            log_info({"message": "Deleted accessory from Elasticsearch", "accessory_code": accessory_code})
            return response.body
        except NotFoundError:
            log_warning({"message": "Accessory not found in Elasticsearch", "accessory_code": accessory_code})
            return {"result": "not_found"}
        except Exception as e:
            log_error({"message": "Error deleting accessory", "accessory_code": accessory_code, "error": str(e)})
            raise

    @staticmethod
    @with_log_context(
        event_action="accessory.bulk_upsert",
        event_category="elasticsearch",
        source_layer="service",
        source_controller="elasticsearch_service",
    )
    async def bulk_upsert_accessories(
        customer_id: str,
        accessories: List[Dict[str, Any]],
        with_embedding: bool = True,
        api_key: Optional[str] = None,
        async_mode: bool = False,
    ) -> Dict[str, Any]:
        """Bulk insert/update accessories in Elasticsearch."""
        if async_mode:
            from app.tasks.elasticsearch_tasks import es_bulk_upsert_accessories
            task = es_bulk_upsert_accessories.delay(
                customer_id=customer_id, accessories=accessories,
                with_embedding=with_embedding, api_key=api_key
            )
            return {"status": "queued", "task_id": task.id}
        
        es = await get_es_client()
        sanitized_customer_id = sanitize_for_es(customer_id)
        
        actions = []
        for accessory_data in accessories:
            doc_id = accessory_data.get("accessory_code")
            if not doc_id:
                continue
            
            sanitized_doc_id = sanitize_for_es(str(doc_id))
            composite_id = f"{sanitized_customer_id}_{sanitized_doc_id}"
            accessory_data["customer_id"] = sanitized_customer_id
            
            if with_embedding:
                name_field = "accessory_name"
                new_name = accessory_data.get(name_field)
                if new_name:
                    try:
                        embedding = await _embed_name_with_google(str(new_name), api_key)
                        if embedding:
                            accessory_data[f"{name_field}_embedding"] = embedding
                    except Exception as e:
                        log_warning({"message": "Error embedding accessory", "doc_id": str(doc_id), "error": str(e)})
            
            actions.append({
                "_index": ACCESSORIES_INDEX,
                "_id": composite_id,
                "_source": accessory_data,
                "routing": sanitized_customer_id,
            })
        
        if not actions:
            return {"success": 0, "failed": 0}
        
        try:
            success, failed = await async_bulk(es, actions, raise_on_error=False, refresh=True)
            log_info({"message": "Bulk upserted accessories to Elasticsearch", "success": success, "failed": len(failed) if failed else 0})
            return {"success": success, "failed": len(failed) if failed else 0}
        except Exception as e:
            log_error({"message": "Error bulk upserting accessories", "error": str(e)})
            raise

    @staticmethod
    @with_log_context(
        event_action="accessory.bulk_delete",
        event_category="elasticsearch",
        source_layer="service",
        source_controller="elasticsearch_service",
    )
    async def bulk_delete_accessories(customer_id: str, accessory_codes: List[str], async_mode: bool = False) -> Dict[str, Any]:
        """Bulk delete accessories from Elasticsearch."""
        if async_mode:
            from app.tasks.elasticsearch_tasks import es_bulk_delete_accessories
            task = es_bulk_delete_accessories.delay(customer_id=customer_id, accessory_codes=accessory_codes)
            return {"status": "queued", "task_id": task.id}
        
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
        
        try:
            response = await es.delete_by_query(
                index=ACCESSORIES_INDEX,
                body=query,
                refresh=True,
                routing=sanitized_customer_id,
            )
            deleted = response.body.get("deleted", 0)
            log_info({"message": "Bulk deleted accessories from Elasticsearch", "deleted": deleted})
            return response.body
        except Exception as e:
            log_error({"message": "Error bulk deleting accessories", "error": str(e)})
            raise

    @staticmethod
    @with_log_context(
        event_action="accessory.delete_all",
        event_category="elasticsearch",
        source_layer="service",
        source_controller="elasticsearch_service",
    )
    async def delete_all_accessories(customer_id: str, async_mode: bool = False) -> Dict[str, Any]:
        """Delete all accessories for a customer from Elasticsearch."""
        if async_mode:
            from app.tasks.elasticsearch_tasks import es_delete_all_accessories
            task = es_delete_all_accessories.delay(customer_id=customer_id)
            return {"status": "queued", "task_id": task.id}
        
        es = await get_es_client()
        sanitized_customer_id = sanitize_for_es(customer_id)
        
        query = {"query": {"term": {"customer_id": sanitized_customer_id}}}
        
        try:
            response = await es.delete_by_query(
                index=ACCESSORIES_INDEX,
                body=query,
                refresh=True,
                routing=sanitized_customer_id,
            )
            deleted = response.body.get("deleted", 0)
            log_info({"message": "Deleted all accessories for customer", "customer_id": customer_id, "deleted": deleted})
            return response.body
        except Exception as e:
            log_error({"message": "Error deleting all accessories", "error": str(e)})
            raise

    # ========================= SERVICES =========================

    @staticmethod
    @with_log_context(
        event_action="service.upsert",
        event_category="elasticsearch",
        source_layer="service",
        source_controller="elasticsearch_service",
    )
    async def upsert_service(
        customer_id: str,
        service_data: Dict[str, Any],
        with_embedding: bool = True,
        api_key: Optional[str] = None,
        async_mode: bool = False,
    ) -> Dict[str, Any]:
        """Insert or update a service in Elasticsearch."""
        if async_mode:
            from app.tasks.elasticsearch_tasks import es_upsert_service
            task = es_upsert_service.delay(
                customer_id=customer_id, service_data=service_data,
                with_embedding=with_embedding, api_key=api_key
            )
            return {"status": "queued", "task_id": task.id}
        
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
                log_warning({"message": "Error checking existing service", "composite_id": composite_id, "error": str(e)})
            
            embedding = old_embedding
            if (new_name or "").strip() != (old_name or "").strip():
                try:
                    embedding = await _embed_name_with_google(str(new_name) if new_name else "", api_key)
                except Exception as e:
                    log_warning({"message": "Error embedding service", "doc_id": str(doc_id), "error": str(e)})
            
            if embedding:
                service_data[f"{name_field}_embedding"] = embedding
        
        try:
            response = await es.index(
                index=SERVICES_INDEX,
                id=composite_id,
                document=service_data,
                routing=sanitized_customer_id,
                refresh=True,
            )
            log_info({"message": "Upserted service to Elasticsearch", "doc_id": str(doc_id)})
            return response.body
        except Exception as e:
            log_error({"message": "Error upserting service", "doc_id": str(doc_id), "error": str(e)})
            raise

    @staticmethod
    @with_log_context(
        event_action="service.delete",
        event_category="elasticsearch",
        source_layer="service",
        source_controller="elasticsearch_service",
    )
    async def delete_service(customer_id: str, service_code: str, async_mode: bool = False) -> Dict[str, Any]:
        """Delete a service from Elasticsearch."""
        if async_mode:
            from app.tasks.elasticsearch_tasks import es_delete_service
            task = es_delete_service.delay(customer_id=customer_id, service_code=service_code)
            return {"status": "queued", "task_id": task.id}
        
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
            log_info({"message": "Deleted service from Elasticsearch", "service_code": service_code})
            return response.body
        except NotFoundError:
            log_warning({"message": "Service not found in Elasticsearch", "service_code": service_code})
            return {"result": "not_found"}
        except Exception as e:
            log_error({"message": "Error deleting service", "service_code": service_code, "error": str(e)})
            raise

    @staticmethod
    @with_log_context(
        event_action="service.bulk_upsert",
        event_category="elasticsearch",
        source_layer="service",
        source_controller="elasticsearch_service",
    )
    async def bulk_upsert_services(
        customer_id: str,
        services: List[Dict[str, Any]],
        with_embedding: bool = True,
        api_key: Optional[str] = None,
        async_mode: bool = False,
    ) -> Dict[str, Any]:
        """Bulk insert/update services in Elasticsearch."""
        if async_mode:
            from app.tasks.elasticsearch_tasks import es_bulk_upsert_services
            task = es_bulk_upsert_services.delay(
                customer_id=customer_id, services=services,
                with_embedding=with_embedding, api_key=api_key
            )
            return {"status": "queued", "task_id": task.id}
        
        es = await get_es_client()
        sanitized_customer_id = sanitize_for_es(customer_id)
        
        actions = []
        for service_data in services:
            doc_id = service_data.get("ma_dich_vu")
            if not doc_id:
                continue
            
            sanitized_doc_id = sanitize_for_es(str(doc_id))
            composite_id = f"{sanitized_customer_id}_{sanitized_doc_id}"
            service_data["customer_id"] = sanitized_customer_id
            
            if with_embedding:
                name_field = "ten_dich_vu"
                new_name = service_data.get(name_field)
                if new_name:
                    try:
                        embedding = await _embed_name_with_google(str(new_name), api_key)
                        if embedding:
                            service_data[f"{name_field}_embedding"] = embedding
                    except Exception as e:
                        log_warning({"message": "Error embedding service", "doc_id": str(doc_id), "error": str(e)})
            
            actions.append({
                "_index": SERVICES_INDEX,
                "_id": composite_id,
                "_source": service_data,
                "routing": sanitized_customer_id,
            })
        
        if not actions:
            return {"success": 0, "failed": 0}
        
        try:
            success, failed = await async_bulk(es, actions, raise_on_error=False, refresh=True)
            log_info({"message": "Bulk upserted services to Elasticsearch", "success": success, "failed": len(failed) if failed else 0})
            return {"success": success, "failed": len(failed) if failed else 0}
        except Exception as e:
            log_error({"message": "Error bulk upserting services", "error": str(e)})
            raise

    @staticmethod
    @with_log_context(
        event_action="service.bulk_delete",
        event_category="elasticsearch",
        source_layer="service",
        source_controller="elasticsearch_service",
    )
    async def bulk_delete_services(customer_id: str, service_codes: List[str], async_mode: bool = False) -> Dict[str, Any]:
        """Bulk delete services from Elasticsearch."""
        if async_mode:
            from app.tasks.elasticsearch_tasks import es_bulk_delete_services
            task = es_bulk_delete_services.delay(customer_id=customer_id, service_codes=service_codes)
            return {"status": "queued", "task_id": task.id}
        
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
        
        try:
            response = await es.delete_by_query(
                index=SERVICES_INDEX,
                body=query,
                refresh=True,
                routing=sanitized_customer_id,
            )
            deleted = response.body.get("deleted", 0)
            log_info({"message": "Bulk deleted services from Elasticsearch", "deleted": deleted})
            return response.body
        except Exception as e:
            log_error({"message": "Error bulk deleting services", "error": str(e)})
            raise

    @staticmethod
    @with_log_context(
        event_action="service.delete_all",
        event_category="elasticsearch",
        source_layer="service",
        source_controller="elasticsearch_service",
    )
    async def delete_all_services(customer_id: str, async_mode: bool = False) -> Dict[str, Any]:
        """Delete all services for a customer from Elasticsearch."""
        if async_mode:
            from app.tasks.elasticsearch_tasks import es_delete_all_services
            task = es_delete_all_services.delay(customer_id=customer_id)
            return {"status": "queued", "task_id": task.id}
        
        es = await get_es_client()
        sanitized_customer_id = sanitize_for_es(customer_id)
        
        query = {"query": {"term": {"customer_id": sanitized_customer_id}}}
        
        try:
            response = await es.delete_by_query(
                index=SERVICES_INDEX,
                body=query,
                refresh=True,
                routing=sanitized_customer_id,
            )
            deleted = response.body.get("deleted", 0)
            log_info({"message": "Deleted all services for customer", "customer_id": customer_id, "deleted": deleted})
            return response.body
        except Exception as e:
            log_error({"message": "Error deleting all services", "error": str(e)})
            raise
