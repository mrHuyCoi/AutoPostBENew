"""
Data Sync Service - Synchronize PostgreSQL data with Elasticsearch.

This service provides mechanisms to:
1. Track failed ES sync operations
2. Retry failed syncs
3. Full resync from PostgreSQL to Elasticsearch
4. Compare and reconcile data between the two datastores
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
import uuid

from app.services.elasticsearch_service import ElasticsearchService
from app.services.chatbot_service import get_product_data, get_service_data
from app.repositories.user_device_repository import UserDeviceRepository
from app.repositories.product_component_repository import ProductComponentRepository
from app.repositories.brand_repository import BrandRepository
from app.models.user import User
from app.database.database import async_session

logger = logging.getLogger(__name__)


class DataSyncService:
    """Service for synchronizing data between PostgreSQL and Elasticsearch."""

    # ========================= PRODUCTS (UserDevices) =========================

    @staticmethod
    async def sync_all_products_to_es(
        db: AsyncSession,
        user: User,
        batch_size: int = 100,
    ) -> Dict[str, Any]:
        """
        Sync all products (user_devices) from PostgreSQL to Elasticsearch for a user.
        
        Args:
            db: Database session
            user: User object
            batch_size: Number of records to process per batch
            
        Returns:
            Dict with success_count, failed_count, and errors
        """
        customer_id = str(user.id)
        success_count = 0
        failed_count = 0
        errors: List[Dict[str, Any]] = []
        
        try:
            # Get all user devices
            user_devices = await UserDeviceRepository.get_by_user_id_with_details(
                db, user.id, include_deleted=False
            )
            
            if not user_devices:
                logger.info(f"No products to sync for user {customer_id}")
                return {
                    "success_count": 0,
                    "failed_count": 0,
                    "total": 0,
                    "errors": [],
                }
            
            # Process in batches
            products_data = []
            for device in user_devices:
                try:
                    product_data = await get_product_data(db, device)
                    if product_data.get("ma_san_pham"):
                        products_data.append(product_data)
                except Exception as e:
                    failed_count += 1
                    errors.append({
                        "id": str(device.id),
                        "product_code": device.product_code,
                        "error": f"Failed to prepare data: {str(e)}",
                    })
            
            # Bulk upsert to Elasticsearch
            if products_data:
                try:
                    result = await ElasticsearchService.bulk_upsert_products(
                        customer_id, products_data, with_embedding=True, api_key=user.gemini_api_key
                    )
                    success_count = result.get("success", 0)
                    bulk_failed = result.get("failed", 0)
                    failed_count += bulk_failed
                except Exception as e:
                    logger.error(f"Bulk upsert products failed: {e}")
                    failed_count += len(products_data)
                    errors.append({
                        "batch": "all",
                        "error": f"Bulk upsert failed: {str(e)}",
                    })
            
            logger.info(
                f"Product sync completed for user {customer_id}: "
                f"{success_count} success, {failed_count} failed"
            )
            
            return {
                "success_count": success_count,
                "failed_count": failed_count,
                "total": len(user_devices),
                "errors": errors[:10],  # Limit error details
            }
            
        except Exception as e:
            logger.error(f"Error syncing products for user {customer_id}: {e}")
            return {
                "success_count": 0,
                "failed_count": 0,
                "total": 0,
                "errors": [{"error": str(e)}],
            }

    @staticmethod
    async def sync_single_product_to_es(
        db: AsyncSession,
        user_device_id: uuid.UUID,
        user: User,
    ) -> Tuple[bool, Optional[str]]:
        """
        Sync a single product to Elasticsearch.
        
        Returns:
            Tuple of (success: bool, error_message: Optional[str])
        """
        customer_id = str(user.id)
        
        try:
            user_device = await UserDeviceRepository.get_by_id_with_details(db, user_device_id)
            if not user_device:
                return False, "Product not found"
            
            product_data = await get_product_data(db, user_device)
            await ElasticsearchService.upsert_product(customer_id, product_data, api_key=user.gemini_api_key)
            
            logger.info(f"Synced product {user_device.product_code} to ES")
            return True, None
            
        except Exception as e:
            error_msg = f"Failed to sync product {user_device_id}: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

    # ========================= ACCESSORIES (ProductComponents) =========================

    @staticmethod
    async def sync_all_accessories_to_es(
        db: AsyncSession,
        user: User,
        batch_size: int = 100,
    ) -> Dict[str, Any]:
        """
        Sync all accessories (product_components) from PostgreSQL to Elasticsearch for a user.
        """
        customer_id = str(user.id)
        success_count = 0
        failed_count = 0
        errors: List[Dict[str, Any]] = []
        
        try:
            # Get all product components for user
            components_response = await ProductComponentRepository.get_all(
                db, user_id=user.id, skip=0, limit=100000
            )
            components = components_response.get("data", [])
            
            if not components:
                logger.info(f"No accessories to sync for user {customer_id}")
                return {
                    "success_count": 0,
                    "failed_count": 0,
                    "total": 0,
                    "errors": [],
                }
            
            # Prepare accessory data
            accessories_data = []
            for component in components:
                try:
                    accessory_data = {
                        "accessory_code": component.product_code,
                        "accessory_name": component.product_name,
                        "lifecare_price": float(component.amount) if component.amount else None,
                        "sale_price": float(component.wholesale_price) if component.wholesale_price else None,
                        "trademark": component.trademark,
                        "guarantee": component.guarantee,
                        "inventory": component.stock,
                        "specifications": component.description,
                        "avatar_images": component.product_photo,
                        "link_accessory": component.product_link,
                        "category": component.category,
                        "properties": component.properties,
                    }
                    if accessory_data.get("accessory_code"):
                        accessories_data.append(accessory_data)
                except Exception as e:
                    failed_count += 1
                    errors.append({
                        "id": str(component.id),
                        "product_code": component.product_code,
                        "error": f"Failed to prepare data: {str(e)}",
                    })
            
            # Bulk upsert to Elasticsearch
            if accessories_data:
                try:
                    result = await ElasticsearchService.bulk_upsert_accessories(
                        customer_id, accessories_data, with_embedding=True, api_key=user.gemini_api_key
                    )
                    success_count = result.get("success", 0)
                    bulk_failed = result.get("failed", 0)
                    failed_count += bulk_failed
                except Exception as e:
                    logger.error(f"Bulk upsert accessories failed: {e}")
                    failed_count += len(accessories_data)
                    errors.append({
                        "batch": "all",
                        "error": f"Bulk upsert failed: {str(e)}",
                    })
            
            logger.info(
                f"Accessory sync completed for user {customer_id}: "
                f"{success_count} success, {failed_count} failed"
            )
            
            return {
                "success_count": success_count,
                "failed_count": failed_count,
                "total": len(components),
                "errors": errors[:10],
            }
            
        except Exception as e:
            logger.error(f"Error syncing accessories for user {customer_id}: {e}")
            return {
                "success_count": 0,
                "failed_count": 0,
                "total": 0,
                "errors": [{"error": str(e)}],
            }

    @staticmethod
    async def sync_single_accessory_to_es(
        db: AsyncSession,
        component_id: uuid.UUID,
        user: User,
    ) -> Tuple[bool, Optional[str]]:
        """Sync a single accessory to Elasticsearch."""
        customer_id = str(user.id)
        
        try:
            component = await ProductComponentRepository.get_by_id_for_user(
                db, component_id, user.id
            )
            if not component:
                return False, "Accessory not found"
            
            accessory_data = {
                "accessory_code": component.product_code,
                "accessory_name": component.product_name,
                "lifecare_price": float(component.amount) if component.amount else None,
                "sale_price": float(component.wholesale_price) if component.wholesale_price else None,
                "trademark": component.trademark,
                "guarantee": component.guarantee,
                "inventory": component.stock,
                "specifications": component.description,
                "avatar_images": component.product_photo,
                "link_accessory": component.product_link,
                "category": component.category,
                "properties": component.properties,
            }
            
            await ElasticsearchService.upsert_accessory(customer_id, accessory_data, api_key=user.gemini_api_key)
            
            logger.info(f"Synced accessory {component.product_code} to ES")
            return True, None
            
        except Exception as e:
            error_msg = f"Failed to sync accessory {component_id}: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

    # ========================= SERVICES (Brands) =========================

    @staticmethod
    async def sync_all_services_to_es(
        db: AsyncSession,
        user: User,
        batch_size: int = 100,
    ) -> Dict[str, Any]:
        """
        Sync all services (brands) from PostgreSQL to Elasticsearch for a user.
        """
        customer_id = str(user.id)
        success_count = 0
        failed_count = 0
        errors: List[Dict[str, Any]] = []
        
        try:
            # Get all brands for user
            brands = await BrandRepository.get_by_user_id_with_details(db, user.id)
            
            if not brands:
                logger.info(f"No services to sync for user {customer_id}")
                return {
                    "success_count": 0,
                    "failed_count": 0,
                    "total": 0,
                    "errors": [],
                }
            
            # Prepare service data
            services_data = []
            for brand in brands:
                try:
                    service_data = await get_service_data(brand)
                    if service_data.get("ma_dich_vu"):
                        services_data.append(service_data)
                except Exception as e:
                    failed_count += 1
                    errors.append({
                        "id": str(brand.id),
                        "service_code": brand.service_code,
                        "error": f"Failed to prepare data: {str(e)}",
                    })
            
            # Bulk upsert to Elasticsearch
            if services_data:
                try:
                    result = await ElasticsearchService.bulk_upsert_services(
                        customer_id, services_data, with_embedding=True, api_key=user.gemini_api_key
                    )
                    success_count = result.get("success", 0)
                    bulk_failed = result.get("failed", 0)
                    failed_count += bulk_failed
                except Exception as e:
                    logger.error(f"Bulk upsert services failed: {e}")
                    failed_count += len(services_data)
                    errors.append({
                        "batch": "all",
                        "error": f"Bulk upsert failed: {str(e)}",
                    })
            
            logger.info(
                f"Service sync completed for user {customer_id}: "
                f"{success_count} success, {failed_count} failed"
            )
            
            return {
                "success_count": success_count,
                "failed_count": failed_count,
                "total": len(brands),
                "errors": errors[:10],
            }
            
        except Exception as e:
            logger.error(f"Error syncing services for user {customer_id}: {e}")
            return {
                "success_count": 0,
                "failed_count": 0,
                "total": 0,
                "errors": [{"error": str(e)}],
            }

    @staticmethod
    async def sync_single_service_to_es(
        db: AsyncSession,
        brand_id: uuid.UUID,
        user: User,
    ) -> Tuple[bool, Optional[str]]:
        """Sync a single service to Elasticsearch."""
        customer_id = str(user.id)
        
        try:
            brand = await BrandRepository.get_by_id_with_details(db, brand_id)
            if not brand:
                return False, "Service not found"
            
            service_data = await get_service_data(brand)
            await ElasticsearchService.upsert_service(customer_id, service_data, api_key=user.gemini_api_key)
            
            logger.info(f"Synced service {brand.service_code} to ES")
            return True, None
            
        except Exception as e:
            error_msg = f"Failed to sync service {brand_id}: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

    # ========================= FULL SYNC =========================

    @staticmethod
    async def sync_all_data_to_es(
        db: AsyncSession,
        user: User,
    ) -> Dict[str, Any]:
        """
        Sync all data types from PostgreSQL to Elasticsearch for a user.
        This is a full reconciliation.
        """
        customer_id = str(user.id)
        logger.info(f"Starting full data sync for user {customer_id}")
        
        results = {
            "products": {},
            "accessories": {},
            "services": {},
            "total_success": 0,
            "total_failed": 0,
            "sync_time": datetime.now(timezone.utc).isoformat(),
        }
        
        # Sync products
        products_result = await DataSyncService.sync_all_products_to_es(db, user)
        results["products"] = products_result
        results["total_success"] += products_result.get("success_count", 0)
        results["total_failed"] += products_result.get("failed_count", 0)
        
        # Sync accessories
        accessories_result = await DataSyncService.sync_all_accessories_to_es(db, user)
        results["accessories"] = accessories_result
        results["total_success"] += accessories_result.get("success_count", 0)
        results["total_failed"] += accessories_result.get("failed_count", 0)
        
        # Sync services
        services_result = await DataSyncService.sync_all_services_to_es(db, user)
        results["services"] = services_result
        results["total_success"] += services_result.get("success_count", 0)
        results["total_failed"] += services_result.get("failed_count", 0)
        
        logger.info(
            f"Full sync completed for user {customer_id}: "
            f"{results['total_success']} success, {results['total_failed']} failed"
        )
        
        return results

    # ========================= CLEAR ES DATA =========================

    @staticmethod
    async def clear_all_es_data(user: User) -> Dict[str, Any]:
        """
        Clear all Elasticsearch data for a user.
        Use with caution - this is destructive.
        """
        customer_id = str(user.id)
        results = {
            "products_deleted": 0,
            "accessories_deleted": 0,
            "services_deleted": 0,
        }
        
        try:
            # Clear products
            result = await ElasticsearchService.delete_all_products(customer_id)
            results["products_deleted"] = result.get("deleted", 0)
        except Exception as e:
            logger.error(f"Failed to clear products: {e}")
        
        try:
            # Clear accessories
            result = await ElasticsearchService.delete_all_accessories(customer_id)
            results["accessories_deleted"] = result.get("deleted", 0)
        except Exception as e:
            logger.error(f"Failed to clear accessories: {e}")
        
        try:
            # Clear services
            result = await ElasticsearchService.delete_all_services(customer_id)
            results["services_deleted"] = result.get("deleted", 0)
        except Exception as e:
            logger.error(f"Failed to clear services: {e}")
        
        logger.info(f"Cleared ES data for user {customer_id}: {results}")
        return results

    # ========================= HEALTH CHECK =========================

    @staticmethod
    async def check_es_connection() -> Dict[str, Any]:
        """Check if Elasticsearch is reachable."""
        try:
            from app.services.elasticsearch_service import get_es_client
            es = await get_es_client()
            info = await es.info()
            return {
                "connected": True,
                "cluster_name": info.get("cluster_name"),
                "version": info.get("version", {}).get("number"),
            }
        except Exception as e:
            logger.error(f"ES connection check failed: {e}")
            return {
                "connected": False,
                "error": str(e),
            }
