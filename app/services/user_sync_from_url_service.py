import httpx
import re
from typing import Dict, Any, List, Optional
from fastapi import BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
import uuid
import logging

from app.repositories.user_sync_url_repository import UserSyncUrlRepository
from app.repositories.device_info_repository import DeviceInfoRepository
from app.repositories.device_storage_repository import DeviceStorageRepository
from app.repositories.color_repository import ColorRepository
from app.repositories.user_device_from_url_repository import UserDeviceFromUrlRepository
from app.repositories.product_component_repository import ProductComponentRepository
from app.repositories.service_repository import ServiceRepository
from app.dto.device_info_dto import DeviceInfoCreate
from app.dto.product_component_dto import ProductComponentCreate, ProductComponentUpdate
from app.dto.service_dto import ServiceUpdate
from app.models.service import Service
from app.models.user import User
from app.database.database import async_session
from app.configs.settings import settings
from app.services.chatbot_service import get_product_data, ChatbotService
from app.services.elasticsearch_service import ElasticsearchService
from app.services.excel_service import ExcelService
from app.tasks.elasticsearch_tasks import (
    es_bulk_upsert_products,
    es_bulk_upsert_services,
    es_bulk_upsert_accessories,
)
import html


logger = logging.getLogger(__name__)


class UserSyncFromUrlService:
    @staticmethod
    async def _fetch_items(fetch_url: str) -> List[dict]:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(fetch_url)
            resp.raise_for_status()
            payload = resp.json()
        items = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            raise ValueError("Phản hồi API không hợp lệ: thiếu trường 'data' dạng mảng")
        return items

    @staticmethod
    def _append_query_param(url: str, key: str, value: str) -> str:
        if "?" in url:
            return f"{url}&{key}={value}"
        return f"{url}?{key}={value}"

    @staticmethod
    def _parse_capacity_to_gb(storage_str: Optional[str]) -> Optional[int]:
        if not storage_str:
            return None
        s = str(storage_str).strip().upper()
        match = re.search(r"(\d+(?:\.\d+)?)", s)
        if not match:
            return None
        num = float(match.group(1))
        if "TB" in s:
            return int(num * 1024)
        return int(num)

    @staticmethod
    def _to_float(v) -> Optional[float]:
        try:
            if v is None:
                return None
            s = str(v).strip().replace(",", "")
            return float(s)
        except Exception:
            return None

    @staticmethod
    def _to_int(v) -> int:
        try:
            if v is None:
                return 0
            return int(float(str(v).strip()))
        except Exception:
            return 0

    @staticmethod
    def _clean_text(v) -> Optional[str]:
        """
        Convert incoming value to a clean string:
        - Cast to str
        - Decode HTML entities (e.g., '&#x25;' -> '%', '&#40;' -> '(')
        - Strip whitespace
        Returns None if empty after cleaning.
        """
        if v is None:
            return None
        try:
            s = str(v)
        except Exception:
            return None
        s = html.unescape(s).strip()
        return s if s else None

    @staticmethod
    async def _bg_add_product_to_chatbot(entity_id: uuid.UUID, user_id: uuid.UUID):
        """
        Background task: add product from user_devices_from_url to Elasticsearch directly.
        """
        from app.repositories.user_repository import UserRepository
        async with async_session() as db:
            entity = await UserDeviceFromUrlRepository.get_by_id_with_details(db, entity_id)
            if not entity:
                return
            user = await UserRepository.get_by_id(db, user_id)
            if not user:
                return
            customer_id = str(user.id)
            product_data = await get_product_data(db, entity)
            try:
                await ElasticsearchService.upsert_product(customer_id, product_data, api_key=user.gemini_api_key)
            except Exception:
                pass

    @staticmethod
    async def _bg_update_product_to_chatbot(entity_id: uuid.UUID, user_id: uuid.UUID):
        """
        Background task: update product from user_devices_from_url in Elasticsearch directly.
        """
        from app.repositories.user_repository import UserRepository
        async with async_session() as db:
            entity = await UserDeviceFromUrlRepository.get_by_id_with_details(db, entity_id)
            if not entity or not entity.product_code:
                return
            user = await UserRepository.get_by_id(db, user_id)
            if not user:
                return
            customer_id = str(user.id)
            product_data = await get_product_data(db, entity)
            try:
                await ElasticsearchService.upsert_product(customer_id, product_data, api_key=user.gemini_api_key)
            except Exception:
                pass

    @staticmethod
    async def sync_devices(
        db: AsyncSession,
        user: User,
        updated_today: bool = False,
        background_tasks: Optional[BackgroundTasks] = None,
        fetch_url: Optional[str] = None,
        sync_url_id: Optional[uuid.UUID] = None,
    ) -> Dict[str, Any]:
        """
        Fetch devices from user's sync URL and upsert into user_devices.
        Does not require DB migrations. Uses existing tables and services.
        """
        if not fetch_url:
            record = await UserSyncUrlRepository.get_by_user_id(db, user.id, only_active=True, type_url="device")
            if not record or not record.url:
                raise ValueError("Chưa cấu hình hoặc đã vô hiệu hoá sync URL cho tài khoản này")
            # Prefer dedicated daily URL if provided
            fetch_url = record.url_today if (updated_today and record.url_today) else record.url
            if updated_today and not record.url_today:
                fetch_url = UserSyncFromUrlService._append_query_param(fetch_url, "updated_today", "true")
            # Make sure we carry the sync_url_id for create operations
            sync_url_id = record.id

        created = updated = skipped = 0
        errors: List[str] = []

        # Determine user scoping behavior (admin -> system scope)
        is_admin = getattr(user, "is_admin", False) or getattr(user, "role", "").lower() == "admin" or getattr(user, "is_superuser", False)
        user_scope_id = None if is_admin else user.id

        items = await UserSyncFromUrlService._fetch_items(fetch_url)

        processed_entities = []
        for idx, item in enumerate(items, start=1):
            try:
                # Map fields from incoming JSON
                model_name = UserSyncFromUrlService._clean_text(item.get("device_name") or item.get("model")) or ""
                if not model_name:
                    skipped += 1
                    continue

                color_name = UserSyncFromUrlService._clean_text(item.get("color"))
                storage_str = item.get("device_storage")
                product_code = UserSyncFromUrlService._clean_text(item.get("product_code"))
                warranty = UserSyncFromUrlService._clean_text(item.get("warranty"))
                device_condition = UserSyncFromUrlService._clean_text(item.get("device_condition")) or ""
                device_type = UserSyncFromUrlService._clean_text(item.get("device_type")) or ""
                battery_condition = UserSyncFromUrlService._clean_text(item.get("battery_condition"))
                price = UserSyncFromUrlService._to_float(item.get("price")) or 0.0
                wholesale_price = UserSyncFromUrlService._to_float(item.get("wholesale_price"))
                inventory = UserSyncFromUrlService._to_int(item.get("inventory"))
                notes = UserSyncFromUrlService._clean_text(item.get("notes"))

                # Do not insert into device_info; only use existing one if present.
                # device_info = await DeviceInfoRepository.get_by_model(db, model_name)
                # if not device_info:
                #     skipped += 1
                #     errors.append(f"Dòng {idx}: Không tìm thấy model '{model_name}' trong hệ thống, bỏ qua (không tạo mới device_info)")
                #     continue

                # Skip color and storage linking
                color_id = None
                device_storage_id = None

                # Upsert UserDeviceFromUrl
                background_tasks = background_tasks or BackgroundTasks()

                if product_code:
                    existing_from_url = await UserDeviceFromUrlRepository.get_by_product_code_and_user_id(db, product_code, user.id)
                    if existing_from_url:
                        update_data = {
                            "device_info_id": None,
                            "device_name": model_name,
                            "color_id": color_id,
                            "device_storage_id": device_storage_id,
                            "warranty": warranty,
                            "device_condition": device_condition,
                            "device_type": device_type,
                            "battery_condition": battery_condition,
                            "price": price,
                            "wholesale_price": wholesale_price,
                            "inventory": inventory,
                            "notes": notes,
                        }
                        updated_entity = await UserDeviceFromUrlRepository.update(db, existing_from_url.id, update_data)
                        if updated_entity:
                            processed_entities.append(updated_entity)
                            updated += 1
                            continue

                create_data = {
                    "user_id": user.id,
                    "sync_url_id": sync_url_id,
                    "device_info_id": None,
                    "device_name": model_name,
                    "color_id": color_id,
                    "device_storage_id": device_storage_id,
                    "product_code": product_code,
                    "warranty": warranty,
                    "device_condition": device_condition,
                    "device_type": device_type,
                    "battery_condition": battery_condition,
                    "price": price,
                    "wholesale_price": wholesale_price,
                    "inventory": inventory,
                    "notes": notes,
                }
                created_entity = await UserDeviceFromUrlRepository.create(db, create_data)
                processed_entities.append(created_entity)
                created += 1

            except Exception as e:
                errors.append(f"Dòng {idx}: {str(e)}")

        # After processing, generate Excel and bulk sync to Chatbot like Excel import flow
        if processed_entities:
            try:
                excel_bytes = await ExcelService._generate_chatbot_sync_excel(processed_entities)
                if background_tasks:
                    background_tasks.add_task(ChatbotService.bulk_sync_products_from_file, excel_bytes, user)
                else:
                    # Running in Celery or non-FastAPI context: call directly
                    await ChatbotService.bulk_sync_products_from_file(excel_bytes, user)
            except Exception:
                # Do not fail the whole sync if Excel or chatbot sync fails; surface as an error entry
                errors.append("Lỗi khi tạo file Excel hoặc đồng bộ hàng loạt với Chatbot")

            # Sync to Elasticsearch via Celery task after database sync
            try:
                es_products = []
                for entity in processed_entities:
                    es_products.append({
                        "ma_san_pham": entity.product_code,
                        "model": entity.device_name,
                        "gia_ban": entity.price,
                        "gia_si": entity.wholesale_price,
                        "warranty": entity.warranty,
                        "device_condition": entity.device_condition,
                        "device_type": entity.device_type,
                        "battery_condition": entity.battery_condition,
                        "inventory": entity.inventory,
                        "notes": entity.notes,
                    })
                if es_products:
                    # Trigger Celery task for ES bulk upsert
                    es_bulk_upsert_products.delay(
                        customer_id=str(user.id),
                        products=es_products,
                        with_embedding=True,
                        api_key=user.gemini_api_key,
                    )
                    logger.info(f"Triggered ES bulk upsert for {len(es_products)} devices of user {user.id}")
            except Exception as e:
                logger.warning(f"Failed to trigger ES sync for devices: {e}")
                errors.append(f"Lỗi khi đồng bộ Elasticsearch: {str(e)}")

        return {
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "errors": errors,
            "total": len(items),
        }

    @staticmethod
    async def sync_components(
        db: AsyncSession,
        user: User,
        fetch_url: str,
    ) -> Dict[str, Any]:
        items = await UserSyncFromUrlService._fetch_items(fetch_url)
        created = updated = skipped = 0
        errors: List[str] = []

        for idx, item in enumerate(items, start=1):
            try:
                product_name = UserSyncFromUrlService._clean_text(item.get("product_name") or item.get("name")) or ""
                if not product_name:
                    skipped += 1
                    continue
                product_code = UserSyncFromUrlService._clean_text(item.get("product_code"))
                amount = UserSyncFromUrlService._to_float(item.get("amount") or item.get("price")) or 0.0
                wholesale_price = UserSyncFromUrlService._to_float(item.get("wholesale_price"))
                trademark = UserSyncFromUrlService._clean_text(item.get("trademark"))
                guarantee = UserSyncFromUrlService._clean_text(item.get("guarantee"))
                stock = UserSyncFromUrlService._to_int(item.get("stock"))
                description = UserSyncFromUrlService._clean_text(item.get("description"))
                product_photo = UserSyncFromUrlService._clean_text(item.get("product_photo"))
                product_link = UserSyncFromUrlService._clean_text(item.get("product_link"))
                category = UserSyncFromUrlService._clean_text(item.get("category"))
                properties = item.get("properties")

                if product_code:
                    existing = await ProductComponentRepository.get_by_product_code_and_user_id(db, product_code, user.id)
                    if existing:
                        update_data = ProductComponentUpdate(
                            product_name=product_name,
                            amount=amount,
                            wholesale_price=wholesale_price,
                            trademark=trademark,
                            guarantee=guarantee,
                            stock=stock,
                            description=description,
                            product_photo=product_photo,
                            product_link=product_link,
                            category=category,
                            properties=properties if isinstance(properties, str) else None,
                        )
                        await ProductComponentRepository.update(db, existing.id, update_data)
                        updated += 1
                        continue

                create_data = ProductComponentCreate(
                    product_code=product_code,
                    product_name=product_name,
                    amount=amount,
                    wholesale_price=wholesale_price,
                    trademark=trademark,
                    guarantee=guarantee,
                    stock=stock,
                    description=description,
                    product_photo=product_photo,
                    product_link=product_link,
                    user_id=user.id,
                    category=category,
                    properties=properties if isinstance(properties, str) else None,
                )
                await ProductComponentRepository.create_without_duplicate_check(db, create_data)
                created += 1
            except Exception as e:
                errors.append(f"Dòng {idx}: {str(e)}")

        # Sync to Elasticsearch via Celery task after database sync
        if created > 0 or updated > 0:
            try:
                # Re-fetch all components for this user to bulk sync to ES
                all_components, _ = await ProductComponentRepository.get_all(db, skip=0, limit=100000, user_id=user.id)
                if all_components:
                    es_accessories = []
                    for comp in all_components:
                        es_accessories.append({
                            "accessory_code": comp.product_code,
                            "accessory_name": comp.product_name,
                            "gia_ban": comp.amount,
                            "gia_si": comp.wholesale_price,
                            "trademark": comp.trademark,
                            "guarantee": comp.guarantee,
                            "stock": comp.stock,
                            "description": comp.description,
                            "product_photo": comp.product_photo,
                            "product_link": comp.product_link,
                            "category": comp.category,
                        })
                    # Trigger Celery task for ES bulk upsert
                    es_bulk_upsert_accessories.delay(
                        customer_id=str(user.id),
                        accessories=es_accessories,
                        with_embedding=True,
                        api_key=user.gemini_api_key,
                    )
                    logger.info(f"Triggered ES bulk upsert for {len(es_accessories)} accessories of user {user.id}")
            except Exception as e:
                logger.warning(f"Failed to trigger ES sync for components: {e}")
                errors.append(f"Lỗi khi đồng bộ Elasticsearch: {str(e)}")

        return {
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "errors": errors,
            "total": len(items),
        }

    @staticmethod
    async def sync_services(
        db: AsyncSession,
        user: User,
        fetch_url: str,
    ) -> Dict[str, Any]:
        items = await UserSyncFromUrlService._fetch_items(fetch_url)
        created = updated = skipped = 0
        errors: List[str] = []

        for idx, item in enumerate(items, start=1):
            try:
                name = (item.get("service_name") or item.get("name") or "").strip()
                if not name:
                    skipped += 1
                    continue
                conditions = item.get("conditions")
                applied_conditions = item.get("applied_conditions")

                existing = await ServiceRepository.get_by_name(db, name, user.id)
                if existing:
                    update_data = ServiceUpdate(
                        name=name,
                        conditions=conditions if isinstance(conditions, list) else None,
                        applied_conditions=applied_conditions if isinstance(applied_conditions, list) else None,
                    )
                    await ServiceRepository.update(db, existing.id, update_data)
                    updated += 1
                    continue

                service = Service(name=name, user_id=user.id, conditions=conditions if isinstance(conditions, list) else None, applied_conditions=applied_conditions if isinstance(applied_conditions, list) else None)
                await ServiceRepository.create(db, service)
                created += 1
            except Exception as e:
                errors.append(f"Dòng {idx}: {str(e)}")

        # Sync to Elasticsearch via Celery task after database sync
        if created > 0 or updated > 0:
            try:
                # Re-fetch all services for this user to bulk sync to ES
                all_services = await ServiceRepository.get_all(db, skip=0, limit=100000, user_id=user.id)
                if all_services:
                    es_services = []
                    for svc in all_services:
                        es_services.append({
                            "ma_dich_vu": str(svc.id),
                            "ten_dich_vu": svc.name,
                            "conditions": svc.conditions,
                            "applied_conditions": svc.applied_conditions,
                        })
                    # Trigger Celery task for ES bulk upsert
                    es_bulk_upsert_services.delay(
                        customer_id=str(user.id),
                        services=es_services,
                        with_embedding=True,
                        api_key=user.gemini_api_key,
                    )
                    logger.info(f"Triggered ES bulk upsert for {len(es_services)} services of user {user.id}")
            except Exception as e:
                logger.warning(f"Failed to trigger ES sync for services: {e}")
                errors.append(f"Lỗi khi đồng bộ Elasticsearch: {str(e)}")

        return {
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "errors": errors,
            "total": len(items),
        }

    @staticmethod
    async def sync_from_url(
        db: AsyncSession,
        user: User,
        updated_today: bool = False,
        background_tasks: Optional[BackgroundTasks] = None,
        type_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        # Normalize requested type (if provided)
        normalized_type = (type_url or "").strip().lower() or None
        # Prefer device record by default if no explicit type provided
        if normalized_type is None:
            record = await UserSyncUrlRepository.get_by_user_id(db, user.id, only_active=True, type_url="device")
            if not record:
                record = await UserSyncUrlRepository.get_by_user_id(db, user.id, only_active=True)
        else:
            record = await UserSyncUrlRepository.get_by_user_id(db, user.id, only_active=True, type_url=normalized_type)
        if not record or not record.url:
            raise ValueError("Chưa cấu hình hoặc đã vô hiệu hoá sync URL cho tài khoản này")

        # Compute fetch URL
        fetch_url = record.url_today if (updated_today and record.url_today) else record.url
        if updated_today and not record.url_today:
            fetch_url = UserSyncFromUrlService._append_query_param(fetch_url, "updated_today", "true")

        # Normalize type: prefer explicit param, then record.type_url, default 'device'
        t = (normalized_type or (record.type_url or "device")).strip().lower()
        if t in {"linhkien", "component", "components"}:
            result = await UserSyncFromUrlService.sync_components(db, user, fetch_url)
            result_type = "component"
            result["type"] = result_type
        elif t in {"dichvu", "service", "services"}:
            result = await UserSyncFromUrlService.sync_services(db, user, fetch_url)
            result_type = "service"
            result["type"] = result_type
        else:
            # default: device
            result = await UserSyncFromUrlService.sync_devices(
                db,
                user,
                updated_today=updated_today,
                background_tasks=background_tasks,
                fetch_url=fetch_url,
                sync_url_id=record.id,
            )
            result_type = "device"
            result["type"] = result_type

        # Log tổng kết cho lần sync này
        try:
            logger.info(
                "Hoàn thành UserSyncFromUrlService.sync_from_url",
                extra={
                    "event.action": "sync.user_sync_url.from_url",
                    "event.category": "sync",
                    "source.layer": "service",
                    "source.function": "sync_from_url",
                    "user.id": str(getattr(user, "id", None)) if user else None,
                    "sync.type": result_type,
                    "sync.created": result.get("created"),
                    "sync.updated": result.get("updated"),
                    "sync.skipped": result.get("skipped"),
                    "sync.errors_count": len(result.get("errors", [])),
                },
            )
        except Exception:
            # Không để logging phụ làm hỏng luồng sync chính
            pass

        return result
