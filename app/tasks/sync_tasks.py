import asyncio
import traceback
from sqlalchemy import select
from celery.utils.log import get_task_logger

from app.celery_app import celery_app
from app.database.database import async_session
from app.models.user import User
from app.services.api_data_service import ApiDataService
from app.services.product_component_service import ProductComponentService
from app.models.user_sync_url import UserSyncUrl
from app.services.user_sync_from_url_service import UserSyncFromUrlService
from app.tasks.elasticsearch_tasks import es_bulk_upsert_accessories

logger = get_task_logger(__name__)

def _get_loop() -> asyncio.AbstractEventLoop:
    """Luôn dùng cùng event-loop mặc định của worker."""
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop

@celery_app.task(name="scheduled_sync_from_api_and_export")
def scheduled_sync_from_api_and_export():
    """
    Celery task: Đồng bộ dữ liệu từ API, xuất Excel và đồng bộ với chatbots cho tất cả người dùng.
    """
    loop = _get_loop()
    try:
        return loop.run_until_complete(_scheduled_sync_from_api_and_export())
    except Exception as e:
        logger.error(
            f"Error in scheduled_sync_from_api_and_export: {e}",
            exc_info=True,
            extra={
                "event.action": "task.sync.api_data_and_export",
                "event.category": "task",
                "source.layer": "task",
                "source.function": "scheduled_sync_from_api_and_export",
            },
        )
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

async def _scheduled_sync_from_api_and_export():
    """
    Hàm async thực hiện logic đồng bộ cho tất cả người dùng.
    """
    logger.info(
        "Starting scheduled sync for all users.",
        extra={
            "event.action": "task.sync.api_data_and_export.run",
            "event.category": "task",
            "source.layer": "task",
            "source.function": "_scheduled_sync_from_api_and_export",
        },
    )
    successful_users = []
    failed_users = []

    async with async_session() as db:
        # Lấy người dùng cụ thể để đồng bộ
        user_id_to_sync = "c4cd8f65-5591-4977-aaea-87e5cc547325"
        stmt = select(User).where(User.id == user_id_to_sync, User.is_active == True)
        user = (await db.execute(stmt)).scalars().first()
        
        users = [user] if user else []

        logger.info(f"Found {len(users)} specific user(s) to sync.")

        for user in users:
            try:
                logger.info(
                    f"Processing sync for user {user.email} (ID: {user.id})",
                    extra={
                        "event.action": "task.sync.api_data_and_export.user",
                        "event.category": "task",
                        "source.layer": "task",
                        "source.function": "_scheduled_sync_from_api_and_export",
                        "user.id": str(user.id),
                        "user.email": user.email,
                    },
                )

                # 1. Đồng bộ dữ liệu từ API, không trigger sync lẻ
                # BackgroundTasks không hoạt động trong Celery task, truyền None hoặc đối tượng giả
                await ApiDataService.sync_product_components_from_api(
                    db=db,
                    user_id=user.id,
                    background_tasks=None, # Không dùng background_tasks ở đây
                    current_user=user,
                    is_today=False,
                    sync_individually=False
                )
                logger.info(f"API data synced for user {user.email}")

                # 2. Lấy tất cả linh kiện sau khi đã đồng bộ
                all_components_response = await ProductComponentService.get_all_product_components(db, user_id=user.id, limit=100000)
                
                if all_components_response and all_components_response['data']:
                    components_data = all_components_response['data']
                    logger.info(f"Found {len(components_data)} components to sync for user {user.email}")
                    
                    # 3. Đồng bộ với Elasticsearch via Celery task
                    try:
                        es_accessories = []
                        for comp in components_data:
                            es_accessories.append({
                                "accessory_code": comp.get("product_code") if isinstance(comp, dict) else getattr(comp, "product_code", None),
                                "accessory_name": comp.get("product_name") if isinstance(comp, dict) else getattr(comp, "product_name", None),
                                "gia_ban": comp.get("amount") if isinstance(comp, dict) else getattr(comp, "amount", None),
                                "gia_si": comp.get("wholesale_price") if isinstance(comp, dict) else getattr(comp, "wholesale_price", None),
                                "trademark": comp.get("trademark") if isinstance(comp, dict) else getattr(comp, "trademark", None),
                                "guarantee": comp.get("guarantee") if isinstance(comp, dict) else getattr(comp, "guarantee", None),
                                "stock": comp.get("stock") if isinstance(comp, dict) else getattr(comp, "stock", None),
                                "description": comp.get("description") if isinstance(comp, dict) else getattr(comp, "description", None),
                                "product_photo": comp.get("product_photo") if isinstance(comp, dict) else getattr(comp, "product_photo", None),
                                "product_link": comp.get("product_link") if isinstance(comp, dict) else getattr(comp, "product_link", None),
                                "category": comp.get("category") if isinstance(comp, dict) else getattr(comp, "category", None),
                            })
                        if es_accessories:
                            es_bulk_upsert_accessories.delay(
                                customer_id=str(user.id),
                                accessories=es_accessories,
                                with_embedding=True,
                                api_key=getattr(user, "gemini_api_key", None),
                            )
                            logger.info(f"Triggered ES bulk upsert for {len(es_accessories)} accessories of user {user.email}")
                    except Exception as es_err:
                        logger.warning(f"Failed to trigger ES sync for user {user.email}: {es_err}")
                else:
                    logger.info(f"No components to sync for user {user.email}")
                
                successful_users.append(user.email)

            except Exception as e:
                logger.error(
                    f"Failed to sync for user {user.email}: {e}",
                    exc_info=True,
                    extra={
                        "event.action": "task.sync.api_data_and_export.user_error",
                        "event.category": "task",
                        "source.layer": "task",
                        "source.function": "_scheduled_sync_from_api_and_export",
                        "user.id": str(user.id),
                        "user.email": user.email,
                    },
                )
                failed_users.append({"email": user.email, "error": str(e)})
                # Không re-raise để task tiếp tục với user khác
    
    result = {
        "status": "completed",
        "successful_users": successful_users,
        "failed_users": failed_users,
        "summary": f"Sync completed. Success: {len(successful_users)}, Failed: {len(failed_users)}."
    }
    logger.info(
        result["summary"],
        extra={
            "event.action": "task.sync.api_data_and_export.summary",
            "event.category": "task",
            "source.layer": "task",
            "source.function": "_scheduled_sync_from_api_and_export",
            "sync.success_count": len(successful_users),
            "sync.failed_count": len(failed_users),
        },
    )
    return result


# ===================== New: Sync all users having active URL daily =====================
@celery_app.task(name="sync_user_urls_daily")
def sync_user_urls_daily():
    """
    Celery task: Mỗi ngày lúc 03:00, kiểm tra bảng user_sync_urls và đồng bộ cho từng user có URL đang active.
    Sử dụng updated_today=True để ưu tiên URL theo ngày (url_today) nếu có.
    """
    loop = _get_loop()
    try:
        return loop.run_until_complete(_sync_user_urls_daily())
    except Exception as e:
        logger.error(
            f"Error in sync_user_urls_daily: {e}",
            exc_info=True,
            extra={
                "event.action": "task.sync.user_urls_daily",
                "event.category": "task",
                "source.layer": "task",
                "source.function": "sync_user_urls_daily",
            },
        )
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


async def _sync_user_urls_daily():
    """
    Hàm async thực hiện đồng bộ cho tất cả user có cấu hình URL đang active.
    """
    logger.info(
        "Starting daily URL sync for all users with active user_sync_urls",
        extra={
            "event.action": "task.sync.user_urls_daily.run",
            "event.category": "task",
            "source.layer": "task",
            "source.function": "_sync_user_urls_daily",
        },
    )
    successful_users = []
    failed_users = []

    async with async_session() as db:
        # Lấy danh sách user có user_sync_urls active và có url
        stmt = (
            select(User)
            .join(UserSyncUrl, UserSyncUrl.user_id == User.id)
            .where(User.is_active == True, UserSyncUrl.is_active == True, UserSyncUrl.url.isnot(None))
        )
        users = (await db.execute(stmt)).scalars().all()
        logger.info(
            f"Found {len(users)} users with active sync URLs",
            extra={
                "event.action": "task.sync.user_urls_daily.prepare",
                "event.category": "task",
                "source.layer": "task",
                "source.function": "_sync_user_urls_daily",
                "sync.user_count": len(users),
            },
        )

        for user in users:
            try:
                logger.info(
                    f"Syncing from URL for user {user.email} ({user.id})",
                    extra={
                        "event.action": "task.sync.user_urls_daily.user",
                        "event.category": "task",
                        "source.layer": "task",
                        "source.function": "_sync_user_urls_daily",
                        "user.id": str(user.id),
                        "user.email": user.email,
                    },
                )
                # updated_today=True -> sử dụng url_today nếu có, hoặc thêm tham số updated_today=true
                await UserSyncFromUrlService.sync_from_url(db, user, updated_today=True, background_tasks=None)
                successful_users.append(user.email)
            except Exception as e:
                logger.error(
                    f"Failed URL sync for user {user.email}: {e}",
                    exc_info=True,
                    extra={
                        "event.action": "task.sync.user_urls_daily.user_error",
                        "event.category": "task",
                        "source.layer": "task",
                        "source.function": "_sync_user_urls_daily",
                        "user.id": str(user.id),
                        "user.email": user.email,
                    },
                )
                failed_users.append({"email": user.email, "error": str(e)})

    result = {
        "status": "completed",
        "successful_users": successful_users,
        "failed_users": failed_users,
        "summary": f"Daily URL sync completed. Success: {len(successful_users)}, Failed: {len(failed_users)}."
    }
    logger.info(result["summary"])
    return result
