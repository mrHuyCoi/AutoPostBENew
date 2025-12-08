# KẾ HOẠCH TỐI ƯU CELERY - TRÁNH MẤT DỮ LIỆU

> **Ngày tạo**: 26/11/2024  
> **Mục tiêu**: Tối ưu hệ thống với Celery để tránh mất dữ liệu khi service downstream không khả dụng

---

## TỔNG QUAN HIỆN TRẠNG

### ✅ Đã có sẵn:
- **Celery + Redis** đang hoạt động tốt
- **Retry policy** cho `zalo_oa_tasks.py`, `chatbot_tasks.py`
- **Deduplication** với Redis (ví dụ: `dedupe_id` trong Zalo OA)
- **Beat scheduler** cho các task định kỳ

### ❌ Chưa tối ưu:
| File | Vấn đề |
|------|--------|
| `elasticsearch_service.py` | Gọi ES trực tiếp, nếu ES down → mất dữ liệu |
| `document_controller.py` | Gọi HTTP đến ChatbotMobile trực tiếp, không retry |
| `faq_mobile_controller.py` | Gọi HTTP đến ChatbotMobile trực tiếp, không retry |

---

## PHASE 1: TẠO ELASTICSEARCH TASKS

### Task 1.1: Tạo file elasticsearch_tasks.py
**File mới**: `app/tasks/elasticsearch_tasks.py`

**Tasks cần tạo**:

| Task name | Chức năng | Retry |
|-----------|-----------|-------|
| `es_upsert_product` | Upsert 1 product | 5 lần, backoff |
| `es_delete_product` | Delete 1 product | 5 lần, backoff |
| `es_bulk_upsert_products` | Bulk upsert products | 5 lần, backoff |
| `es_bulk_delete_products` | Bulk delete products | 5 lần, backoff |
| `es_delete_all_products` | Delete all products của customer | 5 lần, backoff |
| `es_upsert_service` | Upsert 1 service | 5 lần, backoff |
| `es_delete_service` | Delete 1 service | 5 lần, backoff |
| `es_bulk_upsert_services` | Bulk upsert services | 5 lần, backoff |
| `es_bulk_delete_services` | Bulk delete services | 5 lần, backoff |
| `es_delete_all_services` | Delete all services | 5 lần, backoff |
| `es_upsert_accessory` | Upsert 1 accessory | 5 lần, backoff |
| `es_delete_accessory` | Delete 1 accessory | 5 lần, backoff |
| `es_bulk_upsert_accessories` | Bulk upsert accessories | 5 lần, backoff |
| `es_bulk_delete_accessories` | Bulk delete accessories | 5 lần, backoff |
| `es_delete_all_accessories` | Delete all accessories | 5 lần, backoff |

**Cấu hình retry**:
```python
@celery_app.task(
    name="es_upsert_product",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,           # Exponential backoff
    retry_backoff_max=600,        # Max 10 phút giữa các retry
    retry_kwargs={"max_retries": 5},
    acks_late=True,               # Chỉ ack sau khi task hoàn thành
    reject_on_worker_lost=True,   # Retry nếu worker bị kill
)
```

---

### Task 1.2: Cập nhật celery_app.py
**File sửa**: `app/celery_app.py`

**Thay đổi**:
- Thêm `'app.tasks.elasticsearch_tasks'` vào `include` list

---

### Task 1.3: Refactor elasticsearch_service.py
**File sửa**: `app/services/elasticsearch_service.py`

**Pattern mới cho mỗi method**:

| Method | Thay đổi |
|--------|----------|
| `upsert_product()` | Thêm param `async_mode=True`. Nếu `True` → gọi Celery task |
| `delete_product()` | Tương tự |
| `bulk_upsert_products()` | Tương tự |
| `bulk_delete_products()` | Tương tự |
| `delete_all_products()` | Tương tự |
| Tất cả methods cho services | Tương tự |
| Tất cả methods cho accessories | Tương tự |

**Logic mới**:
```python
async def upsert_product(
    customer_id: str,
    product_data: Dict[str, Any],
    with_embedding: bool = True,
    api_key: Optional[str] = None,
    async_mode: bool = True,  # <-- THÊM MỚI
) -> Dict[str, Any]:
    if async_mode:
        # Gửi vào Celery queue, return task_id
        from app.tasks.elasticsearch_tasks import es_upsert_product
        task = es_upsert_product.delay(
            customer_id=customer_id,
            product_data=product_data,
            with_embedding=with_embedding,
            api_key=api_key,
        )
        return {"status": "queued", "task_id": task.id}
    else:
        # Gọi trực tiếp (dùng cho Celery worker)
        return await _do_upsert_product(...)
```

**Thêm internal methods**:
- `_do_upsert_product()` - Logic ES thực sự (move code hiện tại vào đây)
- `_do_delete_product()` 
- `_do_bulk_upsert_products()`
- Tương tự cho services và accessories

---

## PHASE 2: TẠO DOCUMENT TASKS

### Task 2.1: Tạo file document_tasks.py
**File mới**: `app/tasks/document_tasks.py`

**Tasks cần tạo**:

| Task name | Chức năng | Retry |
|-----------|-----------|-------|
| `doc_upload_text` | Upload text document | 3 lần |
| `doc_upload_file` | Upload file document | 3 lần |
| `doc_upload_url` | Upload URL document | 3 lần |
| `doc_delete_all` | Delete all documents | 3 lần |
| `doc_delete_by_source` | Delete by source | 3 lần |
| `doc_trigger_reindex` | Trigger GraphRAG reindex | 3 lần |

**Lưu ý với file upload**:
- Task chỉ nhận `file_path` hoặc `file_url` (đã upload lên storage trước)
- Không serialize file content vào Redis message

---

### Task 2.2: Cập nhật celery_app.py
**File sửa**: `app/celery_app.py`

**Thay đổi**:
- Thêm `'app.tasks.document_tasks'` vào `include` list

---

### Task 2.3: Refactor document_controller.py
**File sửa**: `app/controllers/document_controller.py`

**Endpoints cần sửa**:

| Endpoint | Thay đổi |
|----------|----------|
| `POST /documents/upload-text` | Gọi `doc_upload_text.delay()` thay vì HTTP trực tiếp |
| `POST /documents/upload-file` | Upload file lên storage trước → Gọi `doc_upload_file.delay()` với file URL |
| `POST /documents/upload-url` | Gọi `doc_upload_url.delay()` |
| `DELETE /documents/delete-all` | Gọi `doc_delete_all.delay()` |
| `DELETE /documents/delete-by-source` | Gọi `doc_delete_by_source.delay()` |

**Response mới**:
```python
return {
    "status": "queued",
    "task_id": task.id,
    "message": "Tác vụ đã được đưa vào hàng đợi xử lý"
}
```

**Giữ nguyên** (read operations):
- `GET /documents/list`
- `GET /documents-original/list`
- `GET /documents/sources`

---

## PHASE 3: TẠO FAQ TASKS

### Task 3.1: Tạo file faq_tasks.py
**File mới**: `app/tasks/faq_tasks.py`

**Tasks cần tạo**:

| Task name | Chức năng | Retry |
|-----------|-----------|-------|
| `faq_create` | Tạo FAQ mới | 3 lần |
| `faq_update` | Cập nhật FAQ | 3 lần |
| `faq_delete` | Xóa 1 FAQ | 3 lần |
| `faq_delete_all` | Xóa tất cả FAQs | 3 lần |
| `faq_import` | Import từ file | 3 lần |

---

### Task 3.2: Cập nhật celery_app.py
**File sửa**: `app/celery_app.py`

**Thay đổi**:
- Thêm `'app.tasks.faq_tasks'` vào `include` list

---

### Task 3.3: Refactor faq_mobile_controller.py
**File sửa**: `app/controllers/faq_mobile_controller.py`

**Endpoints cần sửa**:

| Endpoint | Thay đổi |
|----------|----------|
| `POST /mobile-faq` | Upload files trước (nếu có) → Gọi `faq_create.delay()` |
| `PUT /mobile-faq/{faq_id}` | Upload files trước (nếu có) → Gọi `faq_update.delay()` |
| `DELETE /mobile-faq/{faq_id}` | Gọi `faq_delete.delay()` |
| `DELETE /mobile-faqs` | Gọi `faq_delete_all.delay()` |
| `POST /mobile-faq/import` | Parse file → Gọi `faq_import.delay()` |

**Giữ nguyên**:
- `GET /mobile-faqs`
- `GET /mobile-faq/export`

---

## PHASE 4: DATABASE CHO TASK TRACKING

### Task 4.1: Tạo model TaskLog
**File mới**: `app/models/task_log.py`

**Fields**:
| Field | Type | Mô tả |
|-------|------|-------|
| `id` | Integer PK | Auto increment |
| `task_id` | String unique | Celery task ID |
| `task_name` | String | Tên task (es_upsert_product, etc.) |
| `user_id` | Integer FK | User thực hiện |
| `payload` | JSON | Dữ liệu đầu vào |
| `status` | Enum | pending, processing, success, failed, retrying |
| `result` | JSON nullable | Kết quả trả về |
| `error_message` | Text nullable | Lỗi nếu có |
| `retry_count` | Integer default 0 | Số lần retry |
| `created_at` | DateTime | Thời điểm tạo |
| `updated_at` | DateTime | Thời điểm cập nhật |

---

### Task 4.2: Tạo migration
**Chạy**: `alembic revision -m "add_task_log_table"`

---

### Task 4.3: Tạo repository
**File mới**: `app/repositories/task_log_repository.py`

**Methods**:
- `create(task_id, task_name, user_id, payload)`
- `update_status(task_id, status, result=None, error=None)`
- `increment_retry(task_id)`
- `get_by_task_id(task_id)`
- `get_failed_tasks(limit=100)`
- `get_user_tasks(user_id, limit=50)`

---

### Task 4.4: Cập nhật tasks để log status
**Files sửa**: 
- `app/tasks/elasticsearch_tasks.py`
- `app/tasks/document_tasks.py`
- `app/tasks/faq_tasks.py`

**Thêm vào mỗi task**:
```python
@celery_app.task(...)
def es_upsert_product(self, ...):
    task_id = self.request.id
    
    # Update status: processing
    TaskLogRepository.update_status(task_id, "processing")
    
    try:
        result = loop.run_until_complete(_do_upsert_product(...))
        # Update status: success
        TaskLogRepository.update_status(task_id, "success", result=result)
        return result
    except Exception as e:
        # Update status: retrying hoặc failed
        if self.request.retries < self.max_retries:
            TaskLogRepository.update_status(task_id, "retrying", error=str(e))
            TaskLogRepository.increment_retry(task_id)
        else:
            TaskLogRepository.update_status(task_id, "failed", error=str(e))
        raise
```

---

## PHASE 5: TASK STATUS API

### Task 5.1: Tạo task_status_controller.py
**File mới**: `app/controllers/task_status_controller.py`

**Endpoints**:

| Endpoint | Chức năng |
|----------|-----------|
| `GET /api/v1/tasks/{task_id}/status` | Lấy status của 1 task |
| `GET /api/v1/tasks/my-tasks` | Lấy danh sách tasks của user hiện tại |
| `POST /api/v1/tasks/{task_id}/retry` | Manual retry task failed |

---

### Task 5.2: Cập nhật main.py
**File sửa**: `main.py`

**Thay đổi**:
- Import và register `task_status_controller.router`

---

## PHASE 6: CẤU HÌNH CELERY NÂNG CAO

### Task 6.1: Cập nhật celery_app.py
**File sửa**: `app/celery_app.py`

**Thêm cấu hình**:
```python
celery_app.conf.update(
    # ... existing config ...
    
    # Task result expiration (7 ngày)
    result_expires=604800,
    
    # Task ack late (chỉ ack sau khi hoàn thành)
    task_acks_late=True,
    
    # Reject task nếu worker bị kill
    task_reject_on_worker_lost=True,
    
    # Prefetch multiplier (số task prefetch)
    worker_prefetch_multiplier=1,
    
    # Task time limit per task type
    task_annotations={
        'es_upsert_product': {'rate_limit': '10/s'},
        'es_bulk_upsert_products': {'time_limit': 300},
        'doc_upload_file': {'time_limit': 120},
    },
)
```

---

### Task 6.2: Thêm Dead Letter Queue
**File sửa**: `app/celery_app.py`

**Thêm error handler**:
```python
from celery.signals import task_failure

@task_failure.connect
def handle_task_failure(sender=None, task_id=None, exception=None, **kwargs):
    # Log to database or alert
    # Có thể gửi notification (email, Slack) khi task fail sau max_retries
    pass
```

---

## PHASE 7: MONITORING

### Task 7.1: Cài đặt Flower
**File sửa**: `requirements.txt`

**Thêm**:
```
flower>=2.0.0
```

---

### Task 7.2: Cập nhật docker-compose.yml
**File sửa**: `docker-compose.yml`

**Thêm service**:
```yaml
flower:
  build: .
  command: celery -A celery_worker.celery_app flower --port=5555
  ports:
    - "5555:5555"
  depends_on:
    - redis
```

---

### Task 7.3: Health check endpoint
**File sửa**: `main.py` hoặc tạo `app/controllers/health_controller.py`

**Endpoint mới**:
- `GET /health/celery`: Kiểm tra Celery connection
  - Ping Redis broker
  - Đếm số workers active
  - Đếm số tasks pending

---

## TIMELINE DỰ KIẾN

| Phase | Nội dung | Thời gian |
|-------|----------|-----------|
| Phase 1 | Elasticsearch Tasks | 2 ngày |
| Phase 2 | Document Tasks | 1 ngày |
| Phase 3 | FAQ Tasks | 1 ngày |
| Phase 4 | Task Tracking DB | 1 ngày |
| Phase 5 | Task Status API | 0.5 ngày |
| Phase 6 | Celery Config | 0.5 ngày |
| Phase 7 | Monitoring | 0.5 ngày |

**Tổng: ~6.5 ngày làm việc** (so với ~14 ngày nếu dùng Kafka)

---

## CHECKLIST FILES

### Files mới:
- [ ] `app/tasks/elasticsearch_tasks.py`
- [ ] `app/tasks/document_tasks.py`
- [ ] `app/tasks/faq_tasks.py`
- [ ] `app/models/task_log.py`
- [ ] `app/repositories/task_log_repository.py`
- [ ] `app/controllers/task_status_controller.py`

### Files cần sửa:
- [ ] `app/celery_app.py` - Thêm includes, cấu hình nâng cao
- [ ] `app/services/elasticsearch_service.py` - Thêm async_mode, tách internal methods
- [ ] `app/controllers/document_controller.py` - Gọi Celery tasks
- [ ] `app/controllers/faq_mobile_controller.py` - Gọi Celery tasks
- [ ] `main.py` - Register task_status_controller
- [ ] `requirements.txt` - Thêm flower
- [ ] `docker-compose.yml` - Thêm flower service

---

## LƯU Ý QUAN TRỌNG

### 1. Backward Compatibility
- Giữ param `async_mode=True` (default) để không break code hiện tại
- Có thể set `async_mode=False` khi cần kết quả ngay

### 2. File Upload Handling
- **KHÔNG** serialize file content vào Redis message
- Upload file lên storage (S3/local) trước, truyền URL vào task

### 3. Idempotency
- Sử dụng `task_id` làm idempotency key
- Check duplicate trước khi thực hiện (giống pattern trong `zalo_oa_tasks.py`)

### 4. Rate Limiting
- Giới hạn số request đến ES/ChatbotMobile để tránh overwhelm
- Cấu hình `rate_limit` cho từng task

### 5. Retry Strategy
- **ES tasks**: 5 retries (ES có thể down lâu)
- **HTTP tasks**: 3 retries (service restart nhanh hơn)
- Exponential backoff: 1s → 2s → 4s → 8s → ...

### 6. Monitoring
- Dùng Flower để monitor tasks real-time
- Alert khi có task fail sau max_retries
- Dashboard cho task success/failure rate

---

## SO SÁNH VỚI KAFKA

| Tiêu chí | Celery | Kafka |
|----------|--------|-------|
| **Complexity** | Thấp (đã có sẵn) | Cao (cần setup mới) |
| **Learning curve** | Không có (đã dùng) | Cao |
| **Infra** | Redis (đã có) | Zookeeper + Kafka |
| **RAM requirement** | ~100MB | ~2-3GB |
| **Time to implement** | ~6.5 ngày | ~14 ngày |
| **Retry mechanism** | ✅ Built-in | ❌ Phải tự implement |
| **Task tracking** | ✅ Flower + Result backend | ❌ Phải tự implement |
| **Fit cho project** | ✅ Perfect | ⚠️ Overkill |
