# Kế hoạch logging & context cho `dangbaitudong`

## 1. Mục tiêu & phạm vi

- **Mục tiêu**
  - Chuẩn hoá cách gắn context (correlation id, trace) cho log của service `dangbaitudong`.
  - Cho phép **truy vết từng request** từ controller → service → repository → service khác, dùng cùng một `trace.id`.
  - Chỉ log chi tiết ở mức **lỗi** cho từng hàm (controller/service/repository), hạn chế log normal flow.

- **Phạm vi giai đoạn đầu**
  - Áp dụng cho service `dangbaitudong` (FastAPI).
  - Tập trung vào **kiến trúc & cấu trúc log**, chưa yêu cầu implement chi tiết cho tất cả controller.
  - Làm mẫu truy vết theo **từng controller** lần lượt.

---

## 2. Quản lý context với `asgi-correlation-id`

### 2.1. Nguyên tắc chung

- Dùng thư viện `asgi-correlation-id` để:
  - Sinh / đọc `correlation id` cho mỗi request HTTP.
  - Lưu `correlation id` vào **contextvar** toàn cục.
  - Tự động gắn `correlation id` vào mọi log được ghi trong request đó.

- Lớp context cơ bản:
  - **`trace.id`**: id duy nhất của request xuyên suốt toàn bộ call chain.
  - **`span.id`** (tùy chọn): id cho từng đoạn xử lý (controller / service / repository). Có thể mở rộng sau.

### 2.2. Tích hợp hạ tầng (tổng quát, không ghi chi tiết code)

- **Middleware** (trong `main.py`):
  - Thêm `CorrelationIdMiddleware` ở mức app.
  - Đọc / ghi header chuẩn (ví dụ: `X-Request-ID` hoặc `X-Correlation-ID`).

- **Logger config chung**:
  - Cấu hình logger Python để:
    - Ghi log ra file (chuẩn bị cho Filebeat).
    - Sử dụng formatter JSON, trong đó luôn có:
      - `@timestamp`
      - `log.level`
      - `service.name` (ví dụ: `"dangbaitudong-api"`)
      - `trace.id` (lấy từ `asgi-correlation-id`)
      - `span.id` (nếu có)

---

## 3. Cấu trúc truy vết: controller → service → repository → service khác

### 3.1. Tầng controller

- Mỗi **controller** (file trong `app/controllers`) được xem là **entry point nghiệp vụ**.
- Mỗi **hàm endpoint** trong controller:
  - Được bọc bởi một cơ chế chung (decorator / helper) để:
    - Gắn metadata định vị: `source.controller`, `source.function`, `event.action`, `event.category`.
    - Khi xảy ra lỗi: ghi log ERROR với đầy đủ context.
  - Không bắt buộc log khi normal flow, trừ các điểm nghiệp vụ đặc biệt.

### 3.2. Tầng service

- Các lớp/hàm service (xử lý nghiệp vụ) phải tuân theo:
  - Không tự tạo `trace.id` mới, **chỉ sử dụng lại** context có sẵn.
  - Khi log lỗi trong service:
    - Dùng cùng `trace.id`.
    - Bổ sung `source.layer = "service"`.
    - Ghi rõ `source.function` (tên hàm service).
    - Gắn các key nghiệp vụ chính (vd: `user.id`, `order.id`, `subscription.id`, `provider`, ...).

### 3.3. Tầng repository

- Repository (làm việc với DB):
  - Khi lỗi DB / query, log ở mức ERROR với:
    - `source.layer = "repository"`.
    - `source.function` là hàm repository.
    - `db.table`, `db.operation` (vd: `SELECT`, `INSERT`).
  - Vẫn dùng chung `trace.id` để có thể nhìn toàn bộ flow.

### 3.4. Gọi sang service khác (nội bộ / external)

- Khi từ `dangbaitudong` gọi sang:
  - **Chatbot API**, **Zalo API**, hoặc service nội bộ khác:
    - Truyền `trace.id` qua header (ví dụ `X-Request-ID`).
    - Ở service nhận, cũng dùng `asgi-correlation-id` (giai đoạn sau) để tiếp tục trace chain.
  - Log tích hợp (integration layer) có thể dùng:
    - `source.layer = "integration"`.
    - `integration.target` (vd: `"chatbot-service"`, `"zaloapi"`).

---

## 4. Chính sách log theo từng hàm

- **Mức log mặc định** cho controller/service/repository:
  - Chỉ bắt buộc ghi log ở **ERROR** (hoặc cao hơn) khi có ngoại lệ.
  - NORMAL flow không bắt buộc log, tránh spam.

- **Khi có lỗi**:
  - Ghi **01 bản ghi log** đầy đủ context cho mỗi lỗi ở vị trí phù hợp nhất (thường là tầng gần business hơn).
  - Có thể re-raise exception để middleware xử lý HTTP response, nhưng **không log trùng nhiều lần** cho cùng một lỗi.

- **Mã lỗi tra cứu (error reference)**:
  - Mỗi lần log lỗi cần có thêm một trường mã lỗi tra cứu, ví dụ `error.ref_id` (chuỗi ngắn, dễ đọc, dễ copy, vd: `"ABCDE"`, `"ERR-5F3A2"`).
  - Mã này được đưa vào **cả response JSON trả về cho client** lẫn bản ghi log trong Elasticsearch.
  - Khi người dùng gửi mã lỗi cho đội hỗ trợ, chỉ cần search theo `error.ref_id` là tìm được **toàn bộ log chi tiết** liên quan đến lần lỗi đó.

- **Truy vết theo từng controller**:
  - Kế hoạch rollout:
    - Chọn một controller (vd: `order_controller`) ⇒ chuẩn hóa luồng log từ:
      - `OrderController` → `OrderService` → `OrderRepository` → các service tích hợp.
    - Sau khi ổn định, chuyển sang controller tiếp theo (vd: `chatbot_controller`, `zalo_controller`, ...).
  - Mỗi controller sẽ có một **sơ đồ call chain** riêng để đảm bảo truy vết được.

---

## 5. Mẫu cấu trúc log (điều chỉnh cho dự án)

> Đây là **mẫu khung** cho một bản ghi log (theo JSON), chia thành 4 nhóm. Một số field sẽ được hạ tầng tự fill, một số field dev phải truyền vào.

### 5.1. Nhóm 1 – Hạ tầng (tự động / cấu hình logger)

- `@timestamp`: thời gian chuẩn ISO8601.
- `log.level`: `ERROR`, `WARNING`, (tuỳ lúc dùng).
- `service.name`: ví dụ `"dangbaitudong-api"`.
- `service.environment`: `dev` / `staging` / `prod`.
- `trace.id`: lấy từ `asgi-correlation-id`.
- `span.id`: optional, dùng khi cần tách nhỏ từng đoạn.

### 5.2. Nhóm 2 – Định vị code (dev phải gắn)

- `event.action`: tên hành động nghiệp vụ, ví dụ:
  - `"order.create"`, `"chatbot.generate_reply"`, `"zalo.oa.sync"`, ...
- `event.category`: nhóm lớn, ví dụ:
  - `"order"`, `"chatbot"`, `"zalo"`, `"auth"`, `"admin"`.
- `source.controller`: tên controller, ví dụ: `"order_controller"`.
- `source.function`: tên hàm cụ thể, ví dụ:
  - `"create_order"` (controller),
  - `"create_order_service"` (service),
  - `"insert_order"` (repository).
- `source.layer`: `"controller"` / `"service"` / `"repository"` / `"integration"`.

### 5.3. Nhóm 3 – Ngữ cảnh nghiệp vụ (dev phải gắn)

- Thông tin user / subscription:
  - `user.id`
  - `user.email` (nếu cần).
  - `subscription.id`
  - `subscription.plan`
- Thông tin đối tượng chính theo từng domain:
  - Order: `order.id`, `order.amount`, `order.status`.
  - Chatbot: `chatbot.session_id`, `chatbot.channel` (web/zalo/facebook), `chatbot.bot_id`.
  - Zalo: `zalo.oa_id`, `zalo.user_id`, `zalo.conversation_id`.
  - Task / Celery: `task.id`, `task.name`.
- Tham số đầu vào quan trọng (đã mask dữ liệu nhạy cảm nếu có):
  - `request.method`, `request.path`.
  - `request.query` (đã rút gọn nếu dài).

### 5.4. Nhóm 4 – Kết quả / lỗi

- `message`: mô tả dễ đọc cho con người.
- `error.type`: `"AppException"`, `"BadRequestException"`, `"ExternalAPIException"`, `"Exception"`, ...
- `error.code`: mã lỗi nội bộ (lấy từ `AppException.error_code` nếu có).
- `error.ref_id`: **mã lỗi hiển thị cho người dùng** (ngắn, duy nhất cho từng lần lỗi), dùng để tra cứu log.
- `error.stack_trace`: chuỗi stack trace (có thể rút gọn).
- `http.status_code`: mã HTTP cuối cùng trả về.
- `process.duration_ms`: thời gian xử lý đoạn code/hàm đó.

---

## 6. Định hướng cho các service khác

- Khi triển khai cho các service khác (vd: `ChatbotMobileStore`, `zaloapi`, ...):
  - **Dùng cùng chuẩn field** như trên (`trace.id`, `service.name`, `event.action`, ...).
  - Cùng header truyền `trace.id` giữa các service.
  - Mỗi service chỉ cần:
    - Thêm `asgi-correlation-id` (hoặc cơ chế tương đương nếu không phải ASGI).
    - Dùng chung module cấu hình logger (hoặc ít nhất là chung schema JSON).

- Mục tiêu cuối:
  - Từ một `trace.id`, có thể mở Kibana/Elasticsearch để xem toàn bộ:
    - Request vào `dangbaitudong`.
    - Gọi sang `ChatbotMobileStore`, `zaloapi`, ...
    - Log ở tất cả layer (controller/service/repository/integration) được xâu chuỗi rõ ràng.

---

## 7. Kế hoạch task & thứ tự controller

- **Task 1 – Hạ tầng log & context (làm trước)**
  - Thêm `CorrelationIdMiddleware` (`asgi-correlation-id`) vào `main.py`.
  - Cấu hình logger JSON chung (ghi file log, gồm: `@timestamp`, `log.level`, `service.*`, `trace.id`, `span.id`).
  - Chuẩn hoá schema log theo 4 nhóm ở mục 5, bao gồm cả `error.ref_id`.

- **Task 2 – Middleware & response lỗi**
  - Điều chỉnh `ErrorHandlerMiddleware` để:
    - Luôn ghi log lỗi với đầy đủ context, kèm `error.code` và `error.ref_id`.
    - Trả về response JSON cho client có chứa `error.code`, `error.ref_id`, `message`.
  - Đảm bảo mọi `AppException` và lỗi không xác định đều đi qua cùng một luồng xử lý.

- **Task 3 – Decorator / helper cho controller & service**
  - Tạo decorator chung để bọc các endpoint controller, tự động:
    - Gắn metadata định vị (`source.controller`, `source.function`, `event.action`, `event.category`).
    - Khi có exception: tạo `error.ref_id`, ghi log ERROR một lần, sau đó re-raise.
  - Định nghĩa quy ước đặt `event.action`, `event.category` cho từng domain (order, chatbot, zalo, auth, ...).

 - **Task 4 – Rollout theo từng controller (ưu tiên)**
 - **Đợt 1 – Controller liên quan đến đơn hàng & dòng tiền**:
   - `order_controller`, `order_chatcustom_controller`.
 - **Đợt 2 – Controller chatbot & tương tác người dùng**:
   - `chatbot_controller`, `chatbot_subscription_controller`, `websocket_controller`.
 - **Đợt 3 – Controller Zalo & webhook**:
   - `zalo_controller`, `zalo_oa_webhook_controller`, `zalo_oa_controller`, `staffzalo_controller`, `zalo_ignored_controller`, `zalo_bot_config_controller`.
 - **Đợt 4 – Các controller còn lại** (CRUD sản phẩm, cấu hình, admin, ...):
   - `user_controller`, `subscription_controller`, `admin_controller`, `user_config_controller`, `brand_controller`, `service_controller`, `device_*_controller`, `product_component_controller`, `category_controller`, `property_controller`, `warranty_service_controller`, `faq_mobile_controller`, `document_controller`, `file_upload_controller`, `youtube_controller`, `facebook_controller`, `instagram_controller`, `scheduled_video_controller`, `task_controller`, `user_sync_url_controller`, `chatbot_js_settings_controller`, `user_device_from_url_router`, ...

- **Task 5 – Áp dụng cho các service khác**
  - Chuẩn hoá logger và context tương tự cho `ChatbotMobileStore`, `zaloapi`, ...
  - Đảm bảo tất cả service đều hiểu và duy trì `trace.id`, `error.ref_id` để hỗ trợ tra cứu chéo.

## 8. Kế hoạch logging chi tiết cho service `ChatbotMobileStore`

### 8.1. Nguyên tắc chung cho `ChatbotMobileStore`

- **service.name** đề xuất: `"chatbotmobilestore-api"`.
- Sử dụng **cùng schema log 4 nhóm field** như mục 5 (`trace.id`, `service.*`, `event.*`, `source.*`, `error.*`, ...).
- Khi request đến từ `dangbaitudong`:
  - Đọc `trace.id` từ header chuẩn (vd: `X-Request-ID` / `X-Correlation-ID`) nếu có.
  - Gắn vào context để mọi log trong cùng request (controller → service nội bộ) đều dùng chung `trace.id`.
- Ở từng controller (route module) của ChatbotMobileStore:
  - Gắn `source.layer = "controller"`.
  - `source.controller` = tên file route (vd: `"chat_routes"`, `"document_routes"`, ...).
  - `source.function` = tên hàm endpoint.
  - `event.category`, `event.action` chuẩn hoá theo domain (chatbot, document, faq, order, config, ...).

### 8.2. Mapping luồng tích hợp từ `dangbaitudong` sang `ChatbotMobileStore`

> Chỉ liệt kê các luồng có trong các controller/service/task bạn đang dùng:  
> `chatbot_controller`, `chatbot_js_settings_controller`, `document_controller`, `faq_mobile_controller`, `order_chatcustom_controller`, `order_controller`, `user_config_controller`, `zalo_oa_webhook_controller`, `chatbot_service`, `user_sync_from_url_service`, `zalo_oa_tasks`.

#### 8.2.1. Luồng Chatbot & lịch sử chat

- **Nguồn `dangbaitudong`**
  - `chatbot_controller.chat_endpoint` → `ChatbotService.chat_with_bot` / `stream_chat_with_bot` → `POST {CHATBOT_API_BASE_URL}/chat/{thread_id}`.
  - `chatbot_controller.get_chat_history_endpoint` → `ChatbotService.get_chat_history` → `GET /chat-history/{customer_id}/{thread_id}`.
  - `chatbot_controller.clear_history_chat` → `ChatbotService.clear_history_chat` → `POST /chat-history-clear/{customer_id}`.
- **Đích `ChatbotMobileStore` (controller cần log)**
  - **`chat_routes`**:
    - `POST /chat/{threadId}` → xử lý truy vấn chat chính.
    - `GET /chat-history/{customer_id}/{thread_id}` → trả lịch sử chat.
    - `POST /chat-history-clear/{customer_id}` → xoá toàn bộ history của customer.
- **Yêu cầu log chính tại `chat_routes`**
  - `event.category = "chatbot"`.
  - `event.action` gợi ý:
    - `"chatbot.chat"` cho `POST /chat/{threadId}`.
    - `"chatbot.chat_history.get"` cho `GET /chat-history/...`.
    - `"chatbot.chat_history.clear"` cho `POST /chat-history-clear/...`.
  - Ngữ cảnh nên có: `customer_id`, `threadId`, `access`, `llm_provider`, cờ `has_image` (có ảnh hay không), kích thước `history_override` (nếu có), `http.status_code`.

#### 8.2.2. Luồng FAQ Mobile

- **Nguồn `dangbaitudong`**
  - `faq_mobile_controller.*` gọi `settings.CHATBOT_API_BASE_URL`:
    - `GET /faqs/{customer_id}`.
    - `POST /faq/{customer_id}`.
    - `PUT /faq/{customer_id}/{faq_id}`.
    - `DELETE /faq/{customer_id}/{faq_id}`.
    - `DELETE /faqs/{customer_id}`.
    - `POST /insert-faq/{customer_id}` (import Excel).
    - `GET /faq-export/{customer_id}` (export Excel).
- **Đích `ChatbotMobileStore` (controller cần log)**
  - **`faq_routes`**: các endpoint tương ứng.
- **Yêu cầu log chính tại `faq_routes`**
  - `event.category = "faq_mobile"`.
  - `event.action` gợi ý: `"faq_mobile.list"`, `"faq_mobile.create"`, `"faq_mobile.update"`, `"faq_mobile.delete"`, `"faq_mobile.delete_all"`, `"faq_mobile.import"`, `"faq_mobile.export"`.
  - Ngữ cảnh: `customer_id`, `faq_id` (nếu có), `classification`, `question`, `has_image`, số bản ghi import/export.

#### 8.2.3. Luồng Document & GraphRAG

- **Nguồn `dangbaitudong`** (`document_controller`)
  - Upload & quản lý document:
    - `upload_text` → `POST /upload-text/{customer_id}`.
    - `upload_file` → `POST /upload-file/{customer_id}`.
    - `upload_url` → `POST /upload-url/{customer_id}`.
    - `list_documents` → `GET /documents/{customer_id}`.
    - `list_documents_original` → `GET /document-original/{customer_id}?source=...`.
    - `delete_all_documents` → `DELETE /documents/{customer_id}`.
    - `delete_documents_by_source` → `DELETE /sources/{customer_id}?source=...`.
    - `list_document_sources` → `GET /sources/{customer_id}`.
  - Crawl website + sitemap:
    - `upload_website` → `POST /start-sitemap-crawl/{customer_id}` rồi client stream `GET /sitemap-progress/{task_id}` và có thể `POST /cancel-crawl/{task_id}`, `GET /crawl-status/{task_id}`.
  - Reindex GraphRAG:
    - `reindex_documents` → `POST /graphrag/reindex/{customer_id}`.
- **Đích `ChatbotMobileStore` (controller cần log)**
  - **`document_routes`**: tất cả endpoint `/upload-*`, `/documents*`, `/sources*`, `/start-sitemap-crawl`, `/sitemap-progress`, `/cancel-crawl`, `/crawl-status`.
  - **`graphrag_routes`**: `POST /graphrag/reindex/{customer_id}`.
- **Yêu cầu log chính**
  - `event.category = "document"` hoặc `"document_sitemap"`.
  - Trường quan trọng: `customer_id`, `source_name`, loại document (`text`/`file`/`url`), kích thước nội dung, `crawl.task_id`, `crawl.status`, `graphrag.method`, `graphrag.provider`.

#### 8.2.4. Luồng đơn hàng (Orders)

- **Nguồn `dangbaitudong`** (`order_controller`, `order_chatcustom_controller`)
  - `order_controller` → ChatbotMobileStore:
    - `GET /orders/{customer_id}/products`.
    - `GET /orders/{customer_id}/services`.
    - `GET /orders/{customer_id}/accessories`.
    - `PUT /orders/{customer_id}/{thread_id}/{order_id}` (cập nhật `status`).
  - `order_chatcustom_controller` → ChatbotCustom (không thuộc ChatbotMobileStore, chỉ ghi để phân biệt).
- **Đích `ChatbotMobileStore` (controller cần log)**
  - **`order_routes`**: các endpoint `/orders/...` và `PUT /orders/{customer_id}/{thread_id}/{order_id}`.
- **Yêu cầu log chính tại `order_routes`**
  - `event.category = "order"`.
  - `event.action` gợi ý: `"order.product.list"`, `"order.service.list"`, `"order.accessory.list"`, `"order.status.update"`.
  - Ngữ cảnh: `customer_id`, `thread_id`, `order_id`, `order_type`, `status` mới, số lượng đơn hàng trả về.

#### 8.2.5. Luồng cấu hình & trạng thái chatbot

- **Nguồn `dangbaitudong`** (`user_config_controller`, `chatbot_js_settings_controller`)
  - Cấu hình persona / prompt / feature:
    - `user_config_controller.*` gọi `config_routes`:
      - `/config/persona/{customer_id}` (`PUT`/`GET`/`DELETE`).
      - `/config/prompt/{customer_id}` (`PUT`/`GET`/`DELETE`).
      - `/config/service-feature/{customer_id}` (`PUT`/`GET`).
      - `/config/accessory-feature/{customer_id}` (`PUT`/`GET`).
  - Trạng thái bật/tắt bot toàn hệ thống:
    - `user_config_controller.get_mobile_bot_status` → `GET /customer/status/{customer_id}`.
    - `user_config_controller.stop_mobile_bot` → `POST /customer/stop/{customer_id}`.
    - `user_config_controller.start_mobile_bot` → `POST /customer/start/{customer_id}`.
    - `zalo_oa_tasks._async_generate_and_send` cũng gọi `GET /customer/status/{owner_user_id}`.
  - Cấu hình khung chat (JS agent) dùng ChatbotMobile:
    - `user_config_controller.create_chatbot_js_agent` → `POST /settings/`.
    - `user_config_controller.get_chatbot_js_agent` → `GET /settings/{customer_id}`.
    - `user_config_controller.update_chatbot_js_agent` → `PUT /settings/{customer_id}`.
    - `user_config_controller.upload_chatbot_js_agent_icon` → `POST /settings/{customer_id}/upload-icon`.
    - `chatbot_js_settings_controller.get_js_chatbot_agent` → `GET /settings/{customer_id}`.
    - `chatbot_js_settings_controller.update_js_chatbot_agent` → `PUT /settings/{customer_id}`.
- **Đích `ChatbotMobileStore` (controller cần log)**
  - **`config_routes`**: tất cả `/config/*` endpoint.
  - **`control_routes`**: `/customer/stop/{customer_id}`, `/customer/start/{customer_id}`, `/customer/status/{customer_id}` (và có thể `/is_sale/*`, `/stop/{customer_id}/{thread_id}`, ... nếu sau này dùng).
  - **`setting_routes`**: `/settings/`, `/settings/{customer_id}`, `/settings/{customer_id}/upload-icon`.
- **Yêu cầu log chính**
  - `event.category` gợi ý: `"config"`, `"chatbot_control"`, `"chatbot_js"`.
  - Ngữ cảnh: `customer_id`, loại config (`persona` / `prompt` / `service_feature` / `accessory_feature` / `product_feature` / `js_settings`), các flag `*_feature_enabled`, trạng thái bot (`active` / `stopped`).

#### 8.2.6. Luồng thông tin cửa hàng (store-info) Mobile

- **Nguồn `dangbaitudong`** (`document_controller` – phần *mobile store info*)
  - `create_or_update_store_info_mobile` → `PUT /store-info/{customer_id}`.
  - `get_store_info_mobile` → `GET /store-info/{customer_id}`.
  - `delete_store_info_mobile` → `DELETE /store-info/{customer_id}`.
- **Đích `ChatbotMobileStore` (controller cần log)**
  - **`info_store_routes`**: các endpoint `/store-info/{customer_id}`.
- **Yêu cầu log chính**
  - `event.category = "store_info"`.
  - Ngữ cảnh: `customer_id`, các trường thay đổi (`store_name`, `store_address`, `store_phone`, ...), cờ `has_image`.

#### 8.2.7. Luồng đồng bộ dữ liệu sản phẩm / dịch vụ / phụ kiện (ES / embedding)

- **Nguồn `dangbaitudong`**
  - `ChatbotService` + `UserSyncFromUrlService` dùng `settings.CHATBOT_API_BASE_URL`:
    - Sản phẩm (`product_routes`):
      - Bulk / full sync: `/products-embed/bulk/{customer_id}`, `/upload-product-embed/{customer_id}`, `/insert-product/{customer_id}`, `/products/{customer_id}`, `/products/bulk/{customer_id}`.
      - Row-level: `/insert-product-embed-row/{customer_id}`, `/product-embed/{customer_id}/{product_id}`, `/product/{customer_id}/{product_id}`.
    - Dịch vụ (`service_routes`):
      - Bulk: `/services-embed/bulk/{customer_id}`, `/upload-service-embed/{customer_id}`, `/insert-service/{customer_id}`, `/services/{customer_id}`, `/services/bulk/{customer_id}`.
      - Row-level: `/insert-service-embed-row/{customer_id}`, `/service-embed/{customer_id}/{service_id}`, `/service/{customer_id}/{service_id}`.
    - Phụ kiện (`accessory_routes`):
      - Bulk: `/accessories-embed/bulk/{customer_id}`, `/upload-accessory-embed/{customer_id}`, `/insert-accessory/{customer_id}`, `/accessories/{customer_id}`, `/accessories/bulk/{customer_id}`.
      - Row-level: `/insert-accessory-embed-row/{customer_id}`, `/accessory-embed/{customer_id}/{accessory_id}`, `/accessory/{customer_id}/{accessory_id}`.
- **Đích `ChatbotMobileStore` (controller cần log)**
  - **`product_routes`**, **`service_routes`**, **`accessory_routes`** cho tất cả endpoint nêu trên.
- **Yêu cầu log chính**
  - `event.category` gợi ý: `"product_catalog"`, `"service_catalog"`, `"accessory_catalog"`.
  - Ngữ cảnh: `customer_id`, loại thao tác (`upload_full`, `upload_embed`, `insert`, `update`, `delete`, `bulk_delete`), số bản ghi thành công / lỗi, các mã chính: `ma_san_pham`, `ma_dich_vu`, `accessory_code`.

#### 8.2.8. Luồng instructions & training (nếu được gọi từ `dangbaitudong`)

- **Nguồn `dangbaitudong`**
  - `chatbot_controller` (admin instructions) đã dùng `_call_chatbot_api` tới:
    - `GET /instructions`.
    - `GET /instructions/{key}`.
    - `POST /instructions`.
    - `PUT /instructions`.
    - `PUT /instructions/{key}`.
    - `DELETE /instructions/{key}`.
  - Hiện tại chưa thấy controller nào gọi trực tiếp `training_routes`, nhưng khi thêm luồng ingest/training từ `dangbaitudong` thì dùng:
    - `POST /training/ingest-chat-batch`.
    - `POST /training/start`.
- **Đích `ChatbotMobileStore` (controller cần log)**
  - **`instruction_routes`** cho toàn bộ CRUD trên instructions.
  - **`training_routes`** cho ingest/training.
- **Yêu cầu log chính**
  - `event.category` gợi ý: `"instruction"`, `"training"`.
  - Ngữ cảnh cho training: `customer_id`, `source`, số bản ghi ingest thành công / trùng / lỗi, provider & model dùng để training.

### 8.3. Quy ước `event.category` / `event.action` cho controller ChatbotMobileStore

- **`chat_routes`**
  - `event.category = "chatbot"`.
  - `event.action`: `"chatbot.chat"`, `"chatbot.chat_history.get"`, `"chatbot.chat_history.clear"`.
- **`document_routes` / `graphrag_routes`**
  - `event.category = "document"` hoặc `"document_sitemap"`.
  - `event.action`: `"document.upload_text"`, `"document.upload_file"`, `"document.upload_url"`, `"document.list"`, `"document.delete_all"`, `"document.sources.list"`, `"document.sources.delete"`, `"document.sitemap.crawl"`, `"document.sitemap.progress"`, `"document.sitemap.cancel"`, `"document.graphrag.reindex"`.
- **`faq_routes`** → `event.category = "faq_mobile"`, action giống mục 8.2.2.
- **`order_routes`** → `event.category = "order"`, action giống mục 8.2.4.
- **`config_routes`** → `event.category = "config"`, action: `"config.persona.*"`, `"config.prompt.*"`, `"config.service_feature.*"`, `"config.accessory_feature.*"`, `"config.product_feature.*"`.
- **`setting_routes`** → `event.category = "chatbot_js"`, action: `"chatbot_js.settings.create"`, `"chatbot_js.settings.get"`, `"chatbot_js.settings.update"`, `"chatbot_js.settings.upload_icon"`.
- **`control_routes`** → `event.category = "chatbot_control"`, action: `"chatbot.customer.stop"`, `"chatbot.customer.start"`, `"chatbot.customer.status"`, ...
- **`info_store_routes`** → `event.category = "store_info"`, action: `"store_info.get"`, `"store_info.update"`, `"store_info.delete"`.
- **`product_routes` / `service_routes` / `accessory_routes`**
  - `event.category` lần lượt: `"product_catalog"`, `"service_catalog"`, `"accessory_catalog"`.
  - Action dạng: `"*.upload_full"`, `"*.upload_embed"`, `"*.insert"`, `"*.update"`, `"*.delete"`, `"*.bulk"`, `"*.bulk_delete"`.
- **`instruction_routes`** → `event.category = "instruction"`, action: `"instruction.list"`, `"instruction.create"`, `"instruction.update"`, `"instruction.delete"`.
- **`training_routes`** → `event.category = "training"`, action: `"training.ingest_chat_batch"`, `"training.start"`.
