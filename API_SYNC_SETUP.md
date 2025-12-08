# Hướng dẫn cài đặt đồng bộ dữ liệu từ API bên ngoài

## Cấu hình Environment Variable

Để sử dụng tính năng đồng bộ dữ liệu từ API bên ngoài, bạn cần thêm biến môi trường sau:

### 1. Thêm vào file .env
```bash
API_DATA=https://your-external-api-endpoint.com/api/products
```

### 2. Hoặc export trong shell
```bash
export API_DATA="https://your-external-api-endpoint.com/api/products"
```

### 3. Hoặc thêm vào docker-compose.yml
```yaml
environment:
  - API_DATA=https://your-external-api-endpoint.com/api/products
```

## Định dạng dữ liệu API mong đợi

API bên ngoài cần trả về dữ liệu JSON với định dạng sau:

```json
[
  {
    "id": "9118",
    "code": "SP011457",
    "name": "Hộp để tô vít, dụng cụ RELIFE RL - 001K",
    "price": "195000",
    "price_wholesale": "180000",
    "category_name": "Thiết bị>>Dụng cụ",
    "manufactory_name": "RELIFE",
    "quantity": 1,
    "content": "mô tả sản phẩm",
    "image": "https://example.com/image.jpg",
    "warranty": null,
    "link": "https://example.com/product-link",
    "sale_off": 0,
    "list_extend": [
      {
        "code": "SP011458",
        "name": "MÀU SẮC:Xám",
        "price": "195000",
        "price_wholesale": 180000,
        "quantity": 1,
        "image": "https://example.com/variant-image.jpg"
      },
      {
        "code": "SP011457",
        "name": "MÀU SẮC:Xanh",
        "price": "195000",
        "price_wholesale": 180000,
        "quantity": 1,
        "image": "https://example.com/variant-image2.jpg"
      }
    ]
  }
]
```

## Cách sử dụng

1. **Cấu hình API**: Đảm bảo biến môi trường `API_DATA` được set đúng URL
2. **Truy cập giao diện**: Vào tab "Linh kiện" > "Nạp dữ liệu API"
3. **Đồng bộ dữ liệu**: Nhấn nút "Đồng bộ ngay" để tải dữ liệu từ API
4. **Xem kết quả**: Hệ thống sẽ hiển thị thống kê về số lượng sản phẩm được tạo mới, cập nhật, hoặc bỏ qua

## Lưu ý quan trọng

- **Xử lý biến thể**: Các sản phẩm trong `list_extend` sẽ được tách thành sản phẩm riêng biệt
- **Cập nhật thông minh**: Hệ thống sẽ cập nhật sản phẩm hiện có nếu `product_code` đã tồn tại
- **Đồng bộ Chatbot**: Dữ liệu sẽ được tự động đồng bộ với các hệ thống Chatbot
- **Background Processing**: Quá trình đồng bộ chạy background để không block UI

## Troubleshooting

### Lỗi "Không tìm thấy cấu hình API_DATA"
- Kiểm tra biến môi trường `API_DATA` đã được set chưa
- Restart application sau khi thay đổi environment variable

### Lỗi kết nối API
- Kiểm tra URL API có đúng không
- Đảm bảo API bên ngoài đang hoạt động
- Kiểm tra firewall/proxy settings

### Dữ liệu không đồng bộ
- Kiểm tra format JSON trả về có đúng schema không
- Xem log để biết chi tiết lỗi

## API Endpoints

### POST /api/v1/product-components/sync-from-api
Endpoint để thực hiện đồng bộ dữ liệu từ API bên ngoài.

**Headers:**
```
Authorization: Bearer <access_token>
Content-Type: application/json
```

**Response:**
```json
{
  "message": "Đồng bộ dữ liệu thành công",
  "total_synced": 50,
  "total_created": 30,
  "total_updated": 15,
  "total_skipped": 5
}
```

## Monitoring

Bạn có thể theo dõi tiến trình đồng bộ qua:
- UI feedback trong tab "Nạp dữ liệu API"
- Application logs
- Database để kiểm tra dữ liệu đã được tạo/cập nhật

## Security

- API endpoint được bảo vệ bằng JWT authentication
- Chỉ user đã đăng nhập mới có thể thực hiện đồng bộ
- Dữ liệu được validate trước khi lưu vào database
