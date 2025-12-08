functions = [
    {
        "name": "generate_metadata",
        "description": "Tạo metadata cho một video YouTube, trả về đủ các trường của bảng youtube_metadata.",
        "parameters": {
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Tiêu đề hấp dẫn, súc tích, thu hút người đọc."
                },
                "description": {
                    "type": "string",
                    "description": "Mô tả chi tiết về nội dung video, chuyên nghiệp và thu hút người xem."
                },
                "tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Danh sách từ khóa SEO, phân cách bởi dấu phẩy."
                },
                "privacy_status": {
                    "type": "string",
                    "enum": ["public", "private", "unlisted"],
                    "default": "public",
                    "description": "Trạng thái riêng tư của video."
                },
                "content_type": {
                    "type": "string",
                    "enum": ["regular", "shorts"],
                    "default": "regular",
                    "description": "Loại nội dung: regular (video thường) hoặc shorts (YouTube Shorts)."
                },
                "shorts_hashtags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Hashtags cho Shorts (nếu là Shorts)."
                }
            },
            "required": ["title", "description", "tags"]
        }
    }
]