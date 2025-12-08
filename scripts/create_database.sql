-- Script tạo database dangbaitudong nếu chưa tồn tại
CREATE DATABASE dangbaitudong WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TEMPLATE = template0;

COMMENT ON DATABASE dangbaitudong IS 'Database cho ứng dụng Đăng Bài Tự Động';
