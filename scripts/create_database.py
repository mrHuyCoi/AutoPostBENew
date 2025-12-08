#!/usr/bin/env python
import asyncio
import sys
import os
from dotenv import load_dotenv
from urllib.parse import urlparse

# Thêm thư mục gốc của dự án vào sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# Load biến môi trường từ file .env
load_dotenv(os.path.join(project_root, '.env'))

import asyncpg

def parse_database_url(url):
    """Parse DATABASE_URL to get connection parameters"""
    parsed = urlparse(url)
    # Remove 'postgresql+asyncpg://' prefix
    path = parsed.path.lstrip('/')
    return {
        'user': parsed.username,
        'password': parsed.password,
        'host': parsed.hostname,
        'port': parsed.port,
        'database': path
    }

async def create_database():
    # Lấy DATABASE_URL từ biến môi trường
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL không được tìm thấy trong biến môi trường")

    # Parse DATABASE_URL để lấy thông tin kết nối
    db_params = parse_database_url(database_url)
    
    # Kết nối đến PostgreSQL với database mặc định
    conn = await asyncpg.connect(
        user=db_params['user'],
        password=db_params['password'],
        host=db_params['host'],
        port=db_params['port'],
        database='postgres'  # Kết nối đến database mặc định
    )
    
    try:
        # Kiểm tra xem database đã tồn tại chưa
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1",
            db_params['database']
        )
        
        if exists:
            print(f"Database {db_params['database']} đã tồn tại!")
        else:
            # Tạo database mới
            await conn.execute(f'''
                CREATE DATABASE {db_params['database']}
                WITH 
                    OWNER = {db_params['user']}
                    ENCODING = 'UTF8'
                    LC_COLLATE = 'en_US.utf8'
                    LC_CTYPE = 'en_US.utf8'
                    TEMPLATE = template0
            ''')
            
            # Thêm comment cho database
            await conn.execute(f'''
                COMMENT ON DATABASE {db_params['database']} 
                IS 'Database cho ứng dụng Đăng Bài Tự Động'
            ''')
            
            print(f"Đã tạo database {db_params['database']} thành công!")
            
    except Exception as e:
        print(f"Lỗi khi tạo database: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(create_database()) 