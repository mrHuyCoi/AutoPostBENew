import os
import psycopg2
from psycopg2.extras import execute_batch

# Ưu tiên dùng DATABASE_URL nếu có, ví dụ:
# postgresql://postgres:password@localhost:5432/dangbaitudong
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    # Nếu chưa có DATABASE_URL, sửa tham số kết nối bên dưới cho đúng môi trường của bạn
    conn = psycopg2.connect(
        dbname="dangbaitudong",
        user="postgres",
        password="root",
        host="localhost",
        port=5432,
    )
else:
    conn = psycopg2.connect(DATABASE_URL)

def seed_chatbot_data():
    with conn:
        with conn.cursor() as cur:
            # 1. Seed các gói chatbot
            insert_plans_sql = """
            INSERT INTO chatbot_plans (id, name, description, monthly_price, max_api_calls)
            VALUES (%s::uuid, %s, %s, %s, %s)
            ON CONFLICT (name) DO NOTHING;
            """

            plans = [
                (
                    "11111111-1111-1111-1111-111111111111",
                    "Chatbot API riêng",
                    "Dùng API key Gemini/OpenAI riêng của bạn, không giới hạn lượt từ hệ thống",
                    100000,
                    None,
                ),
                (
                    "22222222-2222-2222-2222-222222222222",
                    "Chatbot 1000 lượt",
                    "Gói mua theo lượt: 1000 lượt API dùng GEMINI_API_KEY hệ thống",
                    150000,
                    1000,
                ),
                (
                    "33333333-3333-3333-3333-333333333333",
                    "Chatbot 3000 lượt",
                    "Gói mua theo lượt: 3000 lượt API dùng GEMINI_API_KEY hệ thống",
                    250000,
                    3000,
                ),
            ]

            execute_batch(cur, insert_plans_sql, plans)

            # 2. Subscription mẫu cho một user để test nhanh
            # ⚠️ SỬA email này cho đúng user test của bạn
            test_email = "your_test_email@example.com"

            insert_sub_sql = """
            INSERT INTO user_chatbot_subscriptions (
              id,
              user_id,
              plan_id,
              start_date,
              end_date,
              months_subscribed,
              total_price,
              is_active,
              status,
              max_api_calls,
              api_calls_used
            )
            SELECT
              %s::uuid,
              u.id,
              p.id,
              NOW(),
              NOW() + INTERVAL '30 days',
              1,
              p.monthly_price,
              TRUE,
              'approved',
              p.max_api_calls,
              0
            FROM users u
            JOIN chatbot_plans p ON p.name = 'Chatbot 1000 lượt'
            WHERE u.email = %s
              AND NOT EXISTS (
                SELECT 1 FROM user_chatbot_subscriptions s
                WHERE s.id = %s::uuid
              );
            """

            sub_id = "44444444-4444-4444-4444-444444444444"
            cur.execute(insert_sub_sql, (sub_id, test_email, sub_id))

    print("✅ Seed chatbot plans & sample subscription done.")

if __name__ == "__main__":
    try:
        seed_chatbot_data()
    finally:
        conn.close()