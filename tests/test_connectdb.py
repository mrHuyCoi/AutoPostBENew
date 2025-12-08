import asyncio, asyncpg

async def test():
    conn = await asyncpg.connect(
        user='postgres',
        password='postgres',
        database='dangbaitudong',
        host='localhost'
    )
    print("✅ Kết nối thành công")
    await conn.close()

asyncio.run(test())
