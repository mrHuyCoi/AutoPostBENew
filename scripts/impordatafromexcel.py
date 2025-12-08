import asyncio
import sys
import os
import uuid
from datetime import datetime
import tkinter as tk
from tkinter import filedialog

import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

# Thêm đường dẫn gốc dự án
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database.database import engine
from app.models.device_info import DeviceInfo
from app.models.device_storage import DeviceStorage
from app.models.device_color import DeviceColor
from app.models.color import Color


async def import_device_info(file_path, sheet_name, brand_name):
    """Import dữ liệu thiết bị vào bảng device_info."""

    df = pd.read_excel(file_path, sheet_name=sheet_name)

    # ✅ Làm sạch cột
    df.columns = [
        col.strip().lower()
        .replace(" ", "_")
        .replace("→", "_")
        for col in df.columns
    ]

    # ✅ Thêm trường brand từ người dùng
    df["brand"] = brand_name

    def parse_date_to_str(date_val):
        if not pd.notna(date_val):
            return None
        try:
            # pd.to_datetime is robust and can handle strings, timestamps, etc.
            return pd.to_datetime(date_val).strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            print(f"⚠️  Could not parse date value: {date_val}")
            return None

    async with AsyncSession(engine) as session:
        async with session.begin():
            for _, row in df.iterrows():
                model = row.get("model")
                if not model:
                    continue

                # Tìm thiết bị đã có hoặc tạo mới
                result = await session.execute(
                    select(DeviceInfo).where(DeviceInfo.model == model)
                )
                device = result.scalars().first()

                if not device:
                    print(f"✨ Tạo mới thiết bị: '{model}'")
                    release_date_val = row.get("ra_mat")
                    device = DeviceInfo(
                        id=uuid.uuid4(),
                        model=model,
                        release_date=parse_date_to_str(release_date_val),
                        screen=row.get("man_hinh"),
                        chip_ram=row.get("chip_/_ram"),
                        camera=row.get("camera_sau___truoc"),
                        battery=str(row.get("pin_(mah)")).replace(" ", "") if pd.notna(row.get("pin_(mah)")) else "",
                        connectivity_os=row.get("ket_noi_/_hđh"),
                        color_english=row.get("mau_sac_tieng_anh"),
                        dimensions_weight=row.get("kich_thuoc_/_trong_luong"),
                        warranty=row.get("bao_hanh"),
                        brand=row.get("brand"),
                        user_id=None,
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow()
                    )
                    session.add(device)
                    # Cần flush để lấy device.id cho các quan hệ
                    await session.flush()
                else:
                    print(f"ℹ️  Thiết bị '{model}' đã tồn tại. Cập nhật thông tin còn thiếu và kiểm tra dung lượng, màu sắc mới.")
                    updated = False
                    
                    def update_field(field_name, excel_value, current_value):
                        nonlocal updated
                        if pd.notna(excel_value) and not current_value:
                            print(f"    🔄 Cập nhật '{field_name}' cho '{model}'")
                            updated = True
                            return excel_value
                        return current_value

                    device.release_date = update_field(
                        'release_date',
                        parse_date_to_str(row.get("ra_mat")),
                        device.release_date
                    )
                    device.screen = update_field('screen', row.get("man_hinh"), device.screen)
                    device.chip_ram = update_field('chip_ram', row.get("chip_/_ram"), device.chip_ram)
                    device.camera = update_field('camera', row.get("camera_sau___truoc"), device.camera)
                    device.battery = update_field(
                        'battery',
                        str(row.get("pin_(mah)")).replace(" ", "") if pd.notna(row.get("pin_(mah)")) else None,
                        device.battery
                    )
                    device.connectivity_os = update_field('connectivity_os', row.get("ket_noi_/_hđh"), device.connectivity_os)
                    device.color_english = update_field('color_english', row.get("mau_sac_tieng_anh"), device.color_english)
                    device.dimensions_weight = update_field('dimensions_weight', row.get("kich_thuoc_/_trong_luong"), device.dimensions_weight)
                    device.warranty = update_field('warranty', row.get("bao_hanh"), device.warranty)
                    device.brand = update_field('brand', row.get("brand"), device.brand)

                    if updated:
                        device.updated_at = datetime.utcnow()

                # Xử lý dung lượng (cho cả thiết bị mới và đã có)
                storage_raw = row.get("dung_luong")
                if pd.notna(storage_raw):
                    # Normalize separators and split into individual capacity strings
                    capacities_str = str(storage_raw).replace('-', '/')
                    capacities = capacities_str.split('/')
                    
                    for cap_str_raw in capacities:
                        cap_str = cap_str_raw.strip().upper()
                        if not cap_str:
                            continue

                        try:
                            capacity_gb = 0
                            if 'TB' in cap_str:
                                num_part = cap_str.replace('TB', '').strip()
                                capacity_gb = int(num_part) * 1024
                            elif 'GB' in cap_str:
                                num_part = cap_str.replace('GB', '').strip()
                                capacity_gb = int(num_part)
                            else:
                                # Assume GB if no unit is specified
                                capacity_gb = int(cap_str)

                            if capacity_gb > 0:
                                # Check if storage already exists for this device
                                storage_result = await session.execute(
                                    select(DeviceStorage).where(
                                        DeviceStorage.device_info_id == device.id,
                                        DeviceStorage.capacity == capacity_gb
                                    )
                                )
                                if not storage_result.scalars().first():
                                    new_storage = DeviceStorage(
                                        id=uuid.uuid4(),
                                        device_info_id=device.id,
                                        capacity=capacity_gb,
                                        user_id=None
                                    )
                                    session.add(new_storage)
                                    print(f"    ➕ Thêm dung lượng mới {capacity_gb}GB cho '{model}'")
                        except ValueError:
                            print(f"⚠️ Bỏ qua giá trị dung lượng không hợp lệ: '{cap_str_raw}' cho model '{model}'")

                # Xử lý màu sắc (cho cả thiết bị mới và đã có)
                colors_raw = row.get("mau_sac")
                if pd.notna(colors_raw):
                    color_names = [c.strip() for c in str(colors_raw).split(',')]
                    for color_name in color_names:
                        if not color_name:
                            continue
                        
                        # Find or create color
                        color_result = await session.execute(select(Color).where(Color.name == color_name))
                        color = color_result.scalars().first()
                        
                        if not color:
                            color = Color(id=uuid.uuid4(), name=color_name, user_id=None)
                            session.add(color)
                            await session.flush() # Flush to get color.id

                        # Check if device_color link already exists
                        device_color_result = await session.execute(
                            select(DeviceColor).where(
                                DeviceColor.device_info_id == device.id,
                                DeviceColor.color_id == color.id
                            )
                        )
                        if not device_color_result.scalars().first():
                            new_device_color = DeviceColor(
                                id=uuid.uuid4(),
                                device_info_id=device.id,
                                color_id=color.id,
                                user_id=None
                            )
                            session.add(new_device_color)
                            print(f"    🎨 Thêm màu mới '{color_name}' cho '{model}'")

        await session.commit()
        print(f"✅ Import dữ liệu từ sheet '{sheet_name}' hoàn tất!")


async def main():
    root = tk.Tk()
    root.withdraw()

    file_path = filedialog.askopenfilename(
        title="Chọn file Excel",
        filetypes=(("Excel files", "*.xlsx *.xlsm"), ("All files", "*.*"))
    )

    if not file_path:
        print("❌ Đã hủy thao tác.")
        return

    try:
        xls = pd.ExcelFile(file_path)
        sheet_names = xls.sheet_names

        if not sheet_names:
            print("File Excel này không có sheet nào.")
            return

        print("\nCác sheet có trong file:")
        for i, name in enumerate(sheet_names):
            print(f"  {i + 1}. {name}")
        print("  all. Chọn tất cả")

        selected_sheets = []
        while not selected_sheets:
            try:
                choice_str = input(f"\n➡️ Vui lòng chọn các sheet để import (ví dụ: 1,3,5 hoặc 'all'): ")
                if choice_str.strip().lower() == 'all':
                    selected_sheets = sheet_names
                    break

                choices = [int(c.strip()) for c in choice_str.split(',')]

                invalid_choices = [c for c in choices if not (1 <= c <= len(sheet_names))]
                if invalid_choices:
                    print(f"❗️ Lựa chọn không hợp lệ: {invalid_choices}. Vui lòng chọn số từ 1 đến {len(sheet_names)}.")
                    continue
                
                unique_choices = sorted(list(set(choices)))
                selected_sheets = [sheet_names[c - 1] for c in unique_choices]

            except ValueError:
                print("❗️ Lựa chọn không hợp lệ. Vui lòng nhập số, cách nhau bằng dấu phẩy, hoặc 'all'.")

        if not selected_sheets:
            print("❌ Không có sheet nào được chọn. Thao tác đã hủy.")
            return

        brands_for_sheets = {}
        for sheet_name in selected_sheets:
            brand_name = ""
            while not brand_name:
                brand_name = input(f"➡️  Vui lòng nhập tên thương hiệu cho sheet '{sheet_name}': ").strip()
                if not brand_name:
                    print("❗️ Tên thương hiệu không được để trống.")
            brands_for_sheets[sheet_name] = brand_name

        for sheet_name, brand_name in brands_for_sheets.items():
            print(f"\n🚀 Bắt đầu import từ sheet: '{sheet_name}' với thương hiệu '{brand_name}'...")
            await import_device_info(file_path, sheet_name, brand_name)

    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
