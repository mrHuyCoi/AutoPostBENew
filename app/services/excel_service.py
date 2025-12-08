import pandas as pd
import io
import logging
import pandas as pd
import xlsxwriter
from io import BytesIO
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
import uuid
from fastapi import BackgroundTasks
import json
import traceback
from app.dto.user_device_dto import UserDeviceCreate, UserDeviceUpdate
from app.repositories.user_device_repository import UserDeviceRepository
from app.repositories.brand_repository import BrandRepository
from app.dto.user_device_dto import UserDeviceDetailRead
from app.dto.excel_dto import ImportResult
from app.dto.brand_dto import BrandCreate, BrandUpdate, BrandRead
from app.dto.device_info_dto import DeviceInfoCreate, DeviceInfoUpdate, DeviceInfoRead
from app.repositories.device_info_repository import DeviceInfoRepository
from app.models.material import Material
from sqlalchemy import select, or_
from app.repositories.color_repository import ColorRepository
from app.repositories.device_storage_repository import DeviceStorageRepository
from app.repositories.user_repository import UserRepository
from app.repositories.service_repository import ServiceRepository
from app.models.service import Service as ServiceModel
from app.models.brand import Brand
from app.models.device_brand import DeviceBrand
from app.repositories.device_brand_repository import DeviceBrandRepository
from app.services.chatbot_service import ChatbotService
from app.services.chatbot_sync_service import ChatbotSyncService
from app.repositories.product_component_repository import ProductComponentRepository
from app.tasks.elasticsearch_tasks import es_bulk_upsert_accessories
from app.dto.product_component_dto import ProductComponentCreate, ProductComponentUpdate, ProductComponentRead
from app.models.product_component import ProductComponent
from app.models.user_device import UserDevice
from app.repositories.category_repository import CategoryRepository
from app.repositories.property_repository import PropertyRepository
from datetime import datetime
import pytz
import re
from app.dto.property_dto import PropertyCreate
from app.services.product_component_service import ProductComponentService

# Setup logger
logger = logging.getLogger(__name__)


def safe_str(val):
    if pd.isna(val):
        return None
    # Handle UUID objects safely
    if hasattr(val, 'hex'):  # UUID object
        return str(val)
    # Handle regular values
    result = str(val).strip()
    return result if result else None

def convert_to_vietnam_time(dt_value):
    """
    Convert a datetime value to Vietnam time (UTC+7) for import operations.
    """
    if dt_value is None or pd.isna(dt_value):
        return None
    
    # If it's already a datetime object
    if isinstance(dt_value, datetime):
        # If it's naive, assume it's Vietnam time
        if dt_value.tzinfo is None:
            return dt_value
        # If it's timezone-aware, convert to Vietnam time
        else:
            vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
            return dt_value.astimezone(vietnam_tz).replace(tzinfo=None)
    
    # If it's a string, parse it and assume it's Vietnam time
    try:
        dt = pd.to_datetime(dt_value)
        if pd.isna(dt):
            return None
        return dt.replace(tzinfo=None) if isinstance(dt, datetime) else dt
    except:
        return None


def _parse_date_string(date_str: Optional[str]) -> Optional[datetime]:
    """
    Safely parse a date string into a datetime object.
    """
    if not date_str or pd.isna(date_str):
        return None
    try:
        # pd.to_datetime is flexible and can handle various formats
        dt = pd.to_datetime(date_str)
        # Check if parsing resulted in NaT (Not a Time)
        if pd.isna(dt):
            return None
        return dt
    except (ValueError, TypeError):
        # Return None if parsing fails
        return None


class ExcelService:
    """
    Service xử lý các thao tác liên quan đến import/export Excel.
    """

    @staticmethod
    async def export_device_info_template() -> bytes:
        """
        Tạo và trả về một file Excel mẫu để import thông tin thiết bị.
        """
        data = {
            'model': ['iPhone 15 Pro Max'],
            'ra_mat': ['2023-09-22'],
            'man_hinh': ['6.7" Super Retina XDR OLED'],
            'chip_/_ram': ['Apple A17 Pro, 8GB RAM'],
            'camera_sau___truoc': ['Chính 48 MP & Phụ 12 MP, 12 MP'],
            'pin_(mah)': ['4422 mAh, 20W'],
            'ket_noi_/_hđh': ['5G, iOS 17'],
            'mau_sac_tieng_anh': ['Natural Titanium, Blue Titanium, White Titanium, Black Titanium'],
            'kich_thuoc_/_trong_luong': ['Dài 159.9 mm - Ngang 76.7 mm - Dày 8.25 mm - Nặng 221 g'],
            'Vật liệu vỏ': ['Khung viền Titanium, Mặt lưng kính nhám'],
            'Cảm biến & Tính năng sức khỏe': ['Face ID, Cảm biến gia tốc, Cảm biến tiệm cận, Con quay hồi chuyển, La bàn, Barometer'],
            'bao_hanh': ['12 tháng'],
            'brand': ['Apple'],
            'dung_luong': ['256GB/512GB/1TB'],
            'mau_sac': ['Titan Tự nhiên, Titan Xanh, Titan Trắng, Titan Đen'],
            'ghi_chu': ['Ghi chú mẫu về thiết bị']
        }
        df = pd.DataFrame(data)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name="DeviceInfo Import", index=False)
            workbook = writer.book
            worksheet = writer.sheets["DeviceInfo Import"]
            header_format = workbook.add_format({'bold': True, 'font_size': 12, 'bg_color': '#D9EAD3', 'border': 1})
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                worksheet.set_column(col_num, col_num, len(value) + 10)
        output.seek(0)
        return output.getvalue()

    @staticmethod
    async def import_device_infos(db: AsyncSession, file_content: bytes, current_user: "User") -> ImportResult:
        df = pd.read_excel(io.BytesIO(file_content))
        
        # Chuẩn hóa tên cột
        original_columns = df.columns
        normalized_columns = []
        for col in df.columns:
            normalized = str(col).strip().lower()
            # Xử lý các ký tự đặc biệt Vietnamese
            normalized = (normalized
                         .replace(" ", "_")
                         .replace("→", "_")
                         .replace("/", "_")
                         .replace("(", "")
                         .replace(")", "")
                         .replace("&", "_"))
            normalized_columns.append(normalized)
        
        df.columns = normalized_columns
        column_map = dict(zip(original_columns, df.columns))
        
        # Debug logging để kiểm tra column mapping
        logger.info(f"Column mapping: {column_map}")
        logger.info(f"Looking for materials column, available columns: {list(df.columns)}")

        total = len(df)
        success = error = updated_count = created_count = 0
        errors = []

        user_id_for_import = None if current_user.is_admin else current_user.id

        for index, row in df.iterrows():
            model_name = safe_str(row.get("model"))
            if not model_name:
                continue

            try:
                # --- 1. Kiểm tra ID để quyết định update hay create ---
                device_id_raw = row.get("id")
                existing_device = None
                
                if pd.notna(device_id_raw) and str(device_id_raw).strip():
                    # Có ID - tìm device theo ID để update
                    try:
                        device_id = uuid.UUID(str(device_id_raw).strip())
                        existing_device = await DeviceInfoRepository.get_by_id(db, device_id)
                        if existing_device:
                            logger.info(f"Found existing device by ID: {device_id}")
                        else:
                            logger.info(f"No device found with ID: {device_id}, will create new")
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid device ID format: {device_id_raw}, will create new device")
                
                # --- 2. Xử lý Vật liệu --- 
                # Hỗ trợ cả tiêu đề từ file export và các biến thể normalize trước đây
                materials_raw = (
                    row.get("vật_liệu_vỏ")
                    or row.get("vật_liệu_vo")
                    or row.get("vat_lieu_vỏ")
                    or row.get("vat_lieu_vo")
                )
                logger.info(f"Materials raw value for model '{model_name}': {materials_raw}")
                material_ids = []
                if pd.notna(materials_raw):
                    material_names = [m.strip() for m in str(materials_raw).split(',') if m.strip()]
                    for material_name in material_names:
                        result = await db.execute(
                            select(Material).where(
                                Material.name == material_name,
                                or_(Material.user_id == user_id_for_import, Material.user_id.is_(None))
                            )
                        )
                        material_obj = result.scalars().first()
                        if not material_obj:
                            material_obj = Material(name=material_name, user_id=user_id_for_import)
                            db.add(material_obj)
                            await db.flush()
                        material_ids.append(material_obj.id)

                # --- 3. Xử lý màu sắc ---
                colors_raw = row.get("mau_sac")
                color_names = []
                if pd.notna(colors_raw):
                    color_names = [c.strip() for c in str(colors_raw).split(',') if c.strip()]
                
                if not color_names:
                    color_names = [None]

                # Chuẩn bị data chung cho device
                battery_raw = row.get("pin_mah")
                battery_value = None
                if pd.notna(battery_raw):
                    if hasattr(battery_raw, 'hex'):
                        battery_value = str(battery_raw)
                    else:
                        battery_value = str(battery_raw).replace(" ", "")
                
                # Xử lý release_date
                release_date_raw = row.get("ra_mat")
                release_date_value = None
                if pd.notna(release_date_raw):
                    if isinstance(release_date_raw, pd.Timestamp):
                        release_date_value = release_date_raw.strftime('%Y-%m-%d')
                    else:
                        release_date_str = str(release_date_raw)
                        if ' ' in release_date_str:
                            release_date_value = release_date_str.split(' ')[0]
                        else:
                            release_date_value = release_date_str

                device_data = {
                    'model': model_name,
                    'release_date': release_date_value,
                    'screen': safe_str(row.get("man_hinh")),
                    'chip_ram': safe_str(row.get("chip___ram")),
                    'camera': safe_str(row.get("camera_sau___truoc")),
                    'battery': battery_value,
                    'connectivity_os': safe_str(row.get("ket_noi___hđh")),
                    'color_english': safe_str(row.get("mau_sac_tieng_anh")),
                    'dimensions_weight': safe_str(row.get("kich_thuoc___trong_luong")),
                    # Chấp nhận cả tiêu đề export và biến thể do normalize lỗi 'ỏ' -> 'o'
                    'sensors_health_features': safe_str(
                        row.get("cảm_biến_tính_năng_sức_khỏe")
                        or row.get("cảm_biến_tính_năng_sức_khoe")
                        or row.get("cảm_biến__tính_năng_sức_khỏe")
                        or row.get("cam_bien_tinh_nang_suc_khoe")
                    ),
                    'warranty': safe_str(row.get("bao_hanh")),
                    'brand': safe_str(row.get("brand")),
                    'user_id': user_id_for_import
                }

                device = None
                if existing_device:
                    # Cập nhật device theo ID
                    update_data = DeviceInfoUpdate(**{k: v for k, v in device_data.items() if v is not None})
                    update_data.material_ids = material_ids
                    device = await DeviceInfoRepository.update(db, existing_device.id, update_data)
                    updated_count += 1
                else:
                    # Tạo device mới
                    create_data = DeviceInfoCreate(**device_data)
                    create_data.material_ids = material_ids
                    device = await DeviceInfoRepository.create(db, create_data)
                    created_count += 1
                
                # --- 4. Liên kết tất cả màu sắc với device này ---
                if device:
                    for color_name in color_names:
                        if color_name:  # Chỉ xử lý khi có màu sắc
                            color_obj = await ColorRepository.get_by_name(db, color_name, user_id_for_import)
                            if not color_obj:
                                color_obj = await ColorRepository.create(db, {'name': color_name, 'user_id': user_id_for_import})
                            
                            existing_link = await ColorRepository.get_device_color_link(db, device.id, color_obj.id)
                            if not existing_link:
                                await ColorRepository.create_device_color_link(db, {'device_info_id': device.id, 'color_id': color_obj.id, 'user_id': user_id_for_import})

                # --- 5. Xử lý dung lượng cho device ---
                created_devices = [device] if device else []

                # --- 6. Xử lý dung lượng cho device ---
                for device in created_devices:
                    storage_raw = row.get("dung_luong")
                    if pd.notna(storage_raw):
                        # Xử lý an toàn cho UUID object
                        if hasattr(storage_raw, 'hex'):  # UUID object
                            storage_str = str(storage_raw)
                        else:
                            storage_str = str(storage_raw).replace('-', '/').replace(',', '/')
                        capacities = [cap.strip().upper() for cap in storage_str.split('/') if cap.strip()]
                        for cap_str in capacities:
                            try:
                                # Chỉ lấy số đầu tiên trong chuỗi để tránh giá trị quá lớn
                                numbers = re.findall(r'\d+', cap_str)
                                if numbers:
                                    capacity_gb = int(numbers[0])
                                    if 'TB' in cap_str: 
                                        capacity_gb *= 1024
                                    
                                    # Giới hạn capacity tối đa để tránh vượt quá int32
                                    if capacity_gb > 2000000:  # Giới hạn 2TB
                                        capacity_gb = 2000000
                                    
                                    existing_storage = await DeviceStorageRepository.get_by_device_info_id_and_capacity(db, device.id, capacity_gb)
                                    if not existing_storage:
                                        await DeviceStorageRepository.create(db, {'device_info_id': device.id, 'capacity': capacity_gb, 'user_id': user_id_for_import})
                            except (ValueError, IndexError):
                                errors.append(f"Dòng {index + 2}: Giá trị dung lượng không hợp lệ '{cap_str}' cho model '{model_name}'")
                
                success += 1

            except Exception as e:
                error += 1
                errors.append(f"Dòng {index + 2}: Lỗi khi xử lý model '{model_name}': {str(e)}")
                logging.error(traceback.format_exc())

        return ImportResult(
            total=total,
            success=success,
            error=error,
            errors=errors,
            updated_count=updated_count,
            created_count=created_count
        )

    @staticmethod
    async def import_user_devices(db: AsyncSession, file_content: bytes, user_id: uuid.UUID, background_tasks: BackgroundTasks) -> ImportResult:
        default_result = ImportResult(total=0, success=0, error=1, errors=[], updated_count=0, created_count=0)
        try:
            df = pd.read_excel(io.BytesIO(file_content))
            if df.empty:
                default_result.errors = ["File Excel không có dữ liệu"]
                return default_result
        except Exception as e:
            default_result.errors = [f"Lỗi đọc file Excel: {str(e)}"]
            return default_result

        # Chỉ kiểm tra các trường mã sản phẩm, tên thiết bị, màu sắc, dung lượng
        # Cho phép cả "product_code" và "Mã sản phẩm"
        product_code_column = None
        if "product_code" in df.columns:
            product_code_column = "product_code"
        elif "Mã sản phẩm" in df.columns:
            product_code_column = "Mã sản phẩm"

        total = len(df)
        success = error = updated_count = created_count = 0
        errors = []
        processed_devices = []

        user_device_repository = UserDeviceRepository

        for index, row in df.iterrows():
            try:
                current_row = index + 2  # Dòng thực tế trong Excel

                # Nếu tất cả các trường bắt buộc đều trống thì bỏ qua dòng này
                required_fields = []
                if product_code_column:
                    required_fields.append(product_code_column)
                # Không bắt buộc "Màu sắc" và "Dung lượng" nữa
                required_fields += ["Tên thiết bị"]
                if all((field not in row or pd.isna(row[field]) or not str(row[field]).strip()) for field in required_fields):
                    continue

                # Define product_code at the beginning of the loop
                product_code = None
                if product_code_column and not pd.isna(row.get(product_code_column)):
                    product_code = str(row.get(product_code_column)).strip() or None

                # Kiểm tra trùng lặp cho các dòng không có mã sản phẩm (màu sắc và dung lượng là tùy chọn)
                if not product_code:
                    device_name = safe_str(row.get("Tên thiết bị"))
                    color_name = safe_str(row.get("Màu sắc"))
                    dung_luong_str = safe_str(row.get("Dung lượng"))
                    price = float(row["Giá bán lẻ"]) if "Giá bán lẻ" in row and not pd.isna(row["Giá bán lẻ"]) else 0
                    device_type = safe_str(row.get("Loại máy"))
                    device_condition = safe_str(row.get("Tình trạng máy"))
                    battery_condition = safe_str(row.get("Tình trạng pin"))
                    warranty = safe_str(row.get("Bảo hành"))

                    if device_name:
                        device_info = await DeviceInfoRepository.get_by_model(db, device_name)
                        color = await ColorRepository.get_by_name(db, color_name, None) if color_name else None
                        match = re.search(r"(\d+)", dung_luong_str) if dung_luong_str else None
                        capacity = int(match.group(1)) if match else None
                        device_storage = await DeviceStorageRepository.get_by_device_info_id_and_capacity(db, device_info.id, capacity) if device_info and capacity is not None else None

                        if device_info:
                            duplicate_device = await user_device_repository.find_duplicate(
                                db, user_id, device_info.id, color.id if color else None, device_storage.id if device_storage else None,
                                price, None, device_type, device_condition, battery_condition, warranty
                            )
                            if duplicate_device:
                                error += 1
                                errors.append(f"Dòng {current_row}: Thiết bị đã tồn tại với mã sản phẩm {duplicate_device.product_code}")
                                continue
    
                # Không kiểm tra hợp lệ các trường còn lại
                price = float(row["Giá bán lẻ"]) if "Giá bán lẻ" in row and not pd.isna(row["Giá bán lẻ"]) else 0
                inventory = int(float(row["Tồn kho"])) if "Tồn kho" in row and not pd.isna(row["Tồn kho"]) else 0
                device_condition = str(row["Tình trạng máy"]).strip() if "Tình trạng máy" in row and not pd.isna(row["Tình trạng máy"]) else ""  # Default to "Cũ" if not provided
                device_type = str(row["Loại máy"]).strip() if "Loại máy" in row and not pd.isna(row["Loại máy"]) else ""  # Default to "Mới" if not provided

                if product_code:
                    existing_device = await user_device_repository.get_by_product_code_and_user_id(db, product_code, user_id)
                    if existing_device:
                        # Only include fields in the update if they have valid values
                        update_fields = {}
                        
                        # For device_condition, only update if we have a valid value
                        # Otherwise, preserve the existing value to avoid NOT NULL constraint violation
                        if device_condition is not None:
                            update_fields['device_condition'] = device_condition
                        
                        if device_type is not None:
                            update_fields['device_type'] = device_type
                            
                        battery_condition = safe_str(row.get("Tình trạng pin"))
                        if battery_condition is not None:
                            update_fields['battery_condition'] = battery_condition
                        
                        update_fields['price'] = price
                        update_fields['inventory'] = inventory
                        
                        # Handle wholesale_price field
                        wholesale_price = float(row["Giá bán buôn"]) if "Giá bán buôn" in row and not pd.isna(row["Giá bán buôn"]) else None
                        if wholesale_price is not None:
                            update_fields['wholesale_price'] = wholesale_price
                        
                        warranty = safe_str(row.get("Bảo hành"))
                        if warranty is not None:
                            update_fields['warranty'] = warranty
                            
                        notes = safe_str(row.get("Ghi chú"))
                        if notes is not None:
                            update_fields['notes'] = notes
                        
                        # Handle datetime fields from Excel as Vietnam time for updates
                        if "Ngày tạo" in row and not pd.isna(row["Ngày tạo"]):
                            created_at = convert_to_vietnam_time(row["Ngày tạo"])
                            if created_at is not None:
                                update_fields['created_at'] = created_at
                        if "Ngày cập nhật" in row and not pd.isna(row["Ngày cập nhật"]):
                            updated_at = convert_to_vietnam_time(row["Ngày cập nhật"])
                            if updated_at is not None:
                                update_fields['updated_at'] = updated_at
                        
                        update_data = UserDeviceUpdate(**update_fields)
                        updated_device = await user_device_repository.update(db, existing_device.id, update_data)
                        processed_devices.append(updated_device)
                        updated_count += 1
                        success += 1
                        
                    else:
                        # Logic to create a new device if product_code is provided but not found
                        has_error = False
                        if "Tên thiết bị" not in row or pd.isna(row["Tên thiết bị"]) or not str(row["Tên thiết bị"]).strip():
                            errors.append(f"Dòng {current_row}: Thiếu trường 'Tên thiết bị'")
                            has_error = True
                        
                        if has_error:
                            error += 1
                            continue

                        device_name = str(row["Tên thiết bị"]).strip()
                        device_info = await DeviceInfoRepository.get_by_model(db, device_name)
                        if not device_info:
                            errors.append(f"Dòng {current_row}: Không tìm thấy thông tin thiết bị '{device_name}'")
                            error += 1
                            continue

                        color_name = safe_str(row.get("Màu sắc"))
                        color = await ColorRepository.get_by_name(db, color_name, None) if color_name else None

                        dung_luong_str = safe_str(row.get("Dung lượng"))
                        match = re.search(r"(\d+)", dung_luong_str) if dung_luong_str else None
                        capacity = int(match.group(1)) if match else None
                        device_storage = await DeviceStorageRepository.get_by_device_info_id_and_capacity(db, device_info.id, capacity) if capacity is not None else None

                        battery_condition = safe_str(row.get("Tình trạng pin"))
                        warranty = safe_str(row.get("Bảo hành"))
                        notes = safe_str(row.get("Ghi chú"))
                        wholesale_price = float(row["Giá bán buôn"]) if "Giá bán buôn" in row and not pd.isna(row["Giá bán buôn"]) else None

                        created_at = convert_to_vietnam_time(row["Ngày tạo"]) if "Ngày tạo" in row and not pd.isna(row["Ngày tạo"]) else None
                        updated_at = convert_to_vietnam_time(row["Ngày cập nhật"]) if "Ngày cập nhật" in row and not pd.isna(row["Ngày cập nhật"]) else None

                        device_data = UserDeviceCreate(
                            user_id=user_id, device_info_id=device_info.id, color_id=color.id if color else None, 
                            device_storage_id=device_storage.id if device_storage else None, warranty=warranty, device_condition=device_condition,
                            device_type=device_type, battery_condition=battery_condition, price=price,
                            wholesale_price=wholesale_price, inventory=inventory, notes=notes,
                            created_at=created_at, updated_at=updated_at, product_code=product_code # Use provided product_code
                        )
                        try:
                            new_device = await user_device_repository.create(db, device_data)
                            processed_devices.append(new_device)
                            created_count += 1
                            success += 1
                        except Exception as create_error:
                            error += 1
                            errors.append(f"Dòng {current_row}: Lỗi khi tạo thiết bị mới: {str(create_error)}")
                else:
                    # Logic to create a new device (màu sắc và dung lượng là tùy chọn)
                    has_error = False
                    if "Tên thiết bị" not in row or pd.isna(row["Tên thiết bị"]) or not str(row["Tên thiết bị"]).strip():
                        errors.append(f"Dòng {current_row}: Thiếu trường 'Tên thiết bị'")
                        has_error = True
                    
                    if has_error:
                        error += 1
                        continue

                    device_name = str(row["Tên thiết bị"]).strip()
                    device_info = await DeviceInfoRepository.get_by_model(db, device_name)
                    if not device_info:
                        errors.append(f"Dòng {current_row}: Không tìm thấy thiết bị '{device_name}'")
                        error += 1
                        continue

                    color_name = safe_str(row.get("Màu sắc"))
                    color = await ColorRepository.get_by_name(db, color_name, None) if color_name else None

                    dung_luong_str = safe_str(row.get("Dung lượng"))
                    match = re.search(r"(\d+)", dung_luong_str) if dung_luong_str else None
                    capacity = int(match.group(1)) if match else None
                    device_storage = await DeviceStorageRepository.get_by_device_info_id_and_capacity(db, device_info.id, capacity) if capacity is not None else None

                    battery_condition = safe_str(row.get("Tình trạng pin"))
                    warranty = safe_str(row.get("Bảo hành"))
                    notes = safe_str(row.get("Ghi chú"))
                    wholesale_price = float(row["Giá bán buôn"]) if "Giá bán buôn" in row and not pd.isna(row["Giá bán buôn"]) else None

                    duplicate_device = await user_device_repository.find_duplicate(
                        db, user_id, device_info.id, color.id if color else None, device_storage.id if device_storage else None,
                        price, wholesale_price, device_type, device_condition, battery_condition, warranty
                    )
                    if duplicate_device:
                        errors.append(f"Dòng {current_row}: Thiết bị đã tồn tại với mã sản phẩm {duplicate_device.product_code}")
                        error += 1
                        continue

                    created_at = convert_to_vietnam_time(row["Ngày tạo"]) if "Ngày tạo" in row and not pd.isna(row["Ngày tạo"]) else None
                    updated_at = convert_to_vietnam_time(row["Ngày cập nhật"]) if "Ngày cập nhật" in row and not pd.isna(row["Ngày cập nhật"]) else None

                    device_data = UserDeviceCreate(
                        user_id=user_id, device_info_id=device_info.id, color_id=color.id if color else None, 
                        device_storage_id=device_storage.id if device_storage else None, warranty=warranty, device_condition=device_condition,
                        device_type=device_type, battery_condition=battery_condition, price=price,
                        wholesale_price=wholesale_price, inventory=inventory, notes=notes,
                        created_at=created_at, updated_at=updated_at, product_code=None
                    )
                    try:
                        new_device = await user_device_repository.create(db, device_data)
                        processed_devices.append(new_device)
                        created_count += 1
                        success += 1
                    except Exception as create_error:
                        error += 1
                        errors.append(f"Dòng {current_row}: Lỗi khi tạo thiết bị mới: {str(create_error)}")
            except Exception as e:
                error += 1
                errors.append(f"Dòng {index + 2}: {str(e)}")
                logging.error(traceback.format_exc())
                # Reset failed transaction state so subsequent iterations can continue
                try:
                    await db.rollback()
                except Exception:
                    pass

        if processed_devices:
            try:
                enriched_excel_content = await ExcelService._generate_chatbot_sync_excel(processed_devices)
                user = await UserRepository.get_by_id(db, user_id)
                if user:
                    background_tasks.add_task(ChatbotService.bulk_sync_products_from_file, enriched_excel_content, user)
            except Exception as sync_error:
                error += 1
                errors.append(f"Lỗi khi tạo file Excel hoặc đồng bộ hàng loạt: {str(sync_error)}")
                logging.error(traceback.format_exc())

        return ImportResult(
            total=total,
            success=success,
            error=error,
            errors=errors,
            updated_count=updated_count,
            created_count=created_count
        )

    @staticmethod
    async def export_user_devices(db: AsyncSession, user_devices: List[UserDeviceDetailRead]) -> bytes:
        data = []
        for device in user_devices:
            # Preserve original database datetime values exactly as they are
            # For timezone-aware datetimes, we need to convert to naive while preserving the exact time value
            if device.created_at:
                if device.created_at.tzinfo is not None:
                    # For timezone-aware datetimes, remove timezone info without adjusting the time value
                    created_at = device.created_at.replace(tzinfo=None)
                else:
                    # For naive datetimes, use as-is
                    created_at = device.created_at
            else:
                created_at = None
            
            if device.updated_at:
                if device.updated_at.tzinfo is not None:
                    # For timezone-aware datetimes, remove timezone info without adjusting the time value
                    updated_at = device.updated_at.replace(tzinfo=None)
                else:
                    # For naive datetimes, use as-is
                    updated_at = device.updated_at
            else:
                updated_at = None
            
            # Convert price values to numeric, handling None and string values
            price = None
            if device.price is not None:
                try:
                    price = float(device.price) if device.price != '' else None
                except (ValueError, TypeError):
                    price = None
            
            wholesale_price = None
            if device.wholesale_price is not None:
                try:
                    wholesale_price = float(device.wholesale_price) if device.wholesale_price != '' else None
                except (ValueError, TypeError):
                    wholesale_price = None
            
            inventory = None
            if device.inventory is not None:
                try:
                    inventory = int(device.inventory) if device.inventory != '' else None
                except (ValueError, TypeError):
                    inventory = None
            
            data.append({
                "Mã sản phẩm": device.product_code,
                "Tên thiết bị": device.device_info.model,
                "Màu sắc": device.color.name if device.color else None,
                "Dung lượng": f"{device.device_storage.capacity}GB" if device.device_storage and device.device_storage.capacity else None,
                "Loại máy": device.device_type,
                "Tình trạng máy": str(device.device_condition) if device.device_condition else None,
                "Tình trạng pin": str(device.battery_condition) if device.battery_condition else None,
                "Giá bán lẻ": price,
                "Giá bán buôn": wholesale_price,
                "Tồn kho": inventory,
                "Bảo hành": device.warranty,
                "Ghi chú": device.notes,
                "Ngày tạo": created_at,
                "Ngày cập nhật": updated_at
            })

        # Sort by device name (Tên thiết bị) A-Z
        df = pd.DataFrame(data)
        # Kiểm tra xem cột "Tên thiết bị" có tồn tại không trước khi sắp xếp
        if "Tên thiết bị" in df.columns:
            df = df.sort_values(by=["Tên thiết bị"])
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name="Thiết bị người dùng", index=False)
            workbook = writer.book
            worksheet = writer.sheets["Thiết bị người dùng"]

            header_format = workbook.add_format({'bold': True, 'font_size': 12, 'bg_color': '#D9EAD3', 'border': 1})
            content_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})

            last_col = len(df.columns)
            worksheet.write(0, last_col + 1, "HƯỚNG DẪN SỬ DỤNG MÃ SẢN PHẨM:", header_format)
            worksheet.write(1, last_col + 1, "1. Cột 'Mã sản phẩm' dùng để cập nhật thiết bị hiện có", content_format)
            worksheet.write(2, last_col + 1, "2. Khi import lại, hệ thống sẽ tìm thiết bị dựa vào mã sản phẩm", content_format)
            worksheet.write(3, last_col + 1, "3. Không thay đổi giá trị mã sản phẩm để tránh lỗi", content_format)
            worksheet.write(4, last_col + 1, "4. Nếu muốn tạo thiết bị mới, hãy để trống mã sản phẩm", content_format)
            worksheet.write(5, last_col + 1, "5. Khi cập nhật, chỉ một số trường được cập nhật", content_format)

            worksheet.set_column(last_col + 1, last_col + 1, 60)
            money_format = workbook.add_format({'num_format': '#.##0'})
            # Apply number format to price columns (Giá, Giá bán buôn, Tồn kho)
            worksheet.set_column('H:H', 15, money_format)  # Cột 'Giá'
            worksheet.set_column('I:I', 15, money_format)  # Cột 'Giá bán buôn'
            worksheet.set_column('J:J', 12, workbook.add_format({'num_format': '#.##0'}))  # Cột 'Tồn kho' (số nguyên)
            date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm:ss'})
            worksheet.set_column('L:M', 20, date_format)  # Cột 'Ngày tạo' và 'Ngày cập nhật'

            for i, col in enumerate(df.columns):
                col_width = max(df[col].astype(str).map(len).max(), len(col)) + 2
                # Set reasonable limits for column widths (min 10, max 50)
                col_width = max(min(col_width, 50), 10)
                worksheet.set_column(i, i, col_width)

        output.seek(0)
        return output.getvalue()

    @staticmethod
    async def _generate_chatbot_sync_excel(devices: List[UserDevice]) -> bytes:
        """
        Generates an Excel file in memory with the required format for ChatbotMobileStore API.
        """
        # This should match PRODUCT_COLUMNS_CONFIG from ChatbotMobileStore - using Vietnamese column names
        columns = [
            'Mã sản phẩm', 'Tên thiết bị', 'Màu sắc', 'Dung lượng', 'Bảo hành',
            'Tình trạng máy', 'Loại máy', 'Tình trạng pin', 'Giá bán lẻ', 'Giá bán buôn',
            'Tồn kho', 'Ghi chú'
        ]
        
        data = []
        for device in devices:
            device_info = device.device_info
            
            # Prepare a dictionary with all possible keys, defaulting to None
            device_data = {key: None for key in columns}
            
            # Convert price values to numeric, handling None and string values
            price = None
            if device.price is not None:
                try:
                    price = float(device.price) if device.price != '' else None
                except (ValueError, TypeError):
                    price = None
            
            wholesale_price = None
            if device.wholesale_price is not None:
                try:
                    wholesale_price = float(device.wholesale_price) if device.wholesale_price != '' else None
                except (ValueError, TypeError):
                    wholesale_price = None
            
            inventory = None
            if device.inventory is not None:
                try:
                    inventory = int(device.inventory) if device.inventory != '' else None
                except (ValueError, TypeError):
                    inventory = None
            
            device_data.update({
                'Mã sản phẩm': device.product_code,
                'Tên thiết bị': device_info.model if device_info else getattr(device, 'device_name', None),
                'Màu sắc': device.color.name if getattr(device, 'color', None) else None,
                'Dung lượng': f"{getattr(device, 'device_storage').capacity}GB" if getattr(device, 'device_storage', None) and getattr(device.device_storage, 'capacity', None) else None,
                'Bảo hành': getattr(device, 'warranty', None),
                'Tình trạng máy': getattr(device, 'device_condition', None),
                'Loại máy': getattr(device, 'device_type', None),
                'Tình trạng pin': getattr(device, 'battery_condition', None),
                'Giá bán lẻ': price,
                'Giá bán buôn': wholesale_price,
                'Tồn kho': inventory,
                'Ghi chú': getattr(device, 'notes', None),
            })
            
            # Additional device info is not needed for ChatbotMobileStore format

            data.append(device_data)

        df = pd.DataFrame(data, columns=columns)
        output = io.BytesIO()
        
        # The API expects a file with headers
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name="Products", index=False)
            
            # Format header
            workbook = writer.book
            worksheet = writer.sheets["Products"]
            header_format = workbook.add_format({
                'bold': True, 
                'font_size': 12, 
                'bg_color': '#D9EAD3', 
                'border': 1
            })
            
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                worksheet.set_column(col_num, col_num, len(str(value)) + 10)
        
        output.seek(0)
        return output.getvalue()

    @staticmethod
    async def export_product_components(db: AsyncSession, product_components: List[ProductComponentRead]) -> bytes:
        data = []
        # Kiểm tra xem product_components có phải là response object không
        if hasattr(product_components, 'data') and hasattr(product_components, 'total'):
            # Nếu là response object, lấy danh sách components từ .data
            actual_components = product_components.data
        elif isinstance(product_components, dict) and 'data' in product_components:
            # Nếu là dict, lấy danh sách components từ ['data']
            actual_components = product_components['data']
        else:
            # Nếu là danh sách trực tiếp
            actual_components = product_components
        
        for component in actual_components:
            # Lấy tên danh mục - kiểm tra xem component có phải là object không
            try:
                if hasattr(component, 'category'):
                    category_name = component.category
                elif isinstance(component, dict) and 'category' in component:
                    category_name = component['category']
                else:
                    category_name = ''
            except Exception as e:
                category_name = ''
            
            # Lấy key và values của thuộc tính
            property_display = None
            properties_value = None
            
            # Lấy properties từ component
            if hasattr(component, 'properties'):
                properties_value = component.properties
            elif isinstance(component, dict) and 'properties' in component:
                properties_value = component['properties']
            
            if properties_value:
                if isinstance(properties_value, str):
                    # Nếu là chuỗi JSON
                    try:
                        prop_data = json.loads(properties_value)
                        if isinstance(prop_data, list) and len(prop_data) > 0:
                            # Nếu là array, xử lý từng phần tử
                            property_parts = []
                            for prop in prop_data:
                                if isinstance(prop, dict):
                                    key = prop.get('key', '')
                                    values = prop.get('values', [])
                                    if key and values:
                                        values_str = '/'.join(str(v) for v in values)
                                        property_parts.append(f"{key}:{values_str}")
                            if property_parts:
                                property_display = ','.join(property_parts)
                        elif isinstance(prop_data, dict):
                            # Nếu là dict trực tiếp
                            key = prop_data.get('key', '')
                            values = prop_data.get('values', [])
                            if key and values:
                                values_str = '/'.join(str(v) for v in values)
                                property_display = f"{key}:{values_str}"
                    except (json.JSONDecodeError, TypeError):
                        property_display = str(properties_value)
                elif isinstance(component.properties, list) and len(component.properties) > 0:
                    # Nếu đã là list (đã được deserialize)
                    property_parts = []
                    for prop in component.properties:
                        if isinstance(prop, dict):
                            key = prop.get('key', '')
                            values = prop.get('values', [])
                            if key and values:
                                values_str = '/'.join(str(v) for v in values)
                                property_parts.append(f"{key}:{values_str}")
                    if property_parts:
                        property_display = ','.join(property_parts)
                elif isinstance(component.properties, dict):
                    # Nếu đã là dict (đã được deserialize)
                    key = component.properties.get('key', '')
                    values = component.properties.get('values', [])
                    if key and values:
                        values_str = '/'.join(str(v) for v in values)
                        property_display = f"{key}:{values_str}"
                else:
                    # Fallback: chuyển thành string
                    property_display = str(component.properties) if hasattr(component, 'properties') else ''
            
            # Helper function để lấy giá trị từ component (object hoặc dict)
            def get_value(key, default=''):
                if hasattr(component, key):
                    return getattr(component, key)
                elif isinstance(component, dict) and key in component:
                    return component[key]
                return default
            
            # Convert price values to numeric, handling None and string values
            amount = get_value('amount', 0)
            if amount is not None:
                try:
                    amount = float(amount) if amount != '' else 0
                except (ValueError, TypeError):
                    amount = 0
            
            wholesale_price = get_value('wholesale_price', 0)
            if wholesale_price is not None:
                try:
                    wholesale_price = float(wholesale_price) if wholesale_price != '' else 0
                except (ValueError, TypeError):
                    wholesale_price = 0
            
            stock = get_value('stock', 0)
            if stock is not None:
                try:
                    stock = int(stock) if stock != '' else 0
                except (ValueError, TypeError):
                    stock = 0
            
            # Clean description to remove line breaks
            description = get_value('description', '')
            if description:
                # Replace line breaks with spaces and clean up extra whitespace
                description = ' '.join(str(description).replace('\n', ' ').replace('\r', ' ').split())
            
            data.append({
                "Mã sản phẩm": get_value('product_code', ''),
                "Tên sản phẩm": get_value('product_name', ''),
                "Danh mục": category_name,
                "Thuộc tính": property_display or '',
                "Giá bán lẻ": amount,
                "Giá bán buôn": wholesale_price,
                "Thương hiệu": get_value('trademark', ''),
                "Bảo hành": get_value('guarantee', ''),
                "Tồn kho": get_value('stock', 0),
                "Mô tả": description,
                "Ảnh sản phẩm": get_value('product_photo', ''),
                "Link sản phẩm": get_value('product_link', ''),
            })
        
        # Sort by product name (Tên sản phẩm) A-Z
        df = pd.DataFrame(data)
        if "Tên sản phẩm" in df.columns:
            df = df.sort_values(by=["Tên sản phẩm"])
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name="Linh kiện", index=False)
            workbook = writer.book
            worksheet = writer.sheets["Linh kiện"]

            header_format = workbook.add_format({'bold': True, 'font_size': 12, 'bg_color': '#D9EAD3', 'border': 1})
            content_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})

            last_col = len(df.columns)
            worksheet.write(0, last_col + 1, "HƯỚNG DẪN SỬ DỤNG MÃ SẢN PHẨM:", header_format)
            worksheet.write(1, last_col + 1, "1. Cột 'Product code' dùng để cập nhật linh kiện hiện có", content_format)
            worksheet.write(2, last_col + 1, "2. Khi import lại, hệ thống sẽ tìm linh kiện dựa vào mã sản phẩm", content_format)
            worksheet.write(3, last_col + 1, "3. Không thay đổi giá trị mã sản phẩm để tránh lỗi", content_format)
            worksheet.write(4, last_col + 1, "4. Nếu muốn tạo linh kiện mới, hãy để trống mã sản phẩm", content_format)
            worksheet.write(5, last_col + 1, "5. Khi cập nhật, chỉ một số trường được cập nhật", content_format)

            worksheet.set_column(last_col + 1, last_col + 1, 60)
            money_format = workbook.add_format({'num_format': '#.##0'})
            worksheet.set_column('E:E', 15, money_format)  # Cột 'amount'
            worksheet.set_column('F:F', 15, money_format)  # Cột 'Giá bán buôn'
            worksheet.set_column('I:I', 15, money_format)  # Cột 'stock'

            for i, col in enumerate(df.columns):
                col_width = max(df[col].astype(str).map(len).max(), len(col)) + 2
                # Set reasonable limits for column widths (min 10, max 50)
                col_width = max(min(col_width, 50), 10)
                worksheet.set_column(i, i, col_width)

        output.seek(0)
        return output.getvalue()

    @staticmethod
    async def export_sample_product_components() -> bytes:
        """Tạo file Excel mẫu với các cột tiếng Việt để import linh kiện."""
        # Tạo dữ liệu mẫu với 3 dòng ví dụ
        sample_data = [
            {
                "Mã sản phẩm": "LK001",
                "Tên sản phẩm": "Màn hình iPhone 12",
                "Danh mục": "Màn hình",
                "Thuộc tính": "Kích thước:6.1 inch,Độ phân giải:2532x1170",
                "Giá bán lẻ": 2500000,
                "Giá bán buôn": 2200000,
                "Thương hiệu": "Apple",
                "Bảo hành": "6 tháng",
                "Tồn kho": 50,
                "Mô tả": "Màn hình nguyên bản cho iPhone 12",
                "Ảnh sản phẩm": "https://example.com/image1.jpg",
                "Link sản phẩm": "https://example.com/product1",
            },
            {
                "Mã sản phẩm": "LK002", 
                "Tên sản phẩm": "Pin Samsung Galaxy S21",
                "Danh mục": "Pin",
                "Thuộc tính": "Dung lượng:4000mAh,Loại:Li-ion",
                "Giá bán lẻ": 800000,
                "Giá bán buôn": 650000,
                "Thương hiệu": "Samsung",
                "Bảo hành": "12 tháng",
                "Tồn kho": 30,
                "Mô tả": "Pin chính hãng Samsung Galaxy S21",
                "Ảnh sản phẩm": "https://example.com/image2.jpg",
                "Link sản phẩm": "https://example.com/product2",
            },
            {
                "Mã sản phẩm": "LK003",
                "Tên sản phẩm": "Ốp lưng Xiaomi Mi 11",
                "Danh mục": "Phụ kiện",
                "Thuộc tính": "Chất liệu:Silicon,Màu sắc:Đen/Trắng/Xanh",
                "Giá bán lẻ": 150000,
                "Giá bán buôn": 100000,
                "Thương hiệu": "Xiaomi",
                "Bảo hành": "3 tháng",
                "Tồn kho": 100,
                "Mô tả": "Ốp lưng silicon bảo vệ Xiaomi Mi 11",
                "Ảnh sản phẩm": "https://example.com/image3.jpg",
                "Link sản phẩm": "https://example.com/product3",
            }
        ]
        
        # Tạo DataFrame và xuất Excel
        df = pd.DataFrame(sample_data)
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name="Mẫu linh kiện", index=False)
            workbook = writer.book
            worksheet = writer.sheets["Mẫu linh kiện"]

            # Định dạng header
            header_format = workbook.add_format({
                'bold': True, 
                'font_size': 12, 
                'bg_color': '#4CAF50',
                'font_color': 'white',
                'border': 1,
                'align': 'center',
                'valign': 'vcenter'
            })
            
            # Định dạng nội dung
            content_format = workbook.add_format({
                'text_wrap': True, 
                'valign': 'top',
                'border': 1
            })
            
            # Định dạng số tiền
            money_format = workbook.add_format({
                'num_format': '#,##0',
                'border': 1,
                'align': 'right'
            })

            # Áp dụng định dạng cho header
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)

            # Áp dụng định dạng cho nội dung
            for row in range(1, len(df) + 1):
                for col in range(len(df.columns)):
                    col_name = df.columns[col]
                    cell_value = df.iloc[row-1, col]
                    
                    # Định dạng đặc biệt cho cột tiền
                    if col_name in ['Giá bán lẻ', 'Giá bán buôn']:
                        worksheet.write(row, col, cell_value, money_format)
                    else:
                        worksheet.write(row, col, cell_value, content_format)

            # Tự động điều chỉnh độ rộng cột
            column_widths = {
                'Mã sản phẩm': 15,
                'Tên sản phẩm': 25,
                'Danh mục': 15,
                'Thuộc tính': 35,
                'Giá bán lẻ': 15,
                'Giá bán buôn': 15,
                'Thương hiệu': 15,
                'Bảo hành': 12,
                'Tồn kho': 10,
                'Mô tả': 30,
                'Ảnh sản phẩm': 25,
                'Link sản phẩm': 25,
            }
            
            for col_num, column in enumerate(df.columns):
                width = column_widths.get(column, 15)
                worksheet.set_column(col_num, col_num, width)

            # Thêm ghi chú hướng dẫn
            worksheet.write(len(df) + 2, 0, "HƯỚNG DẪN:", workbook.add_format({'bold': True, 'font_size': 14, 'font_color': 'red'}))
            worksheet.write(len(df) + 3, 0, "1. Mã sản phẩm: Mã duy nhất cho từng linh kiện")
            worksheet.write(len(df) + 4, 0, "2. Tên sản phẩm: Tên mô tả linh kiện (bắt buộc)")
            worksheet.write(len(df) + 5, 0, "3. Thuộc tính: Định dạng 'Tên:Giá trị1/Giá trị2,Tên khác:Giá trị'")
            worksheet.write(len(df) + 6, 0, "4. Giá bán lẻ/buôn: Nhập số, không có dấu phẩy")
            worksheet.write(len(df) + 7, 0, "5. Xóa các dòng mẫu này trước khi import")

        output.seek(0)
        return output.getvalue()

    @staticmethod
    async def _convert_to_mobile_store_format(file_content: bytes) -> bytes:
        """
        Chuyển đổi file Excel từ format linh kiện sang format sản phẩm cho ChatbotMobileStore.
        """
        try:
            # Đọc file Excel gốc
            df = pd.read_excel(io.BytesIO(file_content))
            
            if df.empty:
                return None
                
            # Tạo DataFrame mới với các cột theo format ChatbotMobileStore
            mobile_store_data = []
            
            for _, row in df.iterrows():
                # Bỏ qua dòng trống
                if pd.isna(row.get("Mã sản phẩm")) and pd.isna(row.get("Tên sản phẩm")):
                    continue
                    
                # Map dữ liệu từ format linh kiện sang format sản phẩm
                mobile_store_row = {
                    "Mã sản phẩm": str(row.get("Mã sản phẩm", "")).strip() if not pd.isna(row.get("Mã sản phẩm")) else "",
                    "Tên thiết bị": str(row.get("Tên sản phẩm", "")).strip() if not pd.isna(row.get("Tên sản phẩm")) else "",
                    "Màu sắc": "Không xác định",  # Default value vì linh kiện không có màu sắc
                    "Dung lượng": "N/A",  # Default value vì linh kiện không có dung lượng
                    "Bảo hành": str(row.get("Bảo hành", "")).strip() if not pd.isna(row.get("Bảo hành")) else "Không có",
                    "Tình trạng máy": "Mới",  # Default value
                    "Loại máy": str(row.get("Danh mục", "")).strip() if not pd.isna(row.get("Danh mục")) else "Linh kiện",
                    "Tình trạng pin": 100.0,  # Default value
                    "Giá bán lẻ": float(row.get("Giá bán lẻ", 0)) if not pd.isna(row.get("Giá bán lẻ")) else 0.0,
                    "Giá bán buôn": float(row.get("Giá bán buôn", 0)) if not pd.isna(row.get("Giá bán buôn")) else 0.0,
                    "Tồn kho": int(float(row.get("Tồn kho", 0))) if not pd.isna(row.get("Tồn kho")) else 0,
                    "Ghi chú": str(row.get("Mô tả", "")).strip() if not pd.isna(row.get("Mô tả")) else ""
                }
                
                mobile_store_data.append(mobile_store_row)
            
            if not mobile_store_data:
                return None
                
            # Tạo DataFrame mới
            mobile_store_df = pd.DataFrame(mobile_store_data)
            
            # Tạo file Excel mới
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                mobile_store_df.to_excel(writer, sheet_name="Products", index=False)
                
                # Format header
                workbook = writer.book
                worksheet = writer.sheets["Products"]
                header_format = workbook.add_format({
                    'bold': True, 
                    'font_size': 12, 
                    'bg_color': '#D9EAD3', 
                    'border': 1
                })
                
                for col_num, value in enumerate(mobile_store_df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                    worksheet.set_column(col_num, col_num, len(str(value)) + 10)
            
            output.seek(0)
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Lỗi khi chuyển đổi file Excel sang format ChatbotMobileStore: {str(e)}")
            return None

    @staticmethod
    async def import_product_components(db: AsyncSession, file_content: bytes, user_id: uuid.UUID, background_tasks: BackgroundTasks, current_user: "User") -> ImportResult:
        default_result = ImportResult(total=0, success=0, error=1, errors=[], updated_count=0, created_count=0)
        try:
            df = pd.read_excel(io.BytesIO(file_content))
            if df.empty:
                default_result.errors = ["File Excel không có dữ liệu"]
                return default_result
        except Exception as e:
            default_result.errors = [f"Lỗi đọc file Excel: {str(e)}"]
            return default_result

        # Kiểm tra các trường bắt buộc
        product_code_column = None
        if "Mã sản phẩm" in df.columns:
            product_code_column = "Mã sản phẩm"

        total = len(df)
        success = error = updated_count = created_count = 0
        errors = []
        
        for index, row in df.iterrows():
            try:
                current_row = index + 2  # Dòng thực tế trong Excel

                # Nếu tất cả các trường bắt buộc đều trống thì bỏ qua dòng này
                required_fields = []
                if product_code_column:
                    required_fields.append(product_code_column)
                required_fields += ["Tên sản phẩm"]
                if all((field not in row or pd.isna(row[field]) or not str(row[field]).strip()) for field in required_fields):
                    continue

                # Define product_code at the beginning of the loop
                product_code = None
                if product_code_column and not pd.isna(row.get(product_code_column)):
                    product_code = str(row.get(product_code_column)).strip() or None

                # Xử lý các trường dữ liệu
                product_name = safe_str(row.get("Tên sản phẩm"))
                amount = float(row["Giá bán lẻ"]) if "Giá bán lẻ" in row and not pd.isna(row["Giá bán lẻ"]) else 0
                wholesale_price = float(row["Giá bán buôn"]) if "Giá bán buôn" in row and not pd.isna(row["Giá bán buôn"]) else 0
                trademark = safe_str(row.get("Thương hiệu"))
                guarantee = safe_str(row.get("Bảo hành"))
                stock = int(float(row["Tồn kho"])) if "Tồn kho" in row and not pd.isna(row["Tồn kho"]) else 0
                description = safe_str(row.get("Mô tả"))
                product_photo = safe_str(row.get("Ảnh sản phẩm"))
                product_link = safe_str(row.get("Link sản phẩm"))
                
                # Xử lý danh mục
                raw_category = row.get("Danh mục")
                category_name = safe_str(raw_category)
                # Nếu category_name là None hoặc empty string, gán None
                # Nếu có giá trị, gán trực tiếp
                category = category_name if category_name else None
                
                # Xử lý thuộc tính
                property_key = safe_str(row.get("Thuộc tính"))
                properties_json_str = None
                if property_key and property_key.strip() != '0':
                    # Parse the string back into a list of dictionaries
                    property_parts = re.split(r',(?=\s*[^:]+:)', property_key)
                    parsed_properties = []
                    for part in property_parts:
                        part = part.strip()
                        match = re.match(r"([^:]+):(.+)", part)
                        if match:
                            key = match.group(1).strip()
                            values_str = match.group(2)
                            values = [v.strip() for v in values_str.split('/')]
                            parsed_properties.append({'key': key, 'values': values})
                        else:
                            # Fallback for older format or unexpected input
                            parsed_properties.append({'key': part, 'values': []})
                    
                    # Create a JSON string for the database
                    properties_json_str = json.dumps(parsed_properties)


                if product_code:
                    # Cập nhật linh kiện hiện có
                    existing_component = await ProductComponentRepository.get_by_product_code_and_user_id(db, product_code, user_id)
                    if existing_component:
                        # Chỉ cập nhật các trường có giá trị
                        update_fields = {}
                        
                        if product_name is not None:
                            update_fields['product_name'] = product_name
                        if amount is not None:
                            update_fields['amount'] = amount
                        if wholesale_price is not None:
                            update_fields['wholesale_price'] = wholesale_price
                        if trademark is not None:
                            update_fields['trademark'] = trademark
                        if guarantee is not None:
                            update_fields['guarantee'] = guarantee
                        if stock is not None:
                            update_fields['stock'] = stock
                        if description is not None:
                            update_fields['description'] = description
                        if product_photo is not None:
                            update_fields['product_photo'] = product_photo
                        if product_link is not None:
                            update_fields['product_link'] = product_link
                        if category is not None:
                            update_fields['category'] = category
                        if properties_json_str is not None:
                            update_fields['properties'] = properties_json_str
                        
                        if update_fields:
                            update_data = ProductComponentUpdate(**update_fields)
                            await ProductComponentService.update_product_component_with_sync(db, existing_component.id, update_data, current_user)
                            updated_count += 1
                            success += 1
                            
                            # Không cần sync từng component với ChatbotMobileStore khi import Excel
                            # Sẽ sync toàn bộ file Excel sau khi xử lý xong
                        else:
                            success += 1
                    else:
                        # Không tìm thấy linh kiện với mã sản phẩm này, tạo mới
                        # Không cần check trùng lặp vì đã có product_code
                        component_data = ProductComponentCreate(
                            product_code=product_code,  # Sử dụng mã sản phẩm từ Excel
                            product_name=product_name,
                            amount=amount,
                            wholesale_price=wholesale_price,
                            trademark=trademark,
                            guarantee=guarantee,
                            stock=stock,
                            description=description,
                            product_photo=product_photo,
                            product_link=product_link,
                            user_id=user_id,
                            category=category,
                            properties=properties_json_str,
                        )
                        try:
                            # Sử dụng create_without_duplicate_check khi có product_code
                            new_component = await ProductComponentRepository.create_without_duplicate_check(db, component_data)
                            created_count += 1
                            success += 1
                            
                            # Không cần sync từng component với ChatbotMobileStore khi import Excel
                            # Sẽ sync toàn bộ file Excel sau khi xử lý xong
                        except Exception as create_error:
                            error += 1
                            errors.append(f"Dòng {current_row}: Lỗi khi tạo linh kiện mới: {str(create_error)}")
                            # Ensure the session is usable for the next rows
                            try:
                                await db.rollback()
                            except Exception:
                                pass
                else:
                    # Tạo linh kiện mới (không có mã sản phẩm)
                    # Chỉ check trùng lặp khi không có product_code
                    duplicate_component = await ProductComponentRepository.find_duplicate(
                        db, user_id=user_id, product_name=product_name, trademark=trademark, category=category
                    )
                    if duplicate_component:
                        error += 1
                        errors.append(f"Dòng {current_row}: Linh kiện '{product_name}' đã tồn tại trong hệ thống với mã sản phẩm {duplicate_component.product_code}")
                        continue

                    component_data = ProductComponentCreate(
                        product_code=None,  # Mã sản phẩm sẽ được tạo tự động nếu không có
                        product_name=product_name,
                        amount=amount,
                        wholesale_price=wholesale_price,
                        trademark=trademark,
                        guarantee=guarantee,
                        stock=stock,
                        description=description,
                        product_photo=product_photo,
                        product_link=product_link,
                        user_id=user_id,
                        category=category,
                        properties=properties_json_str,
                    )
                    try:
                        # Sử dụng create bình thường (có check trùng lặp) khi không có product_code
                        new_component = await ProductComponentService.create_product_component_with_sync(db, component_data, current_user)
                        created_count += 1
                        success += 1
                        
                        # Không cần sync từng component với ChatbotMobileStore khi import Excel
                        # Sẽ sync toàn bộ file Excel sau khi xử lý xong
                    except Exception as create_error:
                        error += 1
                        errors.append(f"Dòng {current_row}: Lỗi khi tạo linh kiện mới: {str(create_error)}")
                        # Ensure the session is usable for the next rows
                        try:
                            await db.rollback()
                        except Exception:
                            pass
            except Exception as e:
                error += 1
                errors.append(f"Dòng {index + 2}: {str(e)}")
                logging.error(traceback.format_exc())

        # Sau khi xử lý xong tất cả các dòng Excel, sync với Elasticsearch
        if success > 0:
            try:
                # Lấy tất cả components của user để sync với ES
                all_components, _ = await ProductComponentRepository.get_all(db, skip=0, limit=100000, user_id=user_id)
                if all_components:
                    es_accessories = []
                    for comp in all_components:
                        es_accessories.append({
                            "accessory_code": comp.product_code,
                            "accessory_name": comp.product_name,
                            "gia_ban": comp.amount,
                            "gia_si": comp.wholesale_price,
                            "trademark": comp.trademark,
                            "guarantee": comp.guarantee,
                            "stock": comp.stock,
                            "description": comp.description,
                            "product_photo": comp.product_photo,
                            "product_link": comp.product_link,
                            "category": comp.category,
                        })
                    if es_accessories:
                        es_bulk_upsert_accessories.delay(
                            customer_id=str(user_id),
                            accessories=es_accessories,
                            with_embedding=True,
                            api_key=getattr(current_user, "gemini_api_key", None),
                        )
                        logger.info(f"Triggered ES bulk upsert for {len(es_accessories)} accessories of user {current_user.email}")
            except Exception as sync_error:
                logger.error(f"Lỗi khi trigger ES sync: {str(sync_error)}")
                # Không ảnh hưởng đến kết quả import chính

        return ImportResult(
            total=total,
            success=success,
            error=error,
            errors=errors,
            updated_count=updated_count,
            created_count=created_count
        )

    @staticmethod
    async def import_brands(db: AsyncSession, file_content: bytes, user_id: uuid.UUID, background_tasks: BackgroundTasks, current_user: "User") -> ImportResult:
        default_result = ImportResult(total=0, success=0, error=1, errors=[], updated_count=0, created_count=0)
        try:
            df = pd.read_excel(io.BytesIO(file_content))
            if df.empty:
                default_result.errors = ["File Excel không có dữ liệu"]
                return default_result
        except Exception as e:
            default_result.errors = [f"Lỗi đọc file Excel: {str(e)}"]
            return default_result

        # Kiểm tra các trường bắt buộc
        service_code_column = None
        if "Mã DV" in df.columns:
            service_code_column = "Mã DV"

        total = len(df)
        success = error = updated_count = created_count = 0
        errors = []

        service_cache: Dict[str, ServiceModel] = {}

        for index, row in df.iterrows():
            try:
                current_row = index + 2  # Dòng thực tế trong Excel

                # Nếu tất cả các trường dữ liệu chính đều trống thì bỏ qua dòng này (không báo lỗi)
                # Chỉ kiểm tra các trường có thể có dữ liệu, bỏ qua các trường như "Ngày tạo", "Ngày cập nhật"
                data_fields = ["Tên dịch vụ", "Loại sản phẩm", "Hãng điện thoại", "Bảo hành", "Loại máy", "Màu sắc", "Giá bán lẻ", "Giá bán buôn", "Ghi chú"]
                if all((field not in row or pd.isna(row[field]) or not str(row[field]).strip()) for field in data_fields):
                    continue

                service_code = None
                if service_code_column and not pd.isna(row.get(service_code_column)):
                    service_code = str(row.get(service_code_column)).strip() or None

                # Không kiểm tra hợp lệ các trường còn lại
                service_name = safe_str(row.get("Tên dịch vụ"))
                brand_name = safe_str(row.get("Loại sản phẩm"))

                device_brand_raw = row.get("Hãng điện thoại")
                device_brand = safe_str(device_brand_raw)
                device_brand_blank = ("Hãng điện thoại" in row and (pd.isna(device_brand_raw) or not str(device_brand_raw).strip()))

                warranty_raw = row.get("Bảo hành")
                warranty = safe_str(warranty_raw)
                warranty_blank = ("Bảo hành" in row and (pd.isna(warranty_raw) or not str(warranty_raw).strip()))

                device_type_raw = row.get("Loại máy")
                device_type = safe_str(device_type_raw)
                device_type_blank = ("Loại máy" in row and (pd.isna(device_type_raw) or not str(device_type_raw).strip()))

                color_raw = row.get("Màu sắc")
                color = safe_str(color_raw)
                color_blank = ("Màu sắc" in row and (pd.isna(color_raw) or not str(color_raw).strip()))

                price_raw = row.get("Giá bán lẻ")
                price = safe_str(price_raw)
                price_blank = ("Giá bán lẻ" in row and (pd.isna(price_raw) or not str(price_raw).strip()))

                wholesale_price_raw = row.get("Giá bán buôn")
                wholesale_price = safe_str(wholesale_price_raw)
                wholesale_price_blank = ("Giá bán buôn" in row and (pd.isna(wholesale_price_raw) or not str(wholesale_price_raw).strip()))

                note_raw = row.get("Ghi chú")
                note = safe_str(note_raw)
                note_blank = ("Ghi chú" in row and (pd.isna(note_raw) or not str(note_raw).strip()))

                # Logic to create or update a brand
                if not brand_name:
                    errors.append(f"Dòng {current_row}: Thiếu trường 'Loại sản phẩm'")
                    error += 1
                    continue

                brand_to_update = None
                if service_code:
                    brand_to_update = await BrandRepository.get_by_service_code(db, service_code, user_id)

                if brand_to_update:
                    # Update existing brand
                    update_fields = {}
                    if brand_name is not None: update_fields['name'] = brand_name
                    if warranty_blank: update_fields['warranty'] = 'Không có'
                    elif warranty is not None: update_fields['warranty'] = warranty
                    if device_type_blank: update_fields['device_type'] = None
                    elif device_type is not None: update_fields['device_type'] = device_type
                    if color_blank: update_fields['color'] = None
                    elif color is not None: update_fields['color'] = color
                    if price_blank: update_fields['price'] = '0'
                    elif price is not None: update_fields['price'] = price
                    if wholesale_price_blank: update_fields['wholesale_price'] = '0'
                    elif wholesale_price is not None: update_fields['wholesale_price'] = wholesale_price
                    if note_blank: update_fields['note'] = None
                    elif note is not None: update_fields['note'] = note

                    if device_brand is not None:
                        device_brand_obj = await DeviceBrandRepository.get_by_name_and_user(db, device_brand, user_id)
                        if not device_brand_obj:
                            device_brand_model = DeviceBrand(name=device_brand, user_id=user_id)
                            device_brand_obj = await DeviceBrandRepository.create(db, device_brand_model)
                        update_fields['device_brand_id'] = device_brand_obj.id if device_brand_obj else None
                    elif device_brand_blank:
                        update_fields['device_brand_id'] = None
                    
                    if update_fields:
                        update_data = BrandUpdate(**update_fields)
                        await BrandRepository.update(db, brand_to_update.id, update_data)
                    
                    updated_count += 1
                    success += 1
                else:
                    # Create new brand
                    service = service_cache.get(service_name)
                    if not service:
                        service = await ServiceRepository.get_by_name(db, service_name, user_id)
                        if not service:
                            # Tạo dịch vụ mới nếu không tồn tại
                            from app.dto.service_dto import ServiceCreate
                            from app.models.service import Service as ServiceModel
                            
                            service_data = ServiceCreate(name=service_name)
                            service_model = ServiceModel(**service_data.dict(), user_id=user_id)
                            service = await ServiceRepository.create(db, service_model)
                        service_cache[service_name] = service

                    device_brand_id = None
                    if device_brand:
                        device_brand_obj = await DeviceBrandRepository.get_by_name_and_user(db, device_brand, user_id)
                        if not device_brand_obj:
                            device_brand_model = DeviceBrand(name=device_brand, user_id=user_id)
                            device_brand_obj = await DeviceBrandRepository.create(db, device_brand_model)
                        device_brand_id = device_brand_obj.id

                    if not service_code: # Only check for duplicates if service_code is not provided
                        duplicate_brand = await BrandRepository.find_duplicate(
                            db, name=brand_name, service_id=service.id, device_brand_id=device_brand_id,
                            device_type=device_type, color=color, price=price, wholesale_price=wholesale_price, warranty=warranty,
                            user_id=user_id
                        )
                        if duplicate_brand:
                            # Cập nhật dịch vụ tồn tại thay vì báo lỗi
                            update_fields = {}
                            if brand_name is not None: update_fields['name'] = brand_name
                            if warranty_blank: update_fields['warranty'] = 'Không có'
                            elif warranty is not None: update_fields['warranty'] = warranty
                            if device_type_blank: update_fields['device_type'] = None
                            elif device_type is not None: update_fields['device_type'] = device_type
                            if color_blank: update_fields['color'] = None
                            elif color is not None: update_fields['color'] = color
                            if price_blank: update_fields['price'] = '0'
                            elif price is not None: update_fields['price'] = price
                            if wholesale_price_blank: update_fields['wholesale_price'] = '0'
                            elif wholesale_price is not None: update_fields['wholesale_price'] = wholesale_price
                            if note_blank: update_fields['note'] = None
                            elif note is not None: update_fields['note'] = note
                            if device_brand_id is not None: update_fields['device_brand_id'] = device_brand_id
                            elif device_brand_blank: update_fields['device_brand_id'] = None
                            
                            if update_fields:
                                update_data = BrandUpdate(**update_fields)
                                await BrandRepository.update(db, duplicate_brand.id, update_data)
                            
                            updated_count += 1
                            success += 1
                            continue

                    created_at = convert_to_vietnam_time(row["Ngày tạo"]) if "Ngày tạo" in row and not pd.isna(row["Ngày tạo"]) else None
                    updated_at = convert_to_vietnam_time(row["Ngày cập nhật"]) if "Ngày cập nhật" in row and not pd.isna(row["Ngày cập nhật"]) else None

                    brand_data = BrandCreate(
                        name=brand_name,
                        device_brand_id=device_brand_id,
                        warranty=warranty or "Không có",
                        service_id=service.id,
                        device_type=device_type,
                        color=color,
                        price=price,
                        wholesale_price=wholesale_price,
                        note=note,
                        created_at=created_at,
                        updated_at=updated_at,
                        service_code=service_code # Use provided code or None
                    )
                    
                    brand_model = Brand(**brand_data.dict(exclude_unset=True))
                    brand_model.service_code = service_code
                    new_brand = await BrandRepository.create(db, brand_model, user_id)
                    created_count += 1
                    success += 1
            except Exception as e:
                error += 1
                errors.append(f"Dòng {index + 2}: {str(e)}")
                logging.error(traceback.format_exc())

        # Đồng bộ toàn bộ file với ChatbotMobileStore sau khi import thành công
        if success > 0:
            try:
                background_tasks.add_task(ChatbotService.bulk_sync_services_from_file, file_content, current_user)
            except Exception as e:
                logger.error(f"Lỗi khi đồng bộ với ChatbotMobileStore: {e}")

        return ImportResult(
            total=total,
            success=success,
            error=error,
            errors=errors,
            updated_count=updated_count,
            created_count=created_count
        )

    @staticmethod
    async def export_device_infos(device_infos: List[DeviceInfoRead]) -> bytes:
        data = []
        for info in device_infos:
            if info.created_at and info.created_at.tzinfo:
                created_at = info.created_at.replace(tzinfo=None)
            else:
                created_at = info.created_at

            if info.updated_at and info.updated_at.tzinfo:
                updated_at = info.updated_at.replace(tzinfo=None)
            else:
                updated_at = info.updated_at

            # Xử lý danh sách vật liệu
            materials_str = ""
            if info.materials:
                materials_str = ", ".join([material.name for material in info.materials])
            
            # Xử lý danh sách dung lượng
            storage_str = ""
            if hasattr(info, 'device_storages') and info.device_storages:
                storage_str = ", ".join([f"{storage.capacity}GB" for storage in info.device_storages])
            
            # Xử lý danh sách màu sắc
            colors_str = ""
            if hasattr(info, 'device_colors') and info.device_colors:
                colors_str = ", ".join([color['color']['name'] for color in info.device_colors if color.get('color')])
            
            data.append({
                "ID": str(info.id),  # Thêm cột ID để hỗ trợ update
                "model": info.model,
                "ra mat": info.release_date,
                "man hinh": info.screen,
                "Chip / RAM": info.chip_ram,
                "camera_sau___truoc": info.camera,
                "Pin (mAh)": info.battery,
                "Ket noi / HĐH": info.connectivity_os,
                "Mau sac tieng anh": info.color_english,
                "Kich thuoc / Trong luong": info.dimensions_weight,
                "Vật liệu vỏ": materials_str,
                "Dung luong": storage_str,
                "Mau sac": colors_str,
                "cảm_biến_tính_năng_sức_khỏe": info.sensors_health_features,
                "brand": info.brand,
                "ngay_tao": created_at,
                "ngay_cap_nhat": updated_at
            })

        df = pd.DataFrame(data)
        if "model" in df.columns:
            df = df.sort_values(by=["model"])
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name="Thông tin thiết bị", index=False)
            workbook = writer.book
            worksheet = writer.sheets["Thông tin thiết bị"]

            header_format = workbook.add_format({'bold': True, 'font_size': 12, 'bg_color': '#D9EAD3', 'border': 1})
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)

            date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm:ss'})
            worksheet.set_column('O:P', 20, date_format)  # Cột 'ngay_tao' và 'ngay_cap_nhat'

            for i, col in enumerate(df.columns):
                col_width = max(df[col].astype(str).map(len).max(), len(col)) + 2
                col_width = max(min(col_width, 50), 10)
                worksheet.set_column(i, i, col_width)

        output.seek(0)
        return output.getvalue()

    @staticmethod
    async def export_brands(brands: List[BrandRead]) -> bytes:
        data = []
        for brand in brands:
            # Preserve original database datetime values exactly as they are
            # For timezone-aware datetimes, we need to convert to naive while preserving the exact time value
            if brand.created_at:
                if brand.created_at.tzinfo is not None:
                    # For timezone-aware datetimes, remove timezone info without adjusting the time value
                    created_at = brand.created_at.replace(tzinfo=None)
                else:
                    # For naive datetimes, use as-is
                    created_at = brand.created_at
            else:
                created_at = None
            
            if brand.updated_at:
                if brand.updated_at.tzinfo is not None:
                    # For timezone-aware datetimes, remove timezone info without adjusting the time value
                    updated_at = brand.updated_at.replace(tzinfo=None)
                else:
                    # For naive datetimes, use as-is
                    updated_at = brand.updated_at
            else:
                updated_at = None
            
            # Convert price values to numeric, handling None and string values
            price = None
            if brand.price is not None:
                try:
                    price = float(brand.price) if brand.price != '' else None
                except (ValueError, TypeError):
                    price = None
            
            wholesale_price = None
            if brand.wholesale_price is not None:
                try:
                    wholesale_price = float(brand.wholesale_price) if brand.wholesale_price != '' else None
                except (ValueError, TypeError):
                    wholesale_price = None
            
            data.append({
                "Mã DV": brand.service_code,
                "Tên dịch vụ": brand.service.name if brand.service else '',
                "Loại sản phẩm": brand.name,
                "Hãng điện thoại": brand.device_brand.name if brand.device_brand else '',
                "Loại máy": brand.device_type,
                "Màu sắc": brand.color,
                "Giá bán lẻ": price,
                "Giá bán buôn": wholesale_price,
                "Bảo hành": brand.warranty,
                "Ghi chú": brand.note,
                "Ngày tạo": created_at,
                "Ngày cập nhật": updated_at
            })

        # Sort by service name (Tên dịch vụ) A-Z
        df = pd.DataFrame(data)
        df = df.sort_values(by=["Tên dịch vụ"])
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name="Dịch vụ", index=False)
            workbook = writer.book
            worksheet = writer.sheets["Dịch vụ"]

            header_format = workbook.add_format({'bold': True, 'font_size': 12, 'bg_color': '#D9EAD3', 'border': 1})
            content_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})

            last_col = len(df.columns)
            worksheet.write(0, last_col + 1, "HƯỚNG DẪN SỬ DỤNG MÃ DỊCH VỤ:", header_format)
            worksheet.write(1, last_col + 1, "1. Cột 'Mã DV' dùng để cập nhật thương hiệu hiện có", content_format)
            worksheet.write(2, last_col + 1, "2. Khi import lại, hệ thống sẽ tìm thương hiệu dựa vào mã dịch vụ", content_format)
            worksheet.write(3, last_col + 1, "3. Không thay đổi giá trị mã dịch vụ để tránh lỗi", content_format)
            worksheet.write(4, last_col + 1, "4. Nếu muốn tạo thương hiệu mới, hãy để trống mã dịch vụ", content_format)
            worksheet.write(5, last_col + 1, "5. Khi cập nhật, chỉ một số trường được cập nhật", content_format)

            worksheet.set_column(last_col + 1, last_col + 1, 60)
            money_format = workbook.add_format({'num_format': '#.##0'})
            worksheet.set_column('G:H', 15, money_format)  # Cột 'Giá' và 'Giá bán buôn'
            date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm:ss'})
            worksheet.set_column('I:J', 20, date_format)

            for i, col in enumerate(df.columns):
                col_width = max(df[col].astype(str).map(len).max(), len(col)) + 2
                # Set reasonable limits for column widths (min 10, max 50)
                col_width = max(min(col_width, 50), 10)
                worksheet.set_column(i, i, col_width)

        output.seek(0)
        return output.getvalue()
    
    @staticmethod
    async def export_brands_template() -> bytes:
        """Export một file Excel mẫu cho brands với hướng dẫn sử dụng"""
        # Tạo dữ liệu mẫu
        sample_data = [
            {
                "Mã DV": "",  # Để trống để tạo mới
                "Tên dịch vụ": "Sửa chữa iPhone",
                "Loại sản phẩm": "Thay màn hình iPhone 14",
                "Hãng điện thoại": "Apple",
                "Loại máy": "iPhone 14",
                "Màu sắc": "Đen",
                "Giá bán lẻ": 2500000,
                "Giá bán buôn": 2200000,
                "Bảo hành": "3 tháng",
                "Ghi chú": "Màn hình zin chính hãng"
            },
            {
                "Mã DV": "",  # Để trống để tạo mới
                "Tên dịch vụ": "Sửa chữa Samsung",
                "Loại sản phẩm": "Thay pin Samsung S23",
                "Hãng điện thoại": "Samsung",
                "Loại máy": "Galaxy S23",
                "Màu sắc": "Trắng",
                "Giá bán lẻ": 800000,
                "Giá bán buôn": 700000,
                "Bảo hành": "6 tháng",
                "Ghi chú": "Pin chính hãng Samsung"
            }
        ]
        
        df = pd.DataFrame(sample_data)
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name="Mẫu dịch vụ", index=False)
            workbook = writer.book
            worksheet = writer.sheets["Mẫu dịch vụ"]
            
            # Format styles
            header_format = workbook.add_format({
                'bold': True, 
                'font_size': 12, 
                'bg_color': '#D9EAD3', 
                'border': 1,
                'text_wrap': True
            })
            content_format = workbook.add_format({
                'text_wrap': True, 
                'valign': 'top',
                'border': 1
            })
            instruction_format = workbook.add_format({
                'text_wrap': True, 
                'valign': 'top',
                'font_size': 10,
                'bg_color': '#FFF2CC'
            })
            
            # Apply header format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Apply content format to data rows
            for row_num in range(1, len(df) + 1):
                for col_num in range(len(df.columns)):
                    worksheet.write(row_num, col_num, df.iloc[row_num-1, col_num], content_format)
            
            # Add instructions
            instructions_col = len(df.columns) + 1
            instructions = [
                "HƯỚNG DẪN SỬ DỤNG FILE MẪU:",
                "",
                "1. THÊM MỚI DỊCH VỤ:",
                "   - Để trống cột 'Mã DV'",
                "   - Điền đầy đủ thông tin các cột khác",
                "",
                "2. CẬP NHẬT DỊCH VỤ CÓ SẴN:",
                "   - Điền 'Mã DV' của dịch vụ cần cập nhật",
                "   - Hệ thống sẽ tự động cập nhật theo mã này",
                "",
                "3. CÁC TRƯỜNG BẮT BUỘC:",
                "   - Tên dịch vụ",
                "   - Loại sản phẩm", 
                "   - Giá bán lẻ",
                "",
                "4. LƯU Ý:",
                "   - Giá phải là số (không có dấu phẩy, chấm)",
                "   - Hãng điện thoại phải đúng tên trong hệ thống",
                "   - Bảo hành nhập theo định dạng: '3 tháng', '1 năm'",
                "",
                "5. SAU KHI HOÀN THÀNH:",
                "   - Lưu file và import lại vào hệ thống",
                "   - Kiểm tra kết quả import để sửa lỗi nếu có"
            ]
            
            for i, instruction in enumerate(instructions):
                worksheet.write(i, instructions_col, instruction, instruction_format)
            
            # Set column widths
            worksheet.set_column(instructions_col, instructions_col, 50)
            
            # Format money columns
            money_format = workbook.add_format({'num_format': '#,##0', 'border': 1})
            worksheet.set_column('G:H', 15, money_format)  # Giá bán lẻ và Giá bán buôn
            
            # Auto-adjust other column widths
            for i, col in enumerate(df.columns):
                col_width = max(df[col].astype(str).map(len).max(), len(col)) + 3
                col_width = max(min(col_width, 25), 12)
                worksheet.set_column(i, i, col_width)
        
        output.seek(0)
        return output.getvalue()
    
    @staticmethod
    async def export_user_devices_template() -> bytes:
        """Export một file Excel mẫu cho user devices với hướng dẫn sử dụng"""
        # Tạo dữ liệu mẫu
        sample_data = [
            {
                "Mã sản phẩm": "",  # Để trống để tạo mới
                "Tên thiết bị": "iPhone 14 Pro Max",
                "Màu sắc": "Đen",
                "Dung lượng": "256GB",
                "Loại máy": "Mới",
                "Tình trạng máy": "Mới 100%",
                "Tình trạng pin": "100%",
                "Giá bán lẻ": 28990000,
                "Giá bán buôn": 26500000,
                "Tồn kho": 5,
                "Bảo hành": "12 tháng",
                "Ghi chú": "Hàng chính hãng VN/A"
            },
            {
                "Mã sản phẩm": "",  # Để trống để tạo mới  
                "Tên thiết bị": "Samsung Galaxy S23 Ultra",
                "Màu sắc": "Xanh",
                "Dung lượng": "512GB",
                "Loại máy": "Mới",
                "Tình trạng máy": "Mới 100%", 
                "Tình trạng pin": "100%",
                "Giá bán lẻ": 25990000,
                "Giá bán buôn": 23500000,
                "Tồn kho": 3,
                "Bảo hành": "12 tháng",
                "Ghi chú": "Bản Snapdragon"
            }
        ]
        
        df = pd.DataFrame(sample_data)
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name="Mẫu thiết bị", index=False)
            workbook = writer.book
            worksheet = writer.sheets["Mẫu thiết bị"]
            
            # Format styles
            header_format = workbook.add_format({
                'bold': True,
                'font_size': 12,
                'bg_color': '#D9EAD3',
                'border': 1,
                'text_wrap': True
            })
            content_format = workbook.add_format({
                'text_wrap': True,
                'valign': 'top', 
                'border': 1
            })
            instruction_format = workbook.add_format({
                'text_wrap': True,
                'valign': 'top',
                'font_size': 10,
                'bg_color': '#E1F5FE'
            })
            
            # Apply header format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Apply content format to data rows
            for row_num in range(1, len(df) + 1):
                for col_num in range(len(df.columns)):
                    worksheet.write(row_num, col_num, df.iloc[row_num-1, col_num], content_format)
            
            # Add instructions
            instructions_col = len(df.columns) + 1
            instructions = [
                "HƯỚNG DẪN SỬ DỤNG FILE MẪU:",
                "",
                "1. THÊM MỚI THIẾT BỊ:",
                "   - Để trống cột 'Mã sản phẩm'",
                "   - Điền đầy đủ thông tin các cột khác",
                "",
                "2. CẬP NHẬT THIẾT BỊ CÓ SẴN:",
                "   - Điền 'Mã sản phẩm' của thiết bị cần cập nhật",
                "   - Hệ thống sẽ tự động cập nhật theo mã này",
                "",
                "3. CÁC TRƯỜNG BẮT BUỘC:",
                "   - Tên thiết bị",
                "   - Giá bán lẻ",
                "   - Tồn kho",
                "",
                "4. ĐỊNH DẠNG DỮ LIỆU:",
                "   - Giá: Số nguyên (VD: 28990000)",
                "   - Tồn kho: Số nguyên (VD: 5)",
                "   - Dung lượng: Theo định dạng 'XXXGB' (VD: 256GB)",
                "   - Tình trạng pin: Theo % (VD: 100%, 85%)",
                "",
                "5. MÀU SẮC VÀ THIẾT BỊ:",
                "   - Tên thiết bị phải có trong hệ thống",
                "   - Màu sắc phải đúng với tên trong hệ thống",
                "   - Dung lượng phải khớp với thiết bị",
                "",
                "6. SAU KHI HOÀN THÀNH:",
                "   - Lưu file và import lại vào hệ thống",
                "   - Kiểm tra kết quả import để sửa lỗi nếu có"
            ]
            
            for i, instruction in enumerate(instructions):
                worksheet.write(i, instructions_col, instruction, instruction_format)
            
            # Set column widths
            worksheet.set_column(instructions_col, instructions_col, 55)
            
            # Format money columns  
            money_format = workbook.add_format({'num_format': '#,##0', 'border': 1})
            worksheet.set_column('H:I', 15, money_format)  # Giá bán lẻ và Giá bán buôn
            
            # Format quantity column
            number_format = workbook.add_format({'num_format': '0', 'border': 1})
            worksheet.set_column('J:J', 10, number_format)  # Tồn kho
            
            # Auto-adjust other column widths
            for i, col in enumerate(df.columns):
                col_width = max(df[col].astype(str).map(len).max(), len(col)) + 3
                col_width = max(min(col_width, 20), 12)
                worksheet.set_column(i, i, col_width)
        
        output.seek(0)
        return output.getvalue()