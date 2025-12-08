from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_, and_, func, cast, delete
from typing import Optional, List
import uuid
import re
import json
from sqlalchemy.dialects.postgresql import JSONB

from app.models.product_component import ProductComponent
from app.dto.product_component_dto import ProductComponentCreate, ProductComponentUpdate
from app.dto.product_component_dto import ProductComponentRead
from app.exceptions.api_exceptions import NotFoundException
from datetime import date


class ProductComponentRepository:
    """Repository xử lý các thao tác CRUD cho đối tượng ProductComponent."""

    @staticmethod
    async def create(db: AsyncSession, data: ProductComponentCreate) -> ProductComponent:
        """Tạo một thành phần sản phẩm mới."""
        # # Kiểm tra trùng lặp trước khi tạo
        # existing_component = await ProductComponentRepository.find_duplicate(
        #     db=db,
        #     user_id=data.user_id,
        #     product_name=data.product_name,
        #     trademark=data.trademark,
        #     category=data.category,
        #     product_code=data.product_code
        # )
        # if existing_component:
        #     if existing_component.product_code == data.product_code:
        #         raise ValueError(
        #             f"Linh kiện với mã sản phẩm '{data.product_code}' đã tồn tại cho người dùng này"
        #         )
        #     else:
        #         raise ValueError(
        #             f"Linh kiện '{data.product_name}' với các thuộc tính tương tự đã tồn tại với mã sản phẩm {existing_component.product_code}"
        #         )

        # Tạo đối tượng ProductComponent
        db_product_component = ProductComponent(
            product_code=data.product_code,
            product_name=data.product_name,
            amount=data.amount,
            wholesale_price=data.wholesale_price,
            trademark=data.trademark,
            guarantee=data.guarantee,
            stock=data.stock,
            description=data.description,
            product_photo=data.product_photo,
            product_link=data.product_link,
            user_id=data.user_id,
            category=data.category
        )

        # Handle properties as JSON string if provided
        if data.properties is not None:
            # Validate that properties is a valid JSON string
            try:
                import json
                if data.properties:  # Only validate if not empty
                    json.loads(data.properties)
                db_product_component.properties = data.properties
            except json.JSONDecodeError:
                raise ValueError("Properties must be a valid JSON string")

        # Lưu vào database
        db.add(db_product_component)
        await db.commit()
        await db.refresh(db_product_component)

        return db_product_component

    @staticmethod
    async def create_without_duplicate_check(db: AsyncSession, data: ProductComponentCreate) -> ProductComponent:
        """Tạo một thành phần sản phẩm mới mà không check trùng lặp (dùng cho import Excel khi có product_code)."""
        # Tạo đối tượng ProductComponent
        db_product_component = ProductComponent(
            product_code=data.product_code,
            product_name=data.product_name,
            amount=data.amount,
            wholesale_price=data.wholesale_price,
            trademark=data.trademark,
            guarantee=data.guarantee,
            stock=data.stock,
            description=data.description,
            product_photo=data.product_photo,
            product_link=data.product_link,
            user_id=data.user_id,
            category=data.category
        )

        # Handle properties as JSON string if provided
        if data.properties is not None:
            # Validate that properties is a valid JSON string
            try:
                import json
                if data.properties:  # Only validate if not empty
                    json.loads(data.properties)
                db_product_component.properties = data.properties
            except json.JSONDecodeError:
                raise ValueError("Properties must be a valid JSON string")

        # Lưu vào database
        db.add(db_product_component)
        await db.commit()
        await db.refresh(db_product_component)

        return db_product_component

    @staticmethod
    async def get_by_id(db: AsyncSession, product_component_id: uuid.UUID) -> Optional[ProductComponent]:
        """
        Lấy thông tin thành phần sản phẩm bằng ID.

        Args:
            db: Database session
            product_component_id: ID của thành phần sản phẩm

        Returns:
            Đối tượng ProductComponent hoặc None nếu không tìm thấy
        """
        result = await db.execute(
            select(ProductComponent)
            .where(ProductComponent.id == product_component_id, ProductComponent.trashed_at.is_(None))
        )
        return result.scalars().first()

    @staticmethod
    async def get_by_id_for_user(db: AsyncSession, product_component_id: uuid.UUID, user_id: uuid.UUID) -> Optional[ProductComponent]:
        """
        Lấy thông tin thành phần sản phẩm bằng ID, giới hạn theo user_id.
        """
        result = await db.execute(
            select(ProductComponent)
            .where(
                ProductComponent.id == product_component_id,
                ProductComponent.user_id == user_id,
                ProductComponent.trashed_at.is_(None)
            )
        )
        return result.scalars().first()

    @staticmethod
    async def get_by_product_code(db: AsyncSession, product_code: str) -> Optional[ProductComponent]:
        """
        Lấy thông tin thành phần sản phẩm bằng mã sản phẩm.

        Args:
            db: Database session
            product_code: Mã sản phẩm

        Returns:
            Đối tượng ProductComponent hoặc None nếu không tìm thấy
        """
        result = await db.execute(
            select(ProductComponent)
            .where(ProductComponent.product_code == product_code, ProductComponent.trashed_at.is_(None))
        )
        return result.scalars().first()

    @staticmethod
    async def get_by_product_code_and_user_id(db: AsyncSession, product_code: str, user_id: uuid.UUID) -> Optional[ProductComponent]:
        """Lấy thông tin thành phần sản phẩm bằng mã sản phẩm và user_id."""
        result = await db.execute(
            select(ProductComponent)
            .where(
                (ProductComponent.product_code == product_code) &
                (ProductComponent.user_id == user_id) &
                (ProductComponent.trashed_at.is_(None))
            )
        )
        return result.scalars().first()

    @staticmethod
    async def find_duplicate(
        db: AsyncSession,
        user_id: uuid.UUID,
        product_name: str,
        trademark: Optional[str],
        category: Optional[str],
        product_code: Optional[str] = None
    ) -> Optional[ProductComponent]:
        """Tìm sản phẩm trùng lặp dựa trên user_id, tên, thương hiệu, danh mục, và product_code."""
        # Nếu có product_code, kiểm tra trùng lặp theo user_id và product_code trước
        if product_code:
            existing_by_code = await ProductComponentRepository.get_by_product_code_and_user_id(db, product_code, user_id)
            if existing_by_code:
                return existing_by_code
        
        # Kiểm tra trùng lặp theo tên, thương hiệu, danh mục (chỉ trong cùng user_id)
        query = select(ProductComponent).where(
            (ProductComponent.user_id == user_id) &
            (ProductComponent.product_name == product_name) &
            (ProductComponent.trashed_at.is_(None))
        )
        if trademark:
            query = query.where(ProductComponent.trademark == trademark)
        if category:
            query = query.where(ProductComponent.category == category)
        result = await db.execute(query)
        return result.scalars().first()

    @staticmethod
    async def update(db: AsyncSession, product_component_id: uuid.UUID, data: ProductComponentUpdate) -> Optional[ProductComponent]:
        """Cập nhật thông tin thành phần sản phẩm."""
        result = await db.execute(
            select(ProductComponent).where(
                ProductComponent.id == product_component_id,
                ProductComponent.trashed_at.is_(None)
            )
        )
        db_product_component = result.scalars().first()
        if not db_product_component:
            return None

        # Nếu cập nhật mã sản phẩm, kiểm tra xem mã mới đã tồn tại cho user này chưa
        if data.product_code is not None and data.product_code != db_product_component.product_code:
            existing_product = await ProductComponentRepository.get_by_product_code_and_user_id(db, data.product_code, db_product_component.user_id)
            if existing_product:
                raise ValueError(f"Mã sản phẩm '{data.product_code}' đã tồn tại cho người dùng này")

        # Cập nhật các trường nếu được cung cấp
        if data.product_code is not None:
            db_product_component.product_code = data.product_code
        if data.product_name is not None:
            db_product_component.product_name = data.product_name
        if data.amount is not None:
            db_product_component.amount = data.amount
        if data.wholesale_price is not None:
            db_product_component.wholesale_price = data.wholesale_price
        if data.trademark is not None:
            db_product_component.trademark = data.trademark
        if data.guarantee is not None:
            db_product_component.guarantee = data.guarantee
        if data.stock is not None:
            db_product_component.stock = data.stock
        if data.description is not None:
            db_product_component.description = data.description
        if data.product_photo is not None:
            db_product_component.product_photo = data.product_photo
        if data.product_link is not None:
            db_product_component.product_link = data.product_link
        if data.category is not None:
            db_product_component.category = data.category
        if data.properties is not None:
            # Validate JSON string
            try:
                import json
                if data.properties:
                    json.loads(data.properties)
                db_product_component.properties = data.properties
            except json.JSONDecodeError:
                raise ValueError("Properties must be a valid JSON string")

        await db.commit()
        await db.refresh(db_product_component)
        return db_product_component

    @staticmethod
    async def delete(db: AsyncSession, product_component_id: uuid.UUID) -> bool:
        """Xóa thành phần sản phẩm theo ID."""
        result = await db.execute(select(ProductComponent).where(ProductComponent.id == product_component_id))
        product = result.scalars().first()
        if not product:
            return False
        await db.delete(product)
        await db.commit()
        return True

    @staticmethod
    async def bulk_delete(db: AsyncSession, product_component_ids: list[uuid.UUID]) -> int:
        """Xóa hàng loạt thành phần sản phẩm theo danh sách ID một cách hiệu quả."""
        if not product_component_ids:
            return 0

        delete_stmt = delete(ProductComponent).where(ProductComponent.id.in_(product_component_ids))
        result = await db.execute(delete_stmt)
        await db.commit()
        return result.rowcount

    @staticmethod
    async def delete_all_by_user_id(db: AsyncSession, user_id: uuid.UUID) -> int:
        """Xóa tất cả thành phần sản phẩm của một người dùng."""
        delete_stmt = delete(ProductComponent).where(ProductComponent.user_id == user_id)
        result = await db.execute(delete_stmt)
        await db.commit()
        return result.rowcount

    @staticmethod
    async def get_all(
        db: AsyncSession,
        skip: int = 0,
        limit: int = 100,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        user_id: Optional[uuid.UUID] = None,
        filters: Optional[dict] = None
    ) -> tuple[List[ProductComponent], int]:
        """
        Lấy danh sách thành phần sản phẩm với phân trang, tìm kiếm và sắp xếp.

        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa
            search: Từ khóa tìm kiếm (tìm trong tên sản phẩm và mã sản phẩm)
            sort_by: Trường để sắp xếp
            sort_order: Hướng sắp xếp (asc hoặc desc)
            user_id: ID của người dùng (nếu được cung cấp, chỉ lấy sản phẩm của người dùng này)

        Returns:
            Tuple (danh sách ProductComponent, tổng số bản ghi)
        """
        # Query để đếm tổng số bản ghi
        count_query = select(func.count(ProductComponent.id)).where(ProductComponent.trashed_at.is_(None))
        if user_id:
            count_query = count_query.where(ProductComponent.user_id == user_id)

        # Thêm điều kiện tìm kiếm vào count query
        if search:
            search_filter = or_(
                ProductComponent.product_name.ilike(f"%{search}%"),
                ProductComponent.product_code.ilike(f"%{search}%")
            )
            count_query = count_query.where(search_filter)

        # Query để lấy dữ liệu
        query = select(ProductComponent).where(ProductComponent.trashed_at.is_(None))

        # Lọc theo user_id nếu được cung cấp
        if user_id:
            query = query.where(ProductComponent.user_id == user_id)

        # Thêm điều kiện tìm kiếm
        if search:
            search_filter = or_(
                ProductComponent.product_name.ilike(f"%{search}%"),
                ProductComponent.product_code.ilike(f"%{search}%")
            )
            query = query.where(search_filter)

        # Thêm các filter khác
        if filters:
            # Filter theo category
            if filters.get('category'):
                query = query.where(ProductComponent.category == filters['category'])
                count_query = count_query.where(ProductComponent.category == filters['category'])

            # Filter theo trademark
            if filters.get('trademark'):
                query = query.where(ProductComponent.trademark == filters['trademark'])
                count_query = count_query.where(ProductComponent.trademark == filters['trademark'])

            # Filter theo các property riêng biệt (property_COMBO, property_RAM, etc.)
            for filter_key, filter_value in filters.items():
                if filter_key.startswith('property_') and filter_value:
                    prop_key = filter_key.replace('property_', '')
                    # Ưu tiên dùng JSONB JSONPath để tìm đúng key và value trong cùng object
                    prop_json = cast(ProductComponent.properties, JSONB)
                    json_path = f'$[*] ? (@.key == "{prop_key}" && @.values ? (@ == "{str(filter_value)}"))'
                    jsonb_condition = func.jsonb_path_exists(prop_json, json_path)

                    # Fallback dùng regex/ilike nếu JSON không parse được (dữ liệu không phải JSON hợp lệ)
                    def to_json_escaped(s: str) -> str:
                        dumped = json.dumps(s, ensure_ascii=True)
                        return dumped[1:-1]
                    key_literal = re.escape(prop_key)
                    key_escaped = re.escape(to_json_escaped(prop_key))
                    val_str = str(filter_value)
                    val_literal = re.escape(val_str)
                    val_escaped = re.escape(to_json_escaped(val_str))
                    pattern_literal = rf'"key"\s*:\s*"{key_literal}"[^}}]*"values"\s*:\s*\[[^\]]*"{val_literal}"'
                    pattern_escaped = rf'"key"\s*:\s*"{key_escaped}"[^}}]*"values"\s*:\s*\[[^\]]*"{val_escaped}"'
                    regex_pattern = rf'(?:{pattern_literal})|(?:{pattern_escaped})'
                    regex_match = ProductComponent.properties.op('~*')(regex_pattern)
                    like_key = ProductComponent.properties.ilike(f'%"key": "{prop_key}"%')
                    like_val_literal = ProductComponent.properties.ilike(f'%"values": ["{val_str}"%')
                    like_val_escaped = ProductComponent.properties.ilike(f'%"values": ["{to_json_escaped(val_str)}"%')
                    fallback_match = and_(like_key, or_(like_val_literal, like_val_escaped))

                    property_condition = or_(jsonb_condition, regex_match, fallback_match)

                    query = query.where(property_condition)
                    count_query = count_query.where(property_condition)

            # Filter theo khoảng giá
            if filters.get('price_range_min') is not None:
                min_price = float(filters['price_range_min'])
                query = query.where(ProductComponent.amount >= min_price)
                count_query = count_query.where(ProductComponent.amount >= min_price)

            if filters.get('price_range_max') is not None:
                max_price = float(filters['price_range_max'])
                query = query.where(ProductComponent.amount <= max_price)
                count_query = count_query.where(ProductComponent.amount <= max_price)

        # Tính tổng sau khi áp dụng tất cả filter
        total_result = await db.execute(count_query)
        total = total_result.scalar()

        # Thêm sắp xếp nếu được chỉ định
        if sort_by and hasattr(ProductComponent, sort_by):
            sort_column = getattr(ProductComponent, sort_by)
            if sort_order and sort_order.lower() == 'desc':
                query = query.order_by(sort_column.desc())
            else:
                query = query.order_by(sort_column.asc())

        query = query.offset(skip).limit(limit)
        result = await db.execute(query)
        data = result.scalars().all()

        return data, total

    @staticmethod
    async def get_deleted_today_by_user_id(db: AsyncSession, user_id: uuid.UUID) -> List[ProductComponent]:
        """
        Lấy danh sách các ProductComponent đã bị xóa mềm trong ngày hôm nay của user.
        """
        today = date.today()
        result = await db.execute(
            select(ProductComponent)
            .where(
                ProductComponent.user_id == user_id,
                ProductComponent.trashed_at.isnot(None),
                func.date(ProductComponent.trashed_at) == today,
            )
            .order_by(ProductComponent.trashed_at.desc())
        )
        return result.scalars().all()

    @staticmethod
    async def get_distinct_categories(db: AsyncSession, user_id: uuid.UUID) -> List[str]:
        """Lấy danh sách danh mục không trùng lặp của người dùng."""
        query = select(ProductComponent.category).where(
            ProductComponent.user_id == user_id,
            ProductComponent.category.isnot(None),
            ProductComponent.category != '',
            ProductComponent.trashed_at.is_(None)
        ).distinct()

        result = await db.execute(query)
        categories = result.scalars().all()
        return [cat for cat in categories if cat]

    @staticmethod
    async def get_distinct_trademarks(db: AsyncSession, user_id: uuid.UUID) -> List[str]:
        """Lấy danh sách thương hiệu không trùng lặp của người dùng."""
        query = select(ProductComponent.trademark).where(
            ProductComponent.user_id == user_id,
            ProductComponent.trademark.isnot(None),
            ProductComponent.trademark != '',
            ProductComponent.trashed_at.is_(None)
        ).distinct()

        result = await db.execute(query)
        trademarks = result.scalars().all()
        return [trademark for trademark in trademarks if trademark]

    @staticmethod
    async def get_distinct_properties(db: AsyncSession, user_id: uuid.UUID) -> List[str]:
        """Lấy danh sách thuộc tính không trùng lặp của người dùng."""
        query = select(ProductComponent.properties).where(
            ProductComponent.user_id == user_id,
            ProductComponent.properties.isnot(None),
            ProductComponent.properties != '',
            ProductComponent.trashed_at.is_(None)
        ).distinct()

        result = await db.execute(query)
        properties = result.scalars().all()
        return [prop for prop in properties if prop]
    