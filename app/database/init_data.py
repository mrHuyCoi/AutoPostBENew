# import asyncio
# from sqlalchemy.ext.asyncio import AsyncSession

# from app.database.database import get_db
# from app.dto.role_dto import RoleCreate
# from app.dto.permission_dto import PermissionCreate
# from app.services.role_service import RoleService
# from app.services.permission_service import PermissionService


# async def init_permissions(db: AsyncSession):
#     """Khởi tạo các quyền cơ bản."""
#     permissions = [
#         PermissionCreate(name="Xem vai trò", code="view_roles", description="Xem danh sách và chi tiết vai trò"),
#         PermissionCreate(name="Xem quyền", code="view_permissions", description="Xem danh sách và chi tiết quyền"),
#         PermissionCreate(name="Xem gói đăng ký", code="view_subscriptions", description="Xem danh sách và chi tiết gói đăng ký"),
#         PermissionCreate(name="Quản lý video", code="manage_videos", description="Tải lên, xem, sửa, xóa video"),
#         PermissionCreate(name="Lên lịch đăng bài", code="schedule_posts", description="Lên lịch đăng bài tự động"),
#         PermissionCreate(name="Quản lý tài khoản MXH", code="manage_social_accounts", description="Thêm, xem, sửa, xóa tài khoản mạng xã hội"),
#         PermissionCreate(name="Sinh nội dung AI", code="generate_ai_content", description="Sử dụng AI để sinh nội dung"),
#     ]
    
#     for permission_data in permissions:
#         try:
#             await PermissionService.create_permission(db, permission_data)
#             print(f"Đã tạo quyền: {permission_data.name}")
#         except Exception as e:
#             print(f"Lỗi khi tạo quyền {permission_data.name}: {str(e)}")


# async def init_roles(db: AsyncSession):
#     """Khởi tạo các vai trò cơ bản."""
#     roles = [
#         RoleCreate(name="basic", description="Người dùng cơ bản"),
#         RoleCreate(name="premium", description="Người dùng premium"),
#         RoleCreate(name="professional", description="Người dùng chuyên nghiệp"),
#     ]
    
#     for role_data in roles:
#         try:
#             await RoleService.create_role(db, role_data)
#             print(f"Đã tạo vai trò: {role_data.name}")
#         except Exception as e:
#             print(f"Lỗi khi tạo vai trò {role_data.name}: {str(e)}")


# async def assign_permissions_to_roles(db: AsyncSession):
#     """Gán quyền cho các vai trò."""
#     # Lấy tất cả các quyền
#     permissions = await PermissionService.get_all_permissions(db)
#     permission_dict = {p.code: p for p in permissions}
    
#     # Lấy tất cả các vai trò
#     roles = await RoleService.get_all_roles(db)
#     role_dict = {r.name: r for r in roles}
    
#     # Gán quyền cho vai trò basic
#     if "basic" in role_dict:
#         basic_role = role_dict["basic"]
#         basic_permissions = [
#             permission_dict["view_roles"],
#             permission_dict["manage_videos"],
#             permission_dict["manage_social_accounts"],
#         ]
#         basic_role.permissions = basic_permissions
#         await db.commit()
#         print("Đã gán quyền cho vai trò basic")
    
#     # Gán quyền cho vai trò premium
#     if "premium" in role_dict:
#         premium_role = role_dict["premium"]
#         premium_permissions = [
#             permission_dict["view_roles"],
#             permission_dict["manage_videos"],
#             permission_dict["manage_social_accounts"],
#             permission_dict["schedule_posts"],
#         ]
#         premium_role.permissions = premium_permissions
#         await db.commit()
#         print("Đã gán quyền cho vai trò premium")
    
#     # Gán q