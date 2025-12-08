from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    # For Python < 3.9, zoneinfo can be installed via `pip install backports.zoneinfo`
    from backports.zoneinfo import ZoneInfo

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

def get_vn_now():
    """Returns the current datetime in Vietnam's timezone."""
    return datetime.now(VN_TZ) 