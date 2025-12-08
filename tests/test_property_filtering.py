import json
import pytest
from unittest.mock import Mock, AsyncMock

# Test data mẫu
SAMPLE_PROPERTIES = [
    {"key": "KIỂU", "values": ["Đầu thẳng"]},
    {"key": "MÀU SẮC", "values": ["Đồng"]}
]

SAMPLE_PROPERTIES_JSON = json.dumps(SAMPLE_PROPERTIES, ensure_ascii=False)

def test_property_filtering_logic():
    """Test logic tìm kiếm property với dữ liệu mẫu."""
    
    # Test case 1: Tìm kiếm property "KIỂU" với value "Đầu thẳng"
    test_property = "LOẠI"
    test_value = "số 3"
    
    # Tạo pattern tìm kiếm
    key_pattern = f'%"key": "{test_property}"%'
    value_pattern = f'%"values": ["{test_value}"%'
    
    # Kiểm tra xem pattern có match với dữ liệu không
    properties_str = SAMPLE_PROPERTIES_JSON
    
    # Test key pattern
    assert f'"key": "{test_property}"' in properties_str
    
    # Test value pattern  
    assert f'"values": ["{test_value}"' in properties_str
    
    print(f"✅ Test passed: Property '{test_property}' with value '{test_value}' found in properties")
    
    # Test case 2: Tìm kiếm property "MÀU SẮC" với value "Đồng"
    test_property2 = "MÀU SẮC"
    test_value2 = "Đồng"
    
    key_pattern2 = f'%"key": "{test_property2}"%'
    value_pattern2 = f'%"values": ["{test_value2}"%'
    
    # Kiểm tra xem pattern có match với dữ liệu không
    assert f'"key": "{test_property2}"' in properties_str
    assert f'"values": ["{test_value2}"' in properties_str
    
    print(f"✅ Test passed: Property '{test_property2}' with value '{test_value2}' found in properties")
    
    # Test case 3: Tìm kiếm property không tồn tại
    non_existent_property = "KHÔNG_TỒN_TẠI"
    non_existent_value = "Giá trị không tồn tại"
    
    key_pattern3 = f'%"key": "{non_existent_property}"%'
    value_pattern3 = f'%"values": ["{non_existent_value}"%'
    
    # Kiểm tra xem pattern có match với dữ liệu không
    assert f'"key": "{non_existent_property}"' not in properties_str
    assert f'"values": ["{non_existent_value}"' not in properties_str
    
    print(f"✅ Test passed: Non-existent property '{non_existent_property}' not found in properties")

def test_json_structure():
    """Test cấu trúc JSON của properties."""
    
    # Parse JSON để kiểm tra cấu trúc
    parsed_properties = json.loads(SAMPLE_PROPERTIES_JSON)
    
    # Kiểm tra cấu trúc
    assert isinstance(parsed_properties, list)
    assert len(parsed_properties) == 2
    
    # Kiểm tra phần tử đầu tiên
    first_property = parsed_properties[0]
    assert "key" in first_property
    assert "values" in first_property
    assert first_property["key"] == "KIỂU"
    assert isinstance(first_property["values"], list)
    assert "Đầu thẳng" in first_property["values"]
    
    # Kiểm tra phần tử thứ hai
    second_property = parsed_properties[1]
    assert "key" in second_property
    assert "values" in second_property
    assert second_property["key"] == "MÀU SẮC"
    assert isinstance(second_property["values"], list)
    assert "Đồng" in second_property["values"]
    
    print("✅ Test passed: JSON structure is correct")

if __name__ == "__main__":
    print("🧪 Running property filtering tests...")
    print(f"Sample properties: {SAMPLE_PROPERTIES_JSON}")
    print()
    
    test_property_filtering_logic()
    print()
    test_json_structure()
    
    print("\n🎉 All tests passed!") 