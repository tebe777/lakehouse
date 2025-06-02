#!/usr/bin/env python3
"""
Quick integration test to verify all components are working correctly.
"""

import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_basic_imports():
    """Test basic imports"""
    print("Testing basic imports...")
    
    try:
        from src.common.utils.validator import DataValidator
        print("✅ DataValidator import successful")
    except ImportError as e:
        print(f"❌ DataValidator import failed: {e}")
        return False
    
    try:
        from src.common.utils.filename_parser import FileNameParser
        print("✅ FileNameParser import successful")
    except ImportError as e:
        print(f"❌ FileNameParser import failed: {e}")
        return False
    
    return True

def test_integration_imports():
    """Test integration imports"""
    print("\nTesting integration imports...")
    
    try:
        from src.common.utils.validation_integration import IntegratedDataValidator, FileProcessor
        print("✅ Integration utilities import successful")
    except ImportError as e:
        print(f"❌ Integration utilities import failed: {e}")
        return False
    
    try:
        from src.airflow.utils import create_data_validator, parse_filename
        print("✅ Airflow utils import successful")
    except ImportError as e:
        print(f"❌ Airflow utils import failed: {e}")
        return False
    
    return True

def test_functionality():
    """Test basic functionality"""
    print("\nTesting basic functionality...")
    
    try:
        from src.common.utils.validator import DataValidator
        from src.common.utils.filename_parser import FileNameParser
        
        # Test DataValidator
        rules = {"null_check": ["test_col"], "date_range": {}}
        validator = DataValidator(rules)
        print("✅ DataValidator creation successful")
        
        # Test FileNameParser
        test_filename = "AAA_TEST_20250501_W_20250502120515517.csv.ZIP"
        result = FileNameParser.parse(test_filename)
        print(f"✅ FileNameParser test successful: {result['prefix']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Functionality test failed: {e}")
        return False

def test_config_integration():
    """Test configuration integration"""
    print("\nTesting configuration integration...")
    
    try:
        from src.common.utils.config import TableConfig
        
        config = TableConfig(
            identifier="test.table",
            schema={"col1": "string"},
            key_columns=["col1"],
            validation={"null_check": ["col1"]}
        )
        print("✅ TableConfig creation successful")
        
        from src.common.utils.validation_integration import IntegratedDataValidator
        integrated_validator = IntegratedDataValidator(config)
        print("✅ IntegratedDataValidator creation successful")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration integration test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("=== Integration Test Suite ===\n")
    
    tests = [
        test_basic_imports,
        test_integration_imports,
        test_functionality,
        test_config_integration
    ]
    
    all_passed = True
    for test in tests:
        if not test():
            all_passed = False
    
    print(f"\n=== Test Results ===")
    if all_passed:
        print("🎉 All integration tests passed! The solution is properly integrated.")
    else:
        print("❌ Some tests failed. Please check the errors above.")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 