import unittest
from src.common.models.table_config import TableConfig, LayerType

class TestTableConfig(unittest.TestCase):
    
    def test_table_config_creation(self):
        config_data = {
            "identifier": "test.table",
            "layer": "raw",
            "source_type": "test",
            "schema": {"id": "long", "name": "string"},
            "business_keys": ["id"]
        }
        
        config = TableConfig.from_dict(config_data)
        self.assertEqual(config.identifier, "test.table")
        self.assertEqual(config.layer, LayerType.RAW)

if __name__ == '__main__':
    unittest.main()