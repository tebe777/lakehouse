import unittest
from datetime import datetime
from src.common.models.table_config import (
    TableConfig, ColumnConfig, DataType, ValidationRule, 
    ValidationSeverity, PartitionSpec, PartitionType, IcebergProperties
)


class TestTableConfig(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.valid_columns = [
            ColumnConfig(
                name="customer_id",
                data_type=DataType.LONG,
                nullable=False,
                description="Customer ID"
            ),
            ColumnConfig(
                name="first_name", 
                data_type=DataType.STRING,
                nullable=False,
                description="First name"
            ),
            ColumnConfig(
                name="email",
                data_type=DataType.STRING, 
                nullable=True,
                description="Email address"
            )
        ]
        
        self.valid_validation_rules = [
            ValidationRule(
                rule_type="not_null",
                column="customer_id",
                severity=ValidationSeverity.ERROR,
                description="Customer ID cannot be null"
            )
        ]
    
    def test_table_config_creation_valid(self):
        """Test creation of valid table configuration"""
        config = TableConfig(
            identifier="raw.test.customers",
            layer="raw",
            source_type="test",
            table_name="customers",
            description="Test customer table",
            columns=self.valid_columns,
            business_keys=["customer_id"],
            validation_rules=self.valid_validation_rules
        )
        
        self.assertEqual(config.identifier, "raw.test.customers")
        self.assertEqual(config.layer, "raw")
        self.assertEqual(config.source_type, "test")
        self.assertEqual(config.table_name, "customers")
        self.assertEqual(len(config.columns), 3)
        self.assertEqual(config.business_keys, ["customer_id"])
        self.assertIsInstance(config.created_at, datetime)
        
    def test_table_config_invalid_identifier(self):
        """Test that invalid identifier raises validation error"""
        with self.assertRaises(ValueError):
            TableConfig(
                identifier="invalid_identifier",  # Should be layer.source.table
                layer="raw",
                source_type="test", 
                table_name="customers",
                columns=self.valid_columns,
                business_keys=["customer_id"]
            )
    
    def test_table_config_invalid_business_key(self):
        """Test that business key not in columns raises validation error"""
        with self.assertRaises(ValueError):
            TableConfig(
                identifier="raw.test.customers",
                layer="raw",
                source_type="test",
                table_name="customers", 
                columns=self.valid_columns,
                business_keys=["non_existent_column"]  # This column doesn't exist
            )
    
    def test_table_config_with_partition_spec(self):
        """Test table config with partition specification"""
        partition_spec = PartitionSpec(
            columns=["created_date"],
            partition_type=PartitionType.MONTHLY
        )
        
        # Add created_date column
        columns_with_date = self.valid_columns + [
            ColumnConfig(
                name="created_date",
                data_type=DataType.DATE,
                nullable=False
            )
        ]
        
        config = TableConfig(
            identifier="raw.test.customers",
            layer="raw", 
            source_type="test",
            table_name="customers",
            columns=columns_with_date,
            business_keys=["customer_id"],
            partition_spec=partition_spec
        )
        
        self.assertIsNotNone(config.partition_spec)
        self.assertEqual(config.partition_spec.columns, ["created_date"])
        self.assertEqual(config.partition_spec.partition_type, PartitionType.MONTHLY)
    
    def test_table_config_with_scd(self):
        """Test table config with SCD Type 2 configuration"""
        config = TableConfig(
            identifier="raw.test.customers",
            layer="raw",
            source_type="test", 
            table_name="customers",
            columns=self.valid_columns,
            business_keys=["customer_id"],
            enable_scd=True,
            scd_columns=["first_name", "email"]
        )
        
        self.assertTrue(config.enable_scd)
        self.assertEqual(config.scd_columns, ["first_name", "email"])
    
    def test_table_config_invalid_scd_column(self):
        """Test that SCD column not in columns raises validation error"""
        with self.assertRaises(ValueError):
            TableConfig(
                identifier="raw.test.customers",
                layer="raw",
                source_type="test",
                table_name="customers",
                columns=self.valid_columns, 
                business_keys=["customer_id"],
                enable_scd=True,
                scd_columns=["non_existent_column"]  # This column doesn't exist
            )
    
    def test_iceberg_properties_default(self):
        """Test default Iceberg properties"""
        config = TableConfig(
            identifier="raw.test.customers",
            layer="raw",
            source_type="test",
            table_name="customers",
            columns=self.valid_columns,
            business_keys=["customer_id"]
        )
        
        self.assertIsInstance(config.iceberg_properties, IcebergProperties)
        self.assertEqual(config.iceberg_properties.format_version, "2")
        self.assertEqual(config.iceberg_properties.write_delete_mode, "merge-on-read")
    
    def test_iceberg_properties_to_spark_format(self):
        """Test conversion of Iceberg properties to Spark format"""
        config = TableConfig(
            identifier="raw.test.customers",
            layer="raw", 
            source_type="test",
            table_name="customers",
            columns=self.valid_columns,
            business_keys=["customer_id"]
        )
        
        spark_props = config.iceberg_properties.to_spark_properties()
        
        self.assertIn("format-version", spark_props)
        self.assertIn("write.delete.mode", spark_props)
        self.assertEqual(spark_props["format-version"], "2")
        self.assertEqual(spark_props["write.delete.mode"], "merge-on-read")
    
    def test_get_full_schema_with_technical_columns(self):
        """Test that full schema includes technical columns"""
        config = TableConfig(
            identifier="raw.test.customers",
            layer="raw",
            source_type="test",
            table_name="customers", 
            columns=self.valid_columns,
            business_keys=["customer_id"]
        )
        
        full_schema = config.get_full_schema()
        
        # Should have original columns + technical columns
        self.assertGreater(len(full_schema), len(config.columns))
        
        # Check for technical columns
        tech_column_names = [col.name for col in full_schema if col.name.startswith('ingested_at') or col.name.startswith('valid_') or col.name.startswith('is_current')]
        self.assertGreater(len(tech_column_names), 0)
    
    def test_get_iceberg_schema(self):
        """Test conversion to Iceberg schema format"""
        config = TableConfig(
            identifier="raw.test.customers",
            layer="raw",
            source_type="test", 
            table_name="customers",
            columns=self.valid_columns,
            business_keys=["customer_id"]
        )
        
        iceberg_schema = config.get_iceberg_schema()
        
        self.assertIsInstance(iceberg_schema, dict)
        self.assertIn("customer_id", iceberg_schema)
        self.assertIn("first_name", iceberg_schema)
        self.assertIn("email", iceberg_schema)
        
        # Check data types are converted
        self.assertEqual(iceberg_schema["customer_id"], "long")
        self.assertEqual(iceberg_schema["first_name"], "string")


if __name__ == '__main__':
    unittest.main()