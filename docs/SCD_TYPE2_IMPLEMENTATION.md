# SCD Type 2 Implementation for Raw Layer

## Overview

This document describes the Slowly Changing Dimension (SCD) Type 2 implementation for the raw layer in our lakehouse ETL framework. SCD Type 2 maintains historical data by creating new records for changes while preserving the complete history of data modifications.

## Architecture

### Core Components

1. **SCDProcessor** (`src/transformation/scd_processor.py`)
   - Handles SCD Type 2 logic
   - Manages change detection and history preservation
   - Supports configurable SCD settings

2. **RawDataLoader** (`src/ingestion/raw_loader.py`)
   - Integrates SCD processing into raw layer ingestion
   - Handles both initial loads and incremental updates
   - Provides detailed metrics and monitoring

3. **ConfigurationManager** (`src/catalog/config_manager.py`)
   - Manages SCD configuration from files
   - Supports environment-specific overrides
   - Converts simplified config to full SCDConfig objects

## SCD Type 2 Features

### Technical Columns

The following technical columns are automatically added to SCD-enabled tables:

| Column | Type | Description |
|--------|------|-------------|
| `valid_from` | TIMESTAMP | Start of validity period |
| `valid_to` | TIMESTAMP | End of validity period (NULL for current records) |
| `is_current` | BOOLEAN | Flag indicating current record |
| `row_hash` | STRING | Hash of tracked columns for change detection |
| `created_date` | TIMESTAMP | Record creation timestamp |
| `updated_date` | TIMESTAMP | Record last update timestamp |
| `batch_id` | STRING | Batch identifier for tracking |
| `source_system` | STRING | Source system identifier |
| `is_deleted` | BOOLEAN | Soft delete flag (optional) |
| `version` | LONG | Record version number |

### Change Detection

The system detects changes by:
1. Computing hash values of tracked columns
2. Comparing hashes between source and target data
3. Identifying new, changed, and unchanged records
4. Managing validity periods automatically

### Processing Logic

1. **New Records**: Added with `is_current = true` and `valid_from = processing_time`
2. **Changed Records**: 
   - Current record updated with `valid_to = processing_time` and `is_current = false`
   - New record inserted with `is_current = true` and `valid_from = processing_time`
3. **Unchanged Records**: No modifications made
4. **Deleted Records** (if enabled): Marked with `is_deleted = true`

## Configuration

### Base Table Configuration

```json
{
  "identifier": "raw.crm.customers",
  "layer": "raw",
  "source_type": "crm",
  "table_name": "customers",
  "enable_scd": true,
  "business_keys": ["customer_id"],
  "scd_columns": ["first_name", "last_name", "email", "phone", "status", "credit_limit"]
}
```

### Environment-Specific Overrides

#### Development Environment (`configs/tables/environments/dev/crm_customers.json`)
- Relaxed validation rules (WARNING severity)
- Daily partitioning for faster testing
- Disabled business key validation
- No soft deletes

#### Production Environment (`configs/tables/environments/prod/crm_customers.json`)
- Strict validation rules (CRITICAL/ERROR severity)
- Monthly partitioning for performance
- Enabled business key validation
- Soft deletes enabled
- Enhanced quality checks

### SCD Configuration Options

```json
{
  "scd_config": {
    "enable_scd": true,
    "business_keys": ["customer_id"],
    "tracked_columns": ["first_name", "last_name", "email", "phone", "status", "credit_limit"],
    "scd_type": 2,
    "effective_date_column": "valid_from",
    "end_date_column": "valid_to",
    "current_flag_column": "is_current",
    "hash_column": "row_hash",
    "merge_on_business_key": true,
    "handle_deletes": true,
    "preserve_history": true,
    "auto_end_date": true,
    "validate_business_keys": true,
    "enable_change_tracking": true,
    "track_column_changes": true,
    "enable_soft_deletes": true
  }
}
```

## Usage Examples

### Basic SCD Loading

```python
from catalog.config_manager import ConfigurationManager
from ingestion.raw_loader import RawDataLoader

# Initialize components
config_manager = ConfigurationManager("configs")
table_config = config_manager.get_table_config("raw.crm.customers")
raw_loader = RawDataLoader(spark, table_config)

# Load data with SCD
result = raw_loader.load_data(
    df=source_dataframe,
    target_table="local.raw_crm_customers",
    write_mode="append",
    enable_scd=True,
    batch_id="batch_001"
)

print(f"Records processed: {result['records_loaded']}")
print(f"New records: {result['new_records']}")
print(f"Updated records: {result['updated_records']}")
```

### Environment-Specific Loading

```python
# Load development configuration
dev_config = config_manager.get_table_config("raw.crm.customers", environment="dev")

# Load production configuration
prod_config = config_manager.get_table_config("raw.crm.customers", environment="prod")
```

### Querying Historical Data

```sql
-- Get current records only
SELECT customer_id, first_name, last_name, email, status
FROM raw_crm_customers
WHERE is_current = true;

-- Get all historical versions
SELECT customer_id, first_name, last_name, email, status,
       valid_from, valid_to, is_current
FROM raw_crm_customers
ORDER BY customer_id, valid_from;

-- Get data as of specific date
SELECT customer_id, first_name, last_name, email, status
FROM raw_crm_customers
WHERE valid_from <= '2024-01-15 10:00:00'
  AND (valid_to IS NULL OR valid_to > '2024-01-15 10:00:00');

-- Track changes for specific customer
SELECT customer_id, first_name, last_name, email, status,
       valid_from, valid_to, version
FROM raw_crm_customers
WHERE customer_id = 1
ORDER BY valid_from;
```

## Performance Considerations

### Partitioning Strategy

- **Development**: Daily partitioning for faster iteration
- **Production**: Monthly partitioning for optimal query performance
- Partition on `created_date` or `valid_from` depending on query patterns

### Indexing Recommendations

1. **Business Keys**: Index on business key columns for fast lookups
2. **Temporal Columns**: Index on `valid_from`, `valid_to`, `is_current`
3. **Composite Index**: Consider composite index on `(business_key, is_current)`

### Optimization Tips

1. **Batch Size**: Configure appropriate batch sizes for your data volume
2. **Compaction**: Regular table compaction for Iceberg tables
3. **Pruning**: Use partition pruning in queries
4. **Caching**: Cache frequently accessed current data

## Monitoring and Metrics

### Available Metrics

- `records_loaded`: Total records processed
- `new_records`: Count of new records added
- `updated_records`: Count of records with changes
- `unchanged_records`: Count of records without changes
- `processing_time`: Time taken for SCD processing
- `batch_id`: Unique identifier for tracking

### Quality Checks

- Business key validation
- Data completeness checks
- Anomaly detection (production)
- Data freshness monitoring
- Change rate monitoring

## Best Practices

### Configuration Management

1. **Environment Separation**: Use environment-specific overrides
2. **Version Control**: Store all configurations in version control
3. **Validation**: Implement configuration validation
4. **Documentation**: Document all configuration changes

### Data Quality

1. **Validation Rules**: Define appropriate validation rules per environment
2. **Monitoring**: Implement comprehensive monitoring
3. **Alerting**: Set up alerts for data quality issues
4. **Testing**: Test SCD logic with various scenarios

### Operations

1. **Batch Tracking**: Use meaningful batch IDs
2. **Error Handling**: Implement robust error handling
3. **Rollback**: Plan for rollback scenarios
4. **Monitoring**: Monitor processing metrics

## Troubleshooting

### Common Issues

1. **Duplicate Business Keys**: Check business key validation settings
2. **Performance Issues**: Review partitioning and indexing strategy
3. **Memory Issues**: Adjust Spark configuration for large datasets
4. **Configuration Errors**: Validate SCD configuration format

### Debugging

1. **Enable Debug Logging**: Set log level to DEBUG
2. **Check Metrics**: Review processing metrics for anomalies
3. **Validate Data**: Check source data quality
4. **Test Incrementally**: Test with small datasets first

## Migration Guide

### Enabling SCD on Existing Tables

1. **Backup Data**: Create backup of existing data
2. **Add Technical Columns**: Alter table to add SCD columns
3. **Backfill History**: Populate technical columns for existing data
4. **Update Configuration**: Enable SCD in table configuration
5. **Test Processing**: Validate SCD processing with test data

### Configuration Migration

1. **Update Table Config**: Add SCD configuration
2. **Environment Overrides**: Create environment-specific configs
3. **Validation Rules**: Update validation rules as needed
4. **Pipeline Config**: Update pipeline configurations

## Examples

See `examples/scd_raw_layer_example.py` for a comprehensive demonstration of SCD Type 2 processing in the raw layer.

## Related Documentation

- [Configuration Management](CONFIG_MANAGEMENT.md)
- [Data Quality Framework](DATA_QUALITY.md)
- [Raw Layer Architecture](RAW_LAYER.md)
- [Performance Tuning](PERFORMANCE_TUNING.md) 