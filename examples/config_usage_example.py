#!/usr/bin/env python3
"""
Example script demonstrating the configuration-as-code approach
for the lakehouse ETL framework.

This script shows how to:
1. Load configurations for different environments
2. Use environment-specific overrides
3. Access table and pipeline configurations
"""

import os
import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from catalog.config_manager import ConfigurationManager


def main():
    """Demonstrate configuration-as-code usage."""
    
    # Initialize configuration manager
    config_dir = Path(__file__).parent.parent / "configs"
    config_manager = ConfigurationManager(str(config_dir))
    
    print("=== Configuration-as-Code Demo ===\n")
    
    # Example 1: Load base table configuration
    print("1. Loading base table configuration:")
    base_config = config_manager.get_table_config("raw.crm.customers")
    if base_config:
        print(f"   Table: {base_config.table_name}")
        print(f"   Description: {base_config.description}")
        print(f"   Columns: {len(base_config.columns)}")
        print(f"   Validation Rules: {len(base_config.validation_rules)}")
        print(f"   Partition Type: {base_config.partition_spec.get('partition_type', 'N/A')}")
    else:
        print("   Base configuration not found!")
    
    print()
    
    # Example 2: Load development environment configuration
    print("2. Loading development environment configuration:")
    dev_config = config_manager.get_table_config("raw.crm.customers", environment="dev")
    if dev_config:
        print(f"   Table: {dev_config.table_name}")
        print(f"   Description: {dev_config.description}")
        print(f"   Partition Type: {dev_config.partition_spec.get('partition_type', 'N/A')}")
        
        # Show relaxed validation rules
        critical_rules = [r for r in dev_config.validation_rules if r.get('severity') == 'critical']
        error_rules = [r for r in dev_config.validation_rules if r.get('severity') == 'error']
        warning_rules = [r for r in dev_config.validation_rules if r.get('severity') == 'warning']
        info_rules = [r for r in dev_config.validation_rules if r.get('severity') == 'info']
        
        print(f"   Validation Rules - Critical: {len(critical_rules)}, Error: {len(error_rules)}, Warning: {len(warning_rules)}, Info: {len(info_rules)}")
    else:
        print("   Development configuration not found!")
    
    print()
    
    # Example 3: Load production environment configuration
    print("3. Loading production environment configuration:")
    prod_config = config_manager.get_table_config("raw.crm.customers", environment="prod")
    if prod_config:
        print(f"   Table: {prod_config.table_name}")
        print(f"   Description: {prod_config.description}")
        print(f"   Partition Type: {prod_config.partition_spec.get('partition_type', 'N/A')}")
        
        # Show strict validation rules
        critical_rules = [r for r in prod_config.validation_rules if r.get('severity') == 'critical']
        error_rules = [r for r in prod_config.validation_rules if r.get('severity') == 'error']
        warning_rules = [r for r in prod_config.validation_rules if r.get('severity') == 'warning']
        info_rules = [r for r in prod_config.validation_rules if r.get('severity') == 'info']
        
        print(f"   Validation Rules - Critical: {len(critical_rules)}, Error: {len(error_rules)}, Warning: {len(warning_rules)}, Info: {len(info_rules)}")
    else:
        print("   Production configuration not found!")
    
    print()
    
    # Example 4: Load pipeline configurations
    print("4. Loading pipeline configurations:")
    
    # Base pipeline configuration
    base_pipeline = config_manager.get_pipeline_config("crm_ingestion")
    if base_pipeline:
        print(f"   Base Pipeline: {base_pipeline.get('name')}")
        print(f"   Schedule: {base_pipeline.get('schedule', {}).get('cron')}")
        print(f"   Batch Size: {base_pipeline.get('processing', {}).get('batch_size')}")
        print(f"   Monitoring: {base_pipeline.get('monitoring', {}).get('enabled')}")
    
    print()
    
    # Development pipeline configuration
    dev_pipeline = config_manager.get_pipeline_config("crm_ingestion", environment="dev")
    if dev_pipeline:
        print(f"   Dev Pipeline: {dev_pipeline.get('name')}")
        print(f"   Schedule: {dev_pipeline.get('schedule', {}).get('cron')}")
        print(f"   Batch Size: {dev_pipeline.get('processing', {}).get('batch_size')}")
        print(f"   Monitoring: {dev_pipeline.get('monitoring', {}).get('enabled')}")
        print(f"   Target Location: {dev_pipeline.get('target', {}).get('location')}")
    
    print()
    
    # Production pipeline configuration
    prod_pipeline = config_manager.get_pipeline_config("crm_ingestion", environment="prod")
    if prod_pipeline:
        print(f"   Prod Pipeline: {prod_pipeline.get('name')}")
        print(f"   Schedule: {prod_pipeline.get('schedule', {}).get('cron')}")
        print(f"   Batch Size: {prod_pipeline.get('processing', {}).get('batch_size')}")
        print(f"   Monitoring: {prod_pipeline.get('monitoring', {}).get('enabled')}")
        print(f"   Target Location: {prod_pipeline.get('target', {}).get('location')}")
        print(f"   Alert Count: {len(prod_pipeline.get('monitoring', {}).get('alerts', []))}")
    
    print()
    
    # Example 5: Show environment-specific differences
    print("5. Environment-specific differences:")
    print("   Development vs Production:")
    
    if dev_config and prod_config:
        dev_partition = dev_config.partition_spec.get('partition_type')
        prod_partition = prod_config.partition_spec.get('partition_type')
        print(f"   - Partitioning: Dev={dev_partition}, Prod={prod_partition}")
        
        dev_critical = len([r for r in dev_config.validation_rules if r.get('severity') == 'critical'])
        prod_critical = len([r for r in prod_config.validation_rules if r.get('severity') == 'critical'])
        print(f"   - Critical Rules: Dev={dev_critical}, Prod={prod_critical}")
    
    if dev_pipeline and prod_pipeline:
        dev_batch = dev_pipeline.get('processing', {}).get('batch_size')
        prod_batch = prod_pipeline.get('processing', {}).get('batch_size')
        print(f"   - Batch Size: Dev={dev_batch}, Prod={prod_batch}")
        
        dev_monitoring = dev_pipeline.get('monitoring', {}).get('enabled')
        prod_monitoring = prod_pipeline.get('monitoring', {}).get('enabled')
        print(f"   - Monitoring: Dev={dev_monitoring}, Prod={prod_monitoring}")
    
    print("\n=== Demo Complete ===")


if __name__ == "__main__":
    main() 