# src/common/monitoring/metrics_collector.py
from typing import Dict, Any, List
from datetime import datetime, timedelta
from collections import defaultdict, deque
import time

from src.common.utils.config_manager import ConfigManager
from src.common.monitoring.logger import ETLLogger


class MetricsCollector:
    """
    Collects and aggregates ETL pipeline metrics for monitoring and alerting.
    
    Features:
    - Performance metrics collection
    - Real-time aggregation
    - Threshold monitoring
    - Historical trend analysis
    """
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.logger = ETLLogger(self.__class__.__name__)
        
        # Load metrics configuration
        self.env_config = config_manager.get_environment_config()
        self.metrics_config = self.env_config.get('metrics', {})
        
        # Metrics storage
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.counters: Dict[str, int] = defaultdict(int)
        self.gauges: Dict[str, float] = defaultdict(float)
        self.histograms: Dict[str, List[float]] = defaultdict(list)
        
        # Thresholds for alerting
        self.thresholds = self.metrics_config.get('thresholds', {})
        
        self.logger.info("MetricsCollector initialized")
    
    def record_validation_metrics(self, validation_result) -> None:
        """Record metrics from validation result"""
        timestamp = datetime.now()
        
        # Basic validation metrics
        self.record_gauge('validation.quality_score', validation_result.summary['data_quality_score'])
        self.record_counter('validation.total_files', validation_result.summary['total_files'])
        self.record_counter('validation.valid_files', validation_result.summary['valid_files'])
        self.record_counter('validation.invalid_files', validation_result.summary['invalid_files'])
        self.record_counter('validation.total_rows', validation_result.summary['total_rows'])
        self.record_counter('validation.critical_issues', validation_result.summary['critical_issues'])
        self.record_counter('validation.warnings', validation_result.summary['warnings'])
        
        # Status metrics
        status_metric = f"validation.status.{validation_result.overall_status}"
        self.record_counter(status_metric, 1)
        
        # Per-source metrics
        source_type = validation_result.table_identifier.split('.')[1] if '.' in validation_result.table_identifier else 'unknown'
        self.record_counter(f'validation.source.{source_type}.count', 1)
        self.record_gauge(f'validation.source.{source_type}.quality_score', validation_result.summary['data_quality_score'])
    
    def record_processing_metrics(self, processing_result: Dict[str, Any]) -> None:
        """Record metrics from processing result"""
        # Processing metrics
        self.record_counter('processing.files_processed', processing_result.get('files_processed', 0))
        self.record_counter('processing.rows_loaded', processing_result.get('rows_loaded', 0))
        self.record_histogram('processing.time_seconds', processing_result.get('processing_time_seconds', 0))
        
        # Success/failure metrics
        if processing_result.get('success'):
            self.record_counter('processing.success', 1)
        else:
            self.record_counter('processing.failure', 1)
        
        # Error metrics
        error_count = len(processing_result.get('errors', []))
        if error_count > 0:
            self.record_counter('processing.errors', error_count)
    
    def record_pipeline_metrics(self, pipeline_result: Dict[str, Any]) -> None:
        """Record metrics from pipeline execution"""
        pipeline_name = pipeline_result.get('pipeline_name', 'unknown')
        
        # Pipeline execution metrics
        self.record_histogram(f'pipeline.{pipeline_name}.duration', pipeline_result.get('total_processing_time', 0))
        self.record_counter(f'pipeline.{pipeline_name}.steps_executed', pipeline_result.get('steps_executed', 0))
        self.record_counter(f'pipeline.{pipeline_name}.steps_failed', pipeline_result.get('steps_failed', 0))
        
        # Success/failure
        if pipeline_result.get('success'):
            self.record_counter(f'pipeline.{pipeline_name}.success', 1)
        else:
            self.record_counter(f'pipeline.{pipeline_name}.failure', 1)
    
    def record_counter(self, metric_name: str, value: int = 1) -> None:
        """Record counter metric"""
        self.counters[metric_name] += value
        self._record_time_series(metric_name, value, 'counter')
    
    def record_gauge(self, metric_name: str, value: float) -> None:
        """Record gauge metric"""
        self.gauges[metric_name] = value
        self._record_time_series(metric_name, value, 'gauge')
    
    def record_histogram(self, metric_name: str, value: float) -> None:
        """Record histogram metric"""
        self.histograms[metric_name].append(value)
        self._record_time_series(metric_name, value, 'histogram')
        
        # Keep only recent values
        if len(self.histograms[metric_name]) > 1000:
            self.histograms[metric_name] = self.histograms[metric_name][-1000:]
    
    def _record_time_series(self, metric_name: str, value: float, metric_type: str) -> None:
        """Record time series data point"""
        data_point = {
            'timestamp': datetime.now(),
            'value': value,
            'type': metric_type
        }
        self.metrics[metric_name].append(data_point)
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get current metrics summary"""
        summary = {
            'timestamp': datetime.now().isoformat(),
            'counters': dict(self.counters),
            'gauges': dict(self.gauges),
            'histograms': {}
        }
        
        # Calculate histogram statistics
        for metric_name, values in self.histograms.items():
            if values:
                summary['histograms'][metric_name] = {
                    'count': len(values),
                    'min': min(values),
                    'max': max(values),
                    'mean': sum(values) / len(values),
                    'p95': self._percentile(values, 95),
                    'p99': self._percentile(values, 99)
                }
        
        return summary
    
    def _percentile(self, values: List[float], percentile: int) -> float:
        """Calculate percentile of values"""
        if not values:
            return 0.0
        
        sorted_values = sorted(values)
        index = int(percentile / 100.0 * len(sorted_values))
        return sorted_values[min(index, len(sorted_values) - 1)]
    
    def check_thresholds(self) -> List[Dict[str, Any]]:
        """Check metrics against configured thresholds"""
        alerts = []
        
        for metric_name, threshold_config in self.thresholds.items():
            try:
                current_value = self._get_current_metric_value(metric_name)
                if current_value is None:
                    continue
                
                alert = self._check_threshold(metric_name, current_value, threshold_config)
                if alert:
                    alerts.append(alert)
                    
            except Exception as e:
                self.logger.error(f"Error checking threshold for {metric_name}: {str(e)}")
        
        return alerts
    
    def _get_current_metric_value(self, metric_name: str) -> Optional[float]:
        """Get current value for metric"""
        if metric_name in self.gauges:
            return self.gauges[metric_name]
        elif metric_name in self.counters:
            return float(self.counters[metric_name])
        elif metric_name in self.histograms and self.histograms[metric_name]:
            # Use most recent value for histograms
            return self.histograms[metric_name][-1]
        else:
            return None
    
    def _check_threshold(self, metric_name: str, current_value: float, 
                        threshold_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check if metric violates threshold"""
        threshold_type = threshold_config.get('type', 'max')
        threshold_value = threshold_config.get('value')
        severity = threshold_config.get('severity', 'warning')
        
        violated = False
        
        if threshold_type == 'max' and current_value > threshold_value:
            violated = True
        elif threshold_type == 'min' and current_value < threshold_value:
            violated = True
        
        if violated:
            return {
                'metric_name': metric_name,
                'current_value': current_value,
                'threshold_value': threshold_value,
                'threshold_type': threshold_type,
                'severity': severity,
                'message': f"Metric {metric_name} ({current_value}) violates {threshold_type} threshold ({threshold_value})"
            }
        
        return None