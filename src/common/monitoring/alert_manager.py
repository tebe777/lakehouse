# src/common/monitoring/alert_manager.py
import json
import requests
from typing import Dict, Any, List, Optional
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, asdict

from src.common.utils.config_manager import ConfigManager
from src.common.monitoring.logger import ETLLogger


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertChannel(Enum):
    """Alert delivery channels"""
    SLACK = "slack"
    EMAIL = "email"
    TEAMS = "teams"
    WEBHOOK = "webhook"
    LOG = "log"


@dataclass
class Alert:
    """Alert data structure"""
    alert_id: str
    title: str
    message: str
    severity: AlertSeverity
    source: str
    timestamp: datetime
    details: Dict[str, Any]
    tags: List[str]
    channels: List[AlertChannel]
    resolved: bool = False
    resolution_time: Optional[datetime] = None


class AlertManager:
    """
    Comprehensive alerting system for ETL pipeline monitoring.
    
    Features:
    - Multiple alert channels (Slack, Email, Teams, Webhook)
    - Severity-based routing
    - Alert aggregation and deduplication
    - Rate limiting and throttling
    - Alert resolution tracking
    """
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.logger = ETLLogger(self.__class__.__name__)
        
        # Load alerting configuration
        self.env_config = config_manager.get_environment_config()
        self.alert_config = self.env_config.get('alerting', {})
        
        # Alert configuration
        self.enabled = self.alert_config.get('enabled', True)
        self.default_channels = [AlertChannel(ch) for ch in self.alert_config.get('default_channels', ['log'])]
        self.rate_limit_window_minutes = self.alert_config.get('rate_limit_window_minutes', 15)
        self.max_alerts_per_window = self.alert_config.get('max_alerts_per_window', 10)
        
        # Channel configurations
        self.channel_configs = self.alert_config.get('channels', {})
        
        # Alert tracking
        self.recent_alerts: List[Alert] = []
        self.alert_counts: Dict[str, int] = {}
        
        self.logger.info("AlertManager initialized")
    
    def send_alert(self, title: str, message: str, severity: AlertSeverity,
                  source: str, details: Dict[str, Any] = None,
                  tags: List[str] = None, channels: List[AlertChannel] = None) -> str:
        """
        Send alert through configured channels
        
        Args:
            title: Alert title
            message: Alert message
            severity: Alert severity level
            source: Source component/system
            details: Additional alert details
            tags: Alert tags for categorization
            channels: Specific channels to use (defaults to config)
            
        Returns:
            Alert ID
        """
        if not self.enabled:
            self.logger.debug("Alerting disabled - skipping alert")
            return ""
        
        # Generate alert ID
        alert_id = f"alert_{int(datetime.now().timestamp())}_{hash(title + message) % 10000}"
        
        # Create alert object
        alert = Alert(
            alert_id=alert_id,
            title=title,
            message=message,
            severity=severity,
            source=source,
            timestamp=datetime.now(),
            details=details or {},
            tags=tags or [],
            channels=channels or self.default_channels
        )
        
        try:
            # Check rate limiting
            if self._is_rate_limited(alert):
                self.logger.warning(f"Alert rate limited: {alert_id}")
                return alert_id
            
            # Check for duplicate alerts
            if self._is_duplicate_alert(alert):
                self.logger.info(f"Duplicate alert suppressed: {alert_id}")
                return alert_id
            
            # Send through configured channels
            self._send_through_channels(alert)
            
            # Track alert
            self.recent_alerts.append(alert)
            self._cleanup_old_alerts()
            
            self.logger.info(f"Alert sent: {alert_id} - {title}")
            
        except Exception as e:
            self.logger.error(f"Failed to send alert {alert_id}: {str(e)}")
        
        return alert_id
    
    def send_validation_alert(self, validation_result, processing_result: Dict[str, Any] = None) -> str:
        """Send alert based on validation result"""
        from src.quality.validators.input_validator import InputValidationResult
        
        if not isinstance(validation_result, InputValidationResult):
            return ""
        
        # Determine severity based on validation status
        severity_map = {
            "passed": AlertSeverity.INFO,
            "warning": AlertSeverity.WARNING,
            "failed": AlertSeverity.ERROR
        }
        
        severity = severity_map.get(validation_result.overall_status, AlertSeverity.WARNING)
        
        # Create alert message
        title = f"Input Validation {validation_result.overall_status.upper()}: {validation_result.table_identifier}"
        
        message_parts = [
            f"Validation Status: {validation_result.overall_status}",
            f"Files Validated: {validation_result.summary['valid_files']}/{validation_result.summary['total_files']}",
            f"Total Rows: {validation_result.summary['total_rows']:,}",
            f"Critical Issues: {validation_result.summary['critical_issues']}",
            f"Warnings: {validation_result.summary['warnings']}"
        ]
        
        if processing_result:
            message_parts.extend([
                f"Processing Success: {processing_result['success']}",
                f"Rows Processed: {processing_result.get('rows_loaded', 0):,}"
            ])
        
        message = "\n".join(message_parts)
        
        # Collect details
        details = {
            'validation_id': validation_result.validation_id,
            'source_path': validation_result.source_path,
            'table_identifier': validation_result.table_identifier,
            'validation_summary': validation_result.summary,
            'file_results': [
                {
                    'file_path': f.file_path,
                    'is_valid': f.is_valid,
                    'row_count': f.row_count,
                    'errors': f.errors,
                    'warnings': f.warnings
                }
                for f in validation_result.file_results
            ]
        }
        
        if processing_result:
            details['processing_result'] = processing_result
        
        # Add recommendations to message if any
        if validation_result.summary.get('recommendations'):
            message += "\n\nRecommendations:\n"
            for rec in validation_result.summary['recommendations']:
                message += f"• {rec}\n"
        
        # Determine channels based on severity
        channels = self._get_channels_for_severity(severity)
        
        return self.send_alert(
            title=title,
            message=message,
            severity=severity,
            source="input_validation",
            details=details,
            tags=["validation", "etl", validation_result.table_identifier.split('.')[1]],  # source type
            channels=channels
        )
    
    def send_pipeline_alert(self, pipeline_name: str, execution_result: Dict[str, Any]) -> str:
        """Send alert for pipeline execution"""
        success = execution_result.get('success', False)
        severity = AlertSeverity.INFO if success else AlertSeverity.ERROR
        
        title = f"Pipeline {'SUCCESS' if success else 'FAILED'}: {pipeline_name}"
        
        message_parts = [
            f"Pipeline: {pipeline_name}",
            f"Execution Date: {execution_result.get('execution_date', 'unknown')}",
            f"Status: {'SUCCESS' if success else 'FAILED'}",
            f"Steps Executed: {execution_result.get('steps_executed', 0)}",
            f"Steps Failed: {execution_result.get('steps_failed', 0)}",
            f"Processing Time: {execution_result.get('total_processing_time', 0):.2f}s"
        ]
        
        if not success and 'step_results' in execution_result:
            failed_steps = [s for s in execution_result['step_results'] if not s.get('success', True)]
            if failed_steps:
                message_parts.append("\nFailed Steps:")
                for step in failed_steps[:3]:  # Show max 3 failed steps
                    message_parts.append(f"• {step.get('step_id', 'unknown')}: {step.get('error', 'unknown error')}")
        
        message = "\n".join(message_parts)
        
        channels = self._get_channels_for_severity(severity)
        
        return self.send_alert(
            title=title,
            message=message,
            severity=severity,
            source="pipeline_execution",
            details=execution_result,
            tags=["pipeline", "etl", pipeline_name],
            channels=channels
        )
    
    def send_data_quality_alert(self, validation_results: Dict[str, Any], table_identifier: str) -> str:
        """Send alert for data quality issues"""
        quality_score = validation_results.get('data_quality_score', 100)
        has_errors = validation_results.get('has_errors', False)
        
        # Determine severity based on quality score and errors
        if has_errors or quality_score < 70:
            severity = AlertSeverity.ERROR
        elif quality_score < 85:
            severity = AlertSeverity.WARNING
        else:
            severity = AlertSeverity.INFO
        
        title = f"Data Quality {'CRITICAL' if has_errors else 'ALERT'}: {table_identifier}"
        
        message_parts = [
            f"Table: {table_identifier}",
            f"Quality Score: {quality_score:.1f}%",
            f"Rules Passed: {validation_results.get('passed_rules', 0)}",
            f"Rules Failed: {validation_results.get('failed_rules', 0)}",
            f"Warnings: {validation_results.get('warnings', 0)}",
            f"Row Count: {validation_results.get('row_count', 0):,}"
        ]
        
        # Add error details
        if validation_results.get('errors'):
            message_parts.append("\nErrors:")
            for error in validation_results['errors'][:3]:  # Show max 3 errors
                message_parts.append(f"• {error.get('rule_type', 'unknown')}: {error.get('message', 'unknown error')}")
        
        message = "\n".join(message_parts)
        
        channels = self._get_channels_for_severity(severity)
        
        return self.send_alert(
            title=title,
            message=message,
            severity=severity,
            source="data_quality",
            details=validation_results,
            tags=["data_quality", "validation", table_identifier.split('.')[1]],
            channels=channels
        )
    
    def resolve_alert(self, alert_id: str, resolution_message: str = "") -> bool:
        """Mark alert as resolved"""
        try:
            for alert in self.recent_alerts:
                if alert.alert_id == alert_id:
                    alert.resolved = True
                    alert.resolution_time = datetime.now()
                    
                    # Send resolution notification if configured
                    if self.alert_config.get('send_resolution_notifications', False):
                        self.send_alert(
                            title=f"RESOLVED: {alert.title}",
                            message=f"Alert resolved: {alert.message}\n\nResolution: {resolution_message}",
                            severity=AlertSeverity.INFO,
                            source=alert.source,
                            tags=alert.tags + ["resolved"]
                        )
                    
                    self.logger.info(f"Alert resolved: {alert_id}")
                    return True
            
            self.logger.warning(f"Alert not found for resolution: {alert_id}")
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to resolve alert {alert_id}: {str(e)}")
            return False
    
    def _is_rate_limited(self, alert: Alert) -> bool:
        """Check if alert should be rate limited"""
        now = datetime.now()
        window_start = now - timedelta(minutes=self.rate_limit_window_minutes)
        
        # Count recent alerts from same source
        recent_count = sum(1 for a in self.recent_alerts 
                          if a.source == alert.source and a.timestamp > window_start)
        
        return recent_count >= self.max_alerts_per_window
    
    def _is_duplicate_alert(self, alert: Alert) -> bool:
        """Check if this is a duplicate alert"""
        # Look for similar alerts in the last 5 minutes
        recent_window = datetime.now() - timedelta(minutes=5)
        
        for recent_alert in self.recent_alerts:
            if (recent_alert.timestamp > recent_window and
                recent_alert.title == alert.title and
                recent_alert.source == alert.source and
                not recent_alert.resolved):
                return True
        
        return False
    
    def _get_channels_for_severity(self, severity: AlertSeverity) -> List[AlertChannel]:
        """Get appropriate channels based on severity"""
        severity_channels = self.alert_config.get('severity_channels', {})
        
        if severity.value in severity_channels:
            return [AlertChannel(ch) for ch in severity_channels[severity.value]]
        
        return self.default_channels
    
    def _send_through_channels(self, alert: Alert) -> None:
        """Send alert through all configured channels"""
        for channel in alert.channels:
            try:
                if channel == AlertChannel.SLACK:
                    self._send_slack_alert(alert)
                elif channel == AlertChannel.EMAIL:
                    self._send_email_alert(alert)
                elif channel == AlertChannel.TEAMS:
                    self._send_teams_alert(alert)
                elif channel == AlertChannel.WEBHOOK:
                    self._send_webhook_alert(alert)
                elif channel == AlertChannel.LOG:
                    self._send_log_alert(alert)
                else:
                    self.logger.warning(f"Unknown alert channel: {channel}")
                    
            except Exception as e:
                self.logger.error(f"Failed to send alert through {channel.value}: {str(e)}")
    
    def _send_slack_alert(self, alert: Alert) -> None:
        """Send alert to Slack"""
        slack_config = self.channel_configs.get('slack', {})
        webhook_url = slack_config.get('webhook_url')
        
        if not webhook_url:
            self.logger.warning("Slack webhook URL not configured")
            return
        
        # Color based on severity
        color_map = {
            AlertSeverity.INFO: "good",
            AlertSeverity.WARNING: "warning", 
            AlertSeverity.ERROR: "danger",
            AlertSeverity.CRITICAL: "danger"
        }
        
        # Format message for Slack
        slack_payload = {
            "username": "ETL Monitor",
            "icon_emoji": ":warning:",
            "attachments": [
                {
                    "color": color_map.get(alert.severity, "warning"),
                    "title": alert.title,
                    "text": alert.message,
                    "fields": [
                        {
                            "title": "Source",
                            "value": alert.source,
                            "short": True
                        },
                        {
                            "title": "Severity",
                            "value": alert.severity.value.upper(),
                            "short": True
                        },
                        {
                            "title": "Alert ID",
                            "value": alert.alert_id,
                            "short": True
                        },
                        {
                            "title": "Timestamp",
                            "value": alert.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                            "short": True
                        }
                    ],
                    "footer": "ETL Pipeline Monitor",
                    "ts": int(alert.timestamp.timestamp())
                }
            ]
        }
        
        # Add tags if present
        if alert.tags:
            slack_payload["attachments"][0]["fields"].append({
                "title": "Tags",
                "value": ", ".join(alert.tags),
                "short": False
            })
        
        response = requests.post(webhook_url, json=slack_payload, timeout=10)
        response.raise_for_status()
        
        self.logger.debug(f"Slack alert sent: {alert.alert_id}")
    
    def _send_email_alert(self, alert: Alert) -> None:
        """Send alert via email"""
        email_config = self.channel_configs.get('email', {})
        
        # This would integrate with your email service
        # For demo purposes, just log
        self.logger.info(f"Email alert would be sent: {alert.title}")
        
        # Actual implementation would use SMTP or email service API
        # smtp_server = email_config.get('smtp_server')
        # recipients = email_config.get('recipients', [])
        # ... email sending logic
    
    def _send_teams_alert(self, alert: Alert) -> None:
        """Send alert to Microsoft Teams"""
        teams_config = self.channel_configs.get('teams', {})
        webhook_url = teams_config.get('webhook_url')
        
        if not webhook_url:
            self.logger.warning("Teams webhook URL not configured")
            return
        
        # Format message for Teams
        teams_payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "FF0000" if alert.severity in [AlertSeverity.ERROR, AlertSeverity.CRITICAL] else "FFA500",
            "summary": alert.title,
            "sections": [
                {
                    "activityTitle": alert.title,
                    "activitySubtitle": f"Source: {alert.source}",
                    "text": alert.message,
                    "facts": [
                        {
                            "name": "Severity",
                            "value": alert.severity.value.upper()
                        },
                        {
                            "name": "Alert ID",
                            "value": alert.alert_id
                        },
                        {
                            "name": "Timestamp",
                            "value": alert.timestamp.strftime("%Y-%m-%d %H:%M:%S")
                        }
                    ]
                }
            ]
        }
        
        response = requests.post(webhook_url, json=teams_payload, timeout=10)
        response.raise_for_status()
        
        self.logger.debug(f"Teams alert sent: {alert.alert_id}")
    
    def _send_webhook_alert(self, alert: Alert) -> None:
        """Send alert to custom webhook"""
        webhook_config = self.channel_configs.get('webhook', {})
        webhook_url = webhook_config.get('url')
        
        if not webhook_url:
            self.logger.warning("Webhook URL not configured")
            return
        
        # Send alert as JSON
        payload = asdict(alert)
        payload['timestamp'] = alert.timestamp.isoformat()
        payload['severity'] = alert.severity.value
        payload['channels'] = [ch.value for ch in alert.channels]
        
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'ETL-Pipeline-Monitor/1.0'
        }
        
        # Add custom headers if configured
        custom_headers = webhook_config.get('headers', {})
        headers.update(custom_headers)
        
        response = requests.post(webhook_url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        
        self.logger.debug(f"Webhook alert sent: {alert.alert_id}")
    
    def _send_log_alert(self, alert: Alert) -> None:
        """Send alert to log"""
        log_level_map = {
            AlertSeverity.INFO: self.logger.info,
            AlertSeverity.WARNING: self.logger.warning,
            AlertSeverity.ERROR: self.logger.error,
            AlertSeverity.CRITICAL: self.logger.critical
        }
        
        log_func = log_level_map.get(alert.severity, self.logger.info)
        log_message = f"ALERT [{alert.alert_id}] {alert.title}: {alert.message}"
        
        log_func(log_message)
    
    def _cleanup_old_alerts(self) -> None:
        """Remove old alerts from memory"""
        cutoff_time = datetime.now() - timedelta(hours=24)
        self.recent_alerts = [a for a in self.recent_alerts if a.timestamp > cutoff_time]