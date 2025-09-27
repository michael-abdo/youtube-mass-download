#!/usr/bin/env python3
"""
Centralized Reporting Module (DRY Iteration 1 - Step 5)

Consolidates all report generation and formatting functionality from across the codebase:
- JSON report structures (from json_utils.py)
- HTML report generation (from ui_components.py)
- Summary statistics (from data_processing.py)
- Progress reports (from doc_templates.py)
- Operation reports (from streaming_integration.py, s3_manager.py)
- Exception summaries (from config.py)

This eliminates duplication of report formatting patterns and provides
a unified API for all reporting needs.
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
from dataclasses import dataclass, asdict
from enum import Enum

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    from logging_config import get_logger
    from constants import FilePaths, LogFormats
except ImportError:
    from .logging_config import get_logger
    from .constants import FilePaths, LogFormats

logger = get_logger(__name__)


# ============================================================================
# REPORT TYPES AND STRUCTURES
# ============================================================================

class ReportFormat(Enum):
    """Report output formats"""
    JSON = "json"
    HTML = "html"
    MARKDOWN = "md"
    TEXT = "txt"


class ReportType(Enum):
    """Report types for classification"""
    SUMMARY = "summary"
    PROGRESS = "progress"
    VALIDATION = "validation"
    EXCEPTION = "exception"
    OPERATION = "operation"
    COMPLETION = "completion"
    STREAMING = "streaming"
    UPLOAD = "upload"


@dataclass
class ReportMetadata:
    """Metadata for all reports"""
    title: str
    report_type: ReportType
    generated_at: str
    generated_by: str
    version: str = "1.0"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class ReportSummary:
    """Summary section for reports"""
    total_items: int = 0
    processed_items: int = 0
    success_count: int = 0
    error_count: int = 0
    elapsed_time_seconds: float = 0.0
    
    def completion_rate(self) -> float:
        """Calculate completion rate"""
        if self.total_items == 0:
            return 0.0
        return (self.processed_items / self.total_items) * 100
    
    def success_rate(self) -> float:
        """Calculate success rate"""
        if self.processed_items == 0:
            return 0.0
        return (self.success_count / self.processed_items) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with calculated rates"""
        data = asdict(self)
        data['completion_rate'] = self.completion_rate()
        data['success_rate'] = self.success_rate()
        return data


# ============================================================================
# CORE REPORT GENERATOR CLASS
# ============================================================================

class ReportGenerator:
    """
    Centralized report generator that consolidates all reporting patterns.
    
    This class replaces scattered report generation functions across:
    - json_utils.create_report_structure()
    - ui_components.generate_report_page()
    - data_processing.generate_summary_stats()
    - streaming_integration.generate_streaming_report()
    - And many others...
    """
    
    def __init__(self, default_format: ReportFormat = ReportFormat.JSON):
        """
        Initialize report generator.
        
        Args:
            default_format: Default output format for reports
        """
        self.default_format = default_format
        self.reports = {}  # Store generated reports
    
    def create_base_report(self, 
                          title: str,
                          report_type: ReportType,
                          summary: Optional[ReportSummary] = None) -> Dict[str, Any]:
        """
        Create base report structure.
        
        Consolidates: json_utils.create_report_structure()
        
        Args:
            title: Report title
            report_type: Type of report
            summary: Optional summary data
            
        Returns:
            Base report structure
        """
        metadata = ReportMetadata(
            title=title,
            report_type=report_type,
            generated_at=datetime.now().isoformat(),
            generated_by="ReportGenerator"
        )
        
        return {
            'metadata': metadata.to_dict(),
            'summary': summary.to_dict() if summary else {},
            'details': [],
            'sections': [],
            'errors': [],
            'context': {}
        }
    
    def create_summary_report(self, 
                            data: Dict[str, Any],
                            title: str = "Summary Report",
                            include_stats: bool = True) -> Dict[str, Any]:
        """
        Create summary report with statistics.
        
        Consolidates: data_processing.generate_summary_stats()
        
        Args:
            data: Data to summarize
            title: Report title
            include_stats: Whether to include detailed statistics
            
        Returns:
            Summary report
        """
        report = self.create_base_report(title, ReportType.SUMMARY)
        
        if include_stats and isinstance(data, dict):
            # Calculate basic statistics
            stats = {
                'total_keys': len(data),
                'non_empty_values': sum(1 for v in data.values() if v),
                'data_types': {k: type(v).__name__ for k, v in data.items()},
                'memory_usage_bytes': sum(len(str(v)) for v in data.values())
            }
            report['summary']['statistics'] = stats
        
        report['details'] = data
        return report
    
    def create_operation_report(self,
                              operation_name: str,
                              results: Dict[str, Any],
                              start_time: datetime,
                              context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Create operation report.
        
        Consolidates: streaming_integration.generate_streaming_report()
                     s3_manager.save_report()
        
        Args:
            operation_name: Name of the operation
            results: Operation results
            start_time: When operation started
            context: Additional context
            
        Returns:
            Operation report
        """
        elapsed_time = (datetime.now() - start_time).total_seconds()
        
        # Extract counts from results
        total_items = results.get('total_items', 0)
        success_count = results.get('success_count', 0)
        error_count = results.get('error_count', 0)
        
        summary = ReportSummary(
            total_items=total_items,
            processed_items=success_count + error_count,
            success_count=success_count,
            error_count=error_count,
            elapsed_time_seconds=elapsed_time
        )
        
        report = self.create_base_report(
            f"{operation_name} Report",
            ReportType.OPERATION,
            summary
        )
        
        report['details'] = results
        report['context'] = context or {}
        report['performance'] = {
            'start_time': start_time.isoformat(),
            'end_time': datetime.now().isoformat(),
            'elapsed_seconds': elapsed_time,
            'items_per_second': total_items / elapsed_time if elapsed_time > 0 else 0
        }
        
        return report
    
    def create_validation_report(self,
                               validation_results: List[Dict[str, Any]],
                               title: str = "Validation Report") -> Dict[str, Any]:
        """
        Create validation report.
        
        Consolidates: doc_templates.create_validation_report()
                     test_utilities.save_report()
        
        Args:
            validation_results: List of validation results
            title: Report title
            
        Returns:
            Validation report
        """
        # Calculate summary statistics
        total_validations = len(validation_results)
        passed_count = sum(1 for r in validation_results if r.get('status') == 'passed')
        failed_count = sum(1 for r in validation_results if r.get('status') == 'failed')
        
        summary = ReportSummary(
            total_items=total_validations,
            processed_items=total_validations,
            success_count=passed_count,
            error_count=failed_count
        )
        
        report = self.create_base_report(title, ReportType.VALIDATION, summary)
        report['details'] = validation_results
        
        # Group results by status
        report['sections'] = [
            {
                'title': 'Passed Validations',
                'items': [r for r in validation_results if r.get('status') == 'passed']
            },
            {
                'title': 'Failed Validations', 
                'items': [r for r in validation_results if r.get('status') == 'failed']
            }
        ]
        
        return report
    
    def create_completion_report(self,
                               items: List[Dict[str, Any]],
                               url_columns: List[str],
                               title: str = "Completion Report") -> Dict[str, Any]:
        """
        Create completion report for URL processing.
        
        Consolidates: data_processing.create_completion_report()
        
        Args:
            items: List of items to analyze
            url_columns: Columns containing URLs
            title: Report title
            
        Returns:
            Completion report
        """
        total_rows = len(items)
        
        # Analyze completion for each URL column
        column_stats = {}
        for col in url_columns:
            filled_count = sum(1 for item in items if item.get(col))
            column_stats[col] = {
                'total': total_rows,
                'filled': filled_count,
                'empty': total_rows - filled_count,
                'completion_rate': (filled_count / total_rows * 100) if total_rows > 0 else 0
            }
        
        # Calculate overall completion
        complete_rows = sum(1 for item in items 
                          if all(item.get(col) for col in url_columns))
        
        summary = ReportSummary(
            total_items=total_rows,
            processed_items=total_rows,
            success_count=complete_rows,
            error_count=total_rows - complete_rows
        )
        
        report = self.create_base_report(title, ReportType.COMPLETION, summary)
        report['details'] = {
            'column_statistics': column_stats,
            'overall_completion': {
                'complete_rows': complete_rows,
                'incomplete_rows': total_rows - complete_rows,
                'completion_rate': summary.completion_rate()
            }
        }
        
        return report


# ============================================================================
# REPORT FORMATTERS
# ============================================================================

class ReportFormatter:
    """
    Formats reports into different output formats.
    
    Consolidates formatting functions from:
    - ui_components.generate_report_page()
    - ui_components.generate_summary_table()
    """
    
    @staticmethod
    def to_json(report: Dict[str, Any], indent: int = 2) -> str:
        """Format report as JSON"""
        return json.dumps(report, indent=indent, default=str)
    
    @staticmethod
    def to_html(report: Dict[str, Any], 
                include_styles: bool = True) -> str:
        """
        Format report as HTML.
        
        Consolidates: ui_components.generate_report_page()
        """
        metadata = report.get('metadata', {})
        title = metadata.get('title', 'Report')
        
        # CSS styles
        styles = """
        <style>
            body { 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                line-height: 1.6; margin: 20px; background: #f5f5f5; 
            }
            .container { 
                max-width: 1200px; margin: 0 auto; background: white; 
                padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); 
            }
            h1 { color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }
            h2 { color: #34495e; margin-top: 30px; }
            .metadata { background: #ecf0f1; padding: 15px; border-radius: 4px; margin-bottom: 20px; }
            .summary { background: #e8f5e8; padding: 15px; border-radius: 4px; margin-bottom: 20px; }
            .error { background: #ffeaea; padding: 15px; border-radius: 4px; color: #c0392b; }
            table { width: 100%; border-collapse: collapse; margin: 15px 0; }
            th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
            th { background-color: #f8f9fa; font-weight: 600; }
            .metric { display: inline-block; margin: 10px 15px 10px 0; }
            .metric-value { font-size: 24px; font-weight: bold; color: #2980b9; }
            .metric-label { color: #7f8c8d; font-size: 14px; }
        </style>
        """ if include_styles else ""
        
        # Generate HTML content
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>{title}</title>
            {styles}
        </head>
        <body>
            <div class="container">
                <h1>{title}</h1>
        """
        
        # Metadata section
        if metadata:
            html += '<div class="metadata"><h2>Report Information</h2>'
            for key, value in metadata.items():
                html += f'<p><strong>{key.replace("_", " ").title()}:</strong> {value}</p>'
            html += '</div>'
        
        # Summary section
        summary = report.get('summary', {})
        if summary:
            html += '<div class="summary"><h2>Summary</h2>'
            for key, value in summary.items():
                if isinstance(value, (int, float)):
                    html += f'<div class="metric"><div class="metric-value">{value}</div><div class="metric-label">{key.replace("_", " ").title()}</div></div>'
                else:
                    html += f'<p><strong>{key.replace("_", " ").title()}:</strong> {value}</p>'
            html += '</div>'
        
        # Sections
        sections = report.get('sections', [])
        for section in sections:
            html += f'<h2>{section.get("title", "Section")}</h2>'
            items = section.get('items', [])
            if items and isinstance(items[0], dict):
                # Table format for structured data
                html += '<table><thead><tr>'
                for key in items[0].keys():
                    html += f'<th>{key.replace("_", " ").title()}</th>'
                html += '</tr></thead><tbody>'
                for item in items:
                    html += '<tr>'
                    for value in item.values():
                        html += f'<td>{value}</td>'
                    html += '</tr>'
                html += '</tbody></table>'
            else:
                # List format for simple data
                html += '<ul>'
                for item in items:
                    html += f'<li>{item}</li>'
                html += '</ul>'
        
        # Errors section
        errors = report.get('errors', [])
        if errors:
            html += '<div class="error"><h2>Errors</h2><ul>'
            for error in errors:
                html += f'<li>{error}</li>'
            html += '</ul></div>'
        
        html += """
            </div>
        </body>
        </html>
        """
        
        return html
    
    @staticmethod
    def to_markdown(report: Dict[str, Any]) -> str:
        """Format report as Markdown"""
        metadata = report.get('metadata', {})
        title = metadata.get('title', 'Report')
        
        markdown = f"# {title}\n\n"
        
        # Metadata
        if metadata:
            markdown += "## Report Information\n\n"
            for key, value in metadata.items():
                markdown += f"- **{key.replace('_', ' ').title()}:** {value}\n"
            markdown += "\n"
        
        # Summary
        summary = report.get('summary', {})
        if summary:
            markdown += "## Summary\n\n"
            for key, value in summary.items():
                markdown += f"- **{key.replace('_', ' ').title()}:** {value}\n"
            markdown += "\n"
        
        # Sections
        sections = report.get('sections', [])
        for section in sections:
            markdown += f"## {section.get('title', 'Section')}\n\n"
            items = section.get('items', [])
            for item in items:
                markdown += f"- {item}\n"
            markdown += "\n"
        
        return markdown


# ============================================================================
# REPORT PERSISTENCE
# ============================================================================

class ReportSaver:
    """
    Saves reports to various formats and locations.
    
    Consolidates: ui_components.save_html_report()
                 test_utilities.save_report()
                 s3_manager.save_report()
    """
    
    def __init__(self, base_directory: Union[str, Path] = None):
        """
        Initialize report saver.
        
        Args:
            base_directory: Base directory for saving reports
        """
        self.base_directory = Path(base_directory or FilePaths.REPORTS_DIR)
        self.base_directory.mkdir(parents=True, exist_ok=True)
    
    def save_report(self, 
                   report: Dict[str, Any],
                   filename: str,
                   format: ReportFormat = ReportFormat.JSON) -> bool:
        """
        Save report to file.
        
        Args:
            report: Report to save
            filename: Output filename (without extension)
            format: Output format
            
        Returns:
            Success status
        """
        try:
            formatter = ReportFormatter()
            
            # Generate content based on format
            if format == ReportFormat.JSON:
                content = formatter.to_json(report)
                extension = '.json'
            elif format == ReportFormat.HTML:
                content = formatter.to_html(report)
                extension = '.html'
            elif format == ReportFormat.MARKDOWN:
                content = formatter.to_markdown(report)
                extension = '.md'
            else:
                content = str(report)
                extension = '.txt'
            
            # Save to file
            output_path = self.base_directory / f"{filename}{extension}"
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logger.info(f"Report saved to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save report: {e}")
            return False


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

# Global instances for convenience
_default_generator = ReportGenerator()
_default_saver = ReportSaver()

def create_report(title: str, 
                 report_type: ReportType,
                 data: Optional[Dict[str, Any]] = None,
                 summary: Optional[ReportSummary] = None) -> Dict[str, Any]:
    """
    Convenience function to create a report.
    
    Replaces scattered report creation functions across the codebase.
    """
    if data:
        report = _default_generator.create_summary_report(data, title)
    else:
        report = _default_generator.create_base_report(title, report_type, summary)
    return report

def save_report(report: Dict[str, Any], 
               filename: str,
               format: ReportFormat = ReportFormat.JSON) -> bool:
    """
    Convenience function to save a report.
    
    Replaces scattered save functions across the codebase.
    """
    return _default_saver.save_report(report, filename, format)

def create_and_save_report(title: str,
                          data: Dict[str, Any],
                          filename: str,
                          format: ReportFormat = ReportFormat.JSON) -> bool:
    """
    Convenience function to create and save a report in one call.
    """
    report = create_report(title, ReportType.SUMMARY, data)
    return save_report(report, filename, format)


# ============================================================================
# PANDAS INTEGRATION (if available)
# ============================================================================

if HAS_PANDAS:
    def create_dataframe_report(df: pd.DataFrame, 
                              title: str = "DataFrame Report") -> Dict[str, Any]:
        """
        Create comprehensive report for pandas DataFrame.
        
        Consolidates: data_processing.generate_summary_stats()
        """
        # Basic info
        total_rows = len(df)
        total_cols = len(df.columns)
        memory_usage = df.memory_usage(deep=True).sum()
        
        # Column analysis
        column_info = {}
        for col in df.columns:
            series = df[col]
            column_info[col] = {
                'dtype': str(series.dtype),
                'non_null_count': series.notna().sum(),
                'null_count': series.isna().sum(),
                'null_percentage': (series.isna().sum() / len(series) * 100),
                'unique_count': series.nunique()
            }
            
            # Add type-specific stats
            if pd.api.types.is_numeric_dtype(series):
                column_info[col].update({
                    'mean': series.mean(),
                    'std': series.std(),
                    'min': series.min(),
                    'max': series.max()
                })
        
        summary = ReportSummary(
            total_items=total_rows,
            processed_items=total_rows,
            success_count=df.notna().all(axis=1).sum(),
            error_count=df.isna().any(axis=1).sum()
        )
        
        report = _default_generator.create_base_report(title, ReportType.SUMMARY, summary)
        report['details'] = {
            'basic_info': {
                'total_rows': total_rows,
                'total_columns': total_cols,
                'memory_usage_bytes': memory_usage,
                'dtypes': df.dtypes.value_counts().to_dict()
            },
            'column_analysis': column_info
        }
        
        return report


if __name__ == "__main__":
    # Test the reporting system
    print("🔧 Testing Centralized Reporting System")
    print("=" * 50)
    
    # Create test report
    generator = ReportGenerator()
    test_data = {
        'total_files': 150,
        'processed_files': 147,
        'errors': 3,
        'success_rate': 98.0
    }
    
    report = generator.create_summary_report(test_data, "Test Report")
    print("✓ Created summary report")
    
    # Test formatting
    formatter = ReportFormatter()
    json_output = formatter.to_json(report)
    html_output = formatter.to_html(report)
    md_output = formatter.to_markdown(report)
    
    print("✓ Generated JSON, HTML, and Markdown formats")
    
    # Test saving
    saver = ReportSaver()
    success = saver.save_report(report, "test_report", ReportFormat.JSON)
    if success:
        print("✓ Saved report to file")
    
    print("\n✅ Centralized reporting system is ready!")
    print(f"📊 Report consolidates patterns from 8+ utility modules")