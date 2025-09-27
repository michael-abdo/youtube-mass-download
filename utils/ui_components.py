#!/usr/bin/env python3
"""
Reusable UI Components Module (DRY Phase 9)

Provides reusable UI components and HTML generation utilities:
- HTML template generators
- Table and form builders
- Progress indicators
- Status displays
- Report generators
- Component patterns for consistent UI

Note: This project is primarily backend-focused, but this module
provides UI utilities for any web interfaces or HTML generation needs.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Callable
from pathlib import Path

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

from utils.logging_config import get_logger
from utils.data_processing import generate_summary_stats

logger = get_logger(__name__)


# ============================================================================
# HTML GENERATION UTILITIES
# ============================================================================

class HTMLBuilder:
    """HTML builder for generating clean, consistent markup."""
    
    def __init__(self):
        """Initialize HTML builder."""
        self.content = []
        self.indent_level = 0
    
    def add_line(self, line: str = "") -> 'HTMLBuilder':
        """Add a line of HTML with proper indentation."""
        indent = "  " * self.indent_level
        self.content.append(f"{indent}{line}")
        return self
    
    def open_tag(self, tag: str, attributes: Optional[Dict[str, str]] = None, 
                 self_closing: bool = False) -> 'HTMLBuilder':
        """Open an HTML tag."""
        attrs = ""
        if attributes:
            attrs = " " + " ".join([f'{k}="{v}"' for k, v in attributes.items()])
        
        if self_closing:
            self.add_line(f"<{tag}{attrs} />")
        else:
            self.add_line(f"<{tag}{attrs}>")
            self.indent_level += 1
        
        return self
    
    def close_tag(self, tag: str) -> 'HTMLBuilder':
        """Close an HTML tag."""
        self.indent_level -= 1
        self.add_line(f"</{tag}>")
        return self
    
    def add_text(self, text: str) -> 'HTMLBuilder':
        """Add text content."""
        self.add_line(self._escape_html(text))
        return self
    
    def add_content(self, content: str) -> 'HTMLBuilder':
        """Add raw HTML content."""
        self.add_line(content)
        return self
    
    def build(self) -> str:
        """Build the final HTML string."""
        return "\n".join(self.content)
    
    @staticmethod
    def _escape_html(text: str) -> str:
        """Escape HTML special characters."""
        return (text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace('"', "&quot;")
                   .replace("'", "&#39;"))


# ============================================================================
# TABLE GENERATION
# ============================================================================

def generate_html_table(data: List[Dict[str, Any]], 
                       title: Optional[str] = None,
                       columns: Optional[List[str]] = None,
                       table_class: str = "data-table",
                       sortable: bool = True,
                       max_rows: Optional[int] = None) -> str:
    """
    Generate HTML table from data.
    
    Consolidates table generation patterns.
    
    Args:
        data: List of dictionaries representing table rows
        title: Optional table title
        columns: Specific columns to include (all if None)
        table_class: CSS class for table
        sortable: Whether to make table sortable
        max_rows: Maximum number of rows to display
        
    Returns:
        HTML table string
        
    Example:
        html = generate_html_table(
            [{'name': 'John', 'email': 'john@example.com'}],
            title="Users",
            columns=['name', 'email']
        )
    """
    if not data:
        return "<p>No data available</p>"
    
    # Limit rows if specified
    if max_rows:
        data = data[:max_rows]
    
    # Determine columns
    if columns is None:
        columns = list(data[0].keys()) if data else []
    
    builder = HTMLBuilder()
    
    # Add title
    if title:
        builder.open_tag("h3").add_text(title).close_tag("h3")
    
    # Start table
    table_attrs = {"class": table_class}
    if sortable:
        table_attrs["class"] += " sortable"
    
    builder.open_tag("table", table_attrs)
    
    # Table header
    builder.open_tag("thead").open_tag("tr")
    for col in columns:
        header_text = col.replace('_', ' ').title()
        builder.open_tag("th").add_text(header_text).close_tag("th")
    builder.close_tag("tr").close_tag("thead")
    
    # Table body
    builder.open_tag("tbody")
    for row in data:
        builder.open_tag("tr")
        for col in columns:
            value = row.get(col, "")
            # Format value based on type
            if isinstance(value, bool):
                formatted_value = "✓" if value else "✗"
            elif isinstance(value, (int, float)):
                formatted_value = f"{value:,}" if isinstance(value, int) else f"{value:.2f}"
            else:
                formatted_value = str(value) if value is not None else ""
            
            builder.open_tag("td").add_text(formatted_value).close_tag("td")
        builder.close_tag("tr")
    builder.close_tag("tbody")
    
    builder.close_tag("table")
    
    # Add row count
    total_rows = len(data)
    if max_rows and len(data) == max_rows:
        builder.add_line(f"<p class='table-note'>Showing first {max_rows} rows</p>")
    else:
        builder.add_line(f"<p class='table-note'>Total rows: {total_rows}</p>")
    
    return builder.build()


def generate_summary_table(stats: Dict[str, Any], title: str = "Summary") -> str:
    """
    Generate summary statistics table.
    
    Args:
        stats: Statistics dictionary
        title: Table title
        
    Returns:
        HTML table string
        
    Example:
        stats = {'Total Files': 100, 'Processed': 85, 'Failed': 15}
        html = generate_summary_table(stats, "Processing Summary")
    """
    table_data = [{"Metric": k, "Value": v} for k, v in stats.items()]
    return generate_html_table(table_data, title=title, table_class="summary-table")


# ============================================================================
# PROGRESS AND STATUS COMPONENTS
# ============================================================================

def generate_progress_bar(current: int, 
                         total: int, 
                         label: str = "Progress",
                         show_percentage: bool = True,
                         bar_class: str = "progress-bar") -> str:
    """
    Generate HTML progress bar.
    
    Args:
        current: Current progress value
        total: Total maximum value
        label: Progress label
        show_percentage: Whether to show percentage
        bar_class: CSS class for progress bar
        
    Returns:
        HTML progress bar string
        
    Example:
        html = generate_progress_bar(75, 100, "Download Progress")
    """
    if total == 0:
        percentage = 0
    else:
        percentage = min(100, (current / total) * 100)
    
    builder = HTMLBuilder()
    
    # Progress container
    builder.open_tag("div", {"class": "progress-container"})
    
    # Label
    if label:
        label_text = f"{label}: {current}/{total}"
        if show_percentage:
            label_text += f" ({percentage:.1f}%)"
        builder.open_tag("div", {"class": "progress-label"}).add_text(label_text).close_tag("div")
    
    # Progress bar
    builder.open_tag("div", {"class": bar_class})
    builder.open_tag("div", {
        "class": "progress-fill",
        "style": f"width: {percentage}%"
    }).close_tag("div")
    builder.close_tag("div")
    
    builder.close_tag("div")
    
    return builder.build()


def generate_status_badge(status: str, 
                         count: Optional[int] = None,
                         badge_class: str = "status-badge") -> str:
    """
    Generate status badge HTML.
    
    Args:
        status: Status text
        count: Optional count to display
        badge_class: CSS class for badge
        
    Returns:
        HTML status badge string
        
    Example:
        html = generate_status_badge("Complete", count=42)
    """
    # Determine badge color based on status
    status_colors = {
        'complete': 'success',
        'completed': 'success',
        'success': 'success',
        'failed': 'danger',
        'error': 'danger',
        'pending': 'warning',
        'processing': 'info',
        'active': 'info'
    }
    
    color_class = status_colors.get(status.lower(), 'secondary')
    full_class = f"{badge_class} badge-{color_class}"
    
    text = status.title()
    if count is not None:
        text += f" ({count})"
    
    builder = HTMLBuilder()
    builder.open_tag("span", {"class": full_class}).add_text(text).close_tag("span")
    
    return builder.build()


def generate_status_grid(status_data: Dict[str, int], 
                        title: str = "Status Overview") -> str:
    """
    Generate status overview grid.
    
    Args:
        status_data: Dictionary of status -> count
        title: Grid title
        
    Returns:
        HTML status grid string
        
    Example:
        html = generate_status_grid({
            'Complete': 85,
            'Failed': 10,
            'Pending': 5
        })
    """
    builder = HTMLBuilder()
    
    # Title
    if title:
        builder.open_tag("h3").add_text(title).close_tag("h3")
    
    # Status grid
    builder.open_tag("div", {"class": "status-grid"})
    
    for status, count in status_data.items():
        builder.open_tag("div", {"class": "status-item"})
        builder.add_content(generate_status_badge(status, count))
        builder.close_tag("div")
    
    builder.close_tag("div")
    
    return builder.build()


# ============================================================================
# FORM GENERATION
# ============================================================================

def generate_form_field(field_type: str,
                       name: str,
                       label: str,
                       value: str = "",
                       required: bool = False,
                       options: Optional[List[str]] = None,
                       attributes: Optional[Dict[str, str]] = None) -> str:
    """
    Generate form field HTML.
    
    Args:
        field_type: Type of field (text, email, select, textarea, etc.)
        name: Field name attribute
        label: Field label
        value: Field value
        required: Whether field is required
        options: Options for select fields
        attributes: Additional HTML attributes
        
    Returns:
        HTML form field string
        
    Example:
        html = generate_form_field('email', 'user_email', 'Email Address', required=True)
    """
    builder = HTMLBuilder()
    
    # Field container
    builder.open_tag("div", {"class": "form-field"})
    
    # Label
    label_attrs = {"for": name}
    if required:
        label_text = f"{label} *"
    else:
        label_text = label
    
    builder.open_tag("label", label_attrs).add_text(label_text).close_tag("label")
    
    # Field attributes
    field_attrs = {"name": name, "id": name}
    if value:
        field_attrs["value"] = value
    if required:
        field_attrs["required"] = "required"
    if attributes:
        field_attrs.update(attributes)
    
    # Generate field based on type
    if field_type == "select":
        builder.open_tag("select", field_attrs)
        if not required:
            builder.open_tag("option", {"value": ""}).add_text("-- Select --").close_tag("option")
        
        if options:
            for option in options:
                option_attrs = {"value": option}
                if option == value:
                    option_attrs["selected"] = "selected"
                builder.open_tag("option", option_attrs).add_text(option).close_tag("option")
        
        builder.close_tag("select")
    
    elif field_type == "textarea":
        builder.open_tag("textarea", field_attrs).add_text(value).close_tag("textarea")
    
    else:
        field_attrs["type"] = field_type
        builder.open_tag("input", field_attrs, self_closing=True)
    
    builder.close_tag("div")
    
    return builder.build()


# ============================================================================
# REPORT GENERATION
# ============================================================================

def generate_report_page(title: str,
                        sections: List[Dict[str, Any]],
                        css_styles: Optional[str] = None,
                        include_timestamp: bool = True) -> str:
    """
    Generate complete HTML report page.
    
    Args:
        title: Report title
        sections: List of report sections
        css_styles: Optional CSS styles
        include_timestamp: Whether to include generation timestamp
        
    Returns:
        Complete HTML page string
        
    Example:
        html = generate_report_page("Processing Report", [
            {'type': 'summary', 'data': {'Total': 100}},
            {'type': 'table', 'data': rows, 'title': 'Results'}
        ])
    """
    builder = HTMLBuilder()
    
    # HTML document structure
    builder.add_line("<!DOCTYPE html>")
    builder.open_tag("html", {"lang": "en"})
    
    # Head section
    builder.open_tag("head")
    builder.open_tag("meta", {"charset": "UTF-8"}, self_closing=True)
    builder.open_tag("meta", {
        "name": "viewport", 
        "content": "width=device-width, initial-scale=1.0"
    }, self_closing=True)
    builder.open_tag("title").add_text(title).close_tag("title")
    
    # Default CSS styles
    if css_styles is None:
        css_styles = _get_default_css()
    
    builder.open_tag("style").add_content(css_styles).close_tag("style")
    builder.close_tag("head")
    
    # Body section
    builder.open_tag("body")
    
    # Header
    builder.open_tag("header")
    builder.open_tag("h1").add_text(title).close_tag("h1")
    if include_timestamp:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        builder.open_tag("p", {"class": "timestamp"}).add_text(f"Generated: {timestamp}").close_tag("p")
    builder.close_tag("header")
    
    # Main content
    builder.open_tag("main")
    
    # Process sections
    for section in sections:
        section_type = section.get('type', 'content')
        section_data = section.get('data', {})
        section_title = section.get('title', '')
        
        if section_type == 'summary':
            builder.add_content(generate_summary_table(section_data, section_title))
        elif section_type == 'table':
            builder.add_content(generate_html_table(section_data, section_title))
        elif section_type == 'progress':
            current = section_data.get('current', 0)
            total = section_data.get('total', 100)
            label = section_data.get('label', 'Progress')
            builder.add_content(generate_progress_bar(current, total, label))
        elif section_type == 'status':
            builder.add_content(generate_status_grid(section_data, section_title))
        elif section_type == 'content':
            if section_title:
                builder.open_tag("h2").add_text(section_title).close_tag("h2")
            builder.add_content(str(section_data))
    
    builder.close_tag("main")
    
    # Footer
    builder.open_tag("footer")
    builder.open_tag("p").add_text("Generated by Typing Clients Processing System").close_tag("p")
    builder.close_tag("footer")
    
    builder.close_tag("body")
    builder.close_tag("html")
    
    return builder.build()


def _get_default_css() -> str:
    """Get default CSS styles for reports."""
    return """
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        
        header, main, footer {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
        
        h1, h2, h3 {
            color: #333;
            margin-bottom: 16px;
        }
        
        .timestamp {
            color: #666;
            font-size: 0.9em;
        }
        
        .data-table, .summary-table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }
        
        .data-table th, .data-table td,
        .summary-table th, .summary-table td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        
        .data-table th, .summary-table th {
            background-color: #f8f9fa;
            font-weight: 600;
        }
        
        .data-table tr:hover {
            background-color: #f8f9fa;
        }
        
        .table-note {
            font-size: 0.9em;
            color: #666;
            margin-top: -10px;
        }
        
        .progress-container {
            margin-bottom: 20px;
        }
        
        .progress-label {
            margin-bottom: 8px;
            font-weight: 500;
        }
        
        .progress-bar {
            width: 100%;
            height: 20px;
            background-color: #e9ecef;
            border-radius: 10px;
            overflow: hidden;
        }
        
        .progress-fill {
            height: 100%;
            background-color: #007bff;
            transition: width 0.3s ease;
        }
        
        .status-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 10px;
            margin-bottom: 20px;
        }
        
        .status-item {
            text-align: center;
        }
        
        .status-badge {
            display: inline-block;
            padding: 6px 12px;
            border-radius: 20px;
            font-weight: 500;
            font-size: 0.875em;
        }
        
        .badge-success {
            background-color: #d4edda;
            color: #155724;
        }
        
        .badge-danger {
            background-color: #f8d7da;
            color: #721c24;
        }
        
        .badge-warning {
            background-color: #fff3cd;
            color: #856404;
        }
        
        .badge-info {
            background-color: #d1ecf1;
            color: #0c5460;
        }
        
        .badge-secondary {
            background-color: #e2e3e5;
            color: #383d41;
        }
        
        .form-field {
            margin-bottom: 20px;
        }
        
        .form-field label {
            display: block;
            margin-bottom: 5px;
            font-weight: 500;
        }
        
        .form-field input,
        .form-field select,
        .form-field textarea {
            width: 100%;
            padding: 8px 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
        }
        
        .form-field input:focus,
        .form-field select:focus,
        .form-field textarea:focus {
            outline: none;
            border-color: #007bff;
            box-shadow: 0 0 0 2px rgba(0, 123, 255, 0.25);
        }
        
        footer {
            text-align: center;
            color: #666;
            font-size: 0.9em;
        }
    """


# ============================================================================
# DASHBOARD COMPONENTS
# ============================================================================

def generate_dashboard_card(title: str,
                          value: Union[str, int, float],
                          subtitle: Optional[str] = None,
                          status: Optional[str] = None,
                          card_class: str = "dashboard-card") -> str:
    """
    Generate dashboard card component.
    
    Args:
        title: Card title
        value: Main value to display
        subtitle: Optional subtitle
        status: Optional status indicator
        card_class: CSS class for card
        
    Returns:
        HTML dashboard card string
        
    Example:
        html = generate_dashboard_card("Total Files", 1234, "Last updated: 1 hour ago")
    """
    builder = HTMLBuilder()
    
    builder.open_tag("div", {"class": card_class})
    
    # Header
    builder.open_tag("div", {"class": "card-header"})
    builder.open_tag("h3", {"class": "card-title"}).add_text(title).close_tag("h3")
    if status:
        builder.add_content(generate_status_badge(status))
    builder.close_tag("div")
    
    # Body
    builder.open_tag("div", {"class": "card-body"})
    builder.open_tag("div", {"class": "card-value"}).add_text(str(value)).close_tag("div")
    if subtitle:
        builder.open_tag("div", {"class": "card-subtitle"}).add_text(subtitle).close_tag("div")
    builder.close_tag("div")
    
    builder.close_tag("div")
    
    return builder.build()


def generate_metric_cards(metrics: Dict[str, Any]) -> str:
    """
    Generate multiple metric cards.
    
    Args:
        metrics: Dictionary of metric_name -> value
        
    Returns:
        HTML metric cards grid
        
    Example:
        html = generate_metric_cards({
            'Total Files': 1234,
            'Processed': 1100,
            'Failed': 134
        })
    """
    builder = HTMLBuilder()
    
    builder.open_tag("div", {"class": "metrics-grid"})
    
    for metric_name, value in metrics.items():
        card_html = generate_dashboard_card(metric_name, value)
        builder.add_content(card_html)
    
    builder.close_tag("div")
    
    return builder.build()


# ============================================================================
# EXPORT FUNCTIONS
# ============================================================================

def save_html_report(content: str, file_path: Union[str, Path]) -> bool:
    """
    Save HTML content to file.
    
    Args:
        content: HTML content
        file_path: Output file path
        
    Returns:
        True if successful
        
    Example:
        success = save_html_report(html_content, 'reports/processing_report.html')
    """
    try:
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"HTML report saved to: {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to save HTML report: {e}")
        return False


# Example usage
if __name__ == "__main__":
    # Test table generation
    sample_data = [
        {'name': 'John Doe', 'email': 'john@example.com', 'status': 'active', 'count': 42},
        {'name': 'Jane Smith', 'email': 'jane@example.com', 'status': 'inactive', 'count': 17}
    ]
    
    table_html = generate_html_table(sample_data, title="User List")
    print("Generated HTML table")
    
    # Test progress bar
    progress_html = generate_progress_bar(75, 100, "Processing Files")
    print("Generated progress bar")
    
    # Test status grid
    status_html = generate_status_grid({
        'Complete': 85,
        'Failed': 10,
        'Pending': 5
    })
    print("Generated status grid")
    
    # Test complete report
    report_sections = [
        {'type': 'summary', 'data': {'Total Files': 100, 'Processed': 85}, 'title': 'Summary'},
        {'type': 'table', 'data': sample_data, 'title': 'User Details'},
        {'type': 'progress', 'data': {'current': 85, 'total': 100, 'label': 'Processing'}}
    ]
    
    report_html = generate_report_page("Test Report", report_sections)
    
    # Save to file
    if save_html_report(report_html, 'test_report.html'):
        print("✓ Report saved successfully")
    
    print("✓ UI components test complete!")