#!/usr/bin/env python3
"""
Documentation Templates Module (DRY Phase 11)

Provides standardized documentation templates consolidating patterns from:
- All markdown files in docs/ and archived/docs/
- README files and documentation scattered throughout the codebase
- Report generation patterns
- API documentation structures
- Implementation guide formats

Key consolidations:
- StandardizedMarkdownTemplates: Common document structures
- ReportGenerator: Automated report generation
- APIDocumentationGenerator: API reference templates
- ImplementationGuideTemplate: Step-by-step guides
- ArchitectureDocumentTemplate: System design docs
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

from utils.logging_config import get_logger
from utils.error_handling import handle_file_operations
from utils.config import get_config, get_project_root
from utils.data_processing import write_json_safe

logger = get_logger(__name__)


# ============================================================================
# TEMPLATE TYPES AND CONFIGURATIONS
# ============================================================================

class DocumentType(Enum):
    """Document type enumeration."""
    API_REFERENCE = "api_reference"
    IMPLEMENTATION_GUIDE = "implementation_guide"
    SYSTEM_ARCHITECTURE = "system_architecture"
    PROGRESS_REPORT = "progress_report"
    VALIDATION_REPORT = "validation_report"
    EXECUTIVE_SUMMARY = "executive_summary"
    TROUBLESHOOTING_GUIDE = "troubleshooting_guide"
    README = "readme"
    CHANGELOG = "changelog"


@dataclass
class DocumentMeta:
    """Document metadata structure."""
    title: str
    document_type: DocumentType
    version: str = "1.0"
    date: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))
    status: str = "Draft"
    author: str = "System Generated"
    tags: List[str] = field(default_factory=list)
    related_files: List[str] = field(default_factory=list)


@dataclass
class DocumentSection:
    """Document section structure."""
    title: str
    content: str
    level: int = 2  # Header level (1-6)
    subsections: List['DocumentSection'] = field(default_factory=list)


# ============================================================================
# BASE DOCUMENT TEMPLATE CLASS
# ============================================================================

class DocumentTemplate:
    """Base class for all document templates."""
    
    def __init__(self, meta: DocumentMeta):
        self.meta = meta
        self.sections = []
        self.template_vars = {}
    
    def add_section(self, section: DocumentSection):
        """Add section to document."""
        self.sections.append(section)
    
    def set_template_var(self, key: str, value: Any):
        """Set template variable."""
        self.template_vars[key] = value
    
    def render_header(self) -> str:
        """Render document header."""
        header = f"# {self.meta.title}\n\n"
        
        # Add metadata table for formal documents
        if self.meta.document_type not in [DocumentType.README, DocumentType.CHANGELOG]:
            header += f"**Date:** {self.meta.date}  \n"
            header += f"**Version:** {self.meta.version}  \n"
            header += f"**Status:** {self.meta.status}  \n"
            
            if self.meta.tags:
                header += f"**Tags:** {', '.join(self.meta.tags)}  \n"
            header += "\n"
        
        return header
    
    def render_section(self, section: DocumentSection) -> str:
        """Render a document section."""
        header_prefix = "#" * section.level
        content = f"{header_prefix} {section.title}\n\n"
        content += f"{section.content}\n\n"
        
        # Render subsections
        for subsection in section.subsections:
            subsection.level = section.level + 1
            content += self.render_section(subsection)
        
        return content
    
    def render(self) -> str:
        """Render complete document."""
        content = self.render_header()
        
        for section in self.sections:
            content += self.render_section(section)
        
        # Add footer if appropriate
        footer = self.render_footer()
        if footer:
            content += footer
        
        return content
    
    def render_footer(self) -> str:
        """Render document footer."""
        return ""
    
    def save(self, output_path: Union[str, Path]) -> bool:
        """Save document to file."""
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(self.render())
            
            logger.info(f"Document saved to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save document: {e}")
            return False


# ============================================================================
# API REFERENCE TEMPLATE
# ============================================================================

class APIReferenceTemplate(DocumentTemplate):
    """
    Template for API reference documentation.
    
    Consolidates patterns from API_REFERENCE.md and similar files.
    """
    
    def __init__(self, title: str = "API Reference"):
        meta = DocumentMeta(
            title=title,
            document_type=DocumentType.API_REFERENCE,
            status="Active"
        )
        super().__init__(meta)
        self.quick_reference = {}
        self.api_endpoints = []
        self.core_classes = []
        self.utility_functions = []
    
    def add_quick_reference(self, category: str, items: Dict[str, str]):
        """Add quick reference items."""
        self.quick_reference[category] = items
    
    def add_api_endpoint(self, endpoint: str, method: str, description: str, 
                        parameters: Optional[List[Dict[str, str]]] = None,
                        example: Optional[str] = None):
        """Add API endpoint documentation."""
        self.api_endpoints.append({
            'endpoint': endpoint,
            'method': method,
            'description': description,
            'parameters': parameters or [],
            'example': example
        })
    
    def add_core_class(self, class_name: str, module: str, description: str,
                      methods: Optional[List[Dict[str, str]]] = None):
        """Add core class documentation."""
        self.core_classes.append({
            'name': class_name,
            'module': module,
            'description': description,
            'methods': methods or []
        })
    
    def render(self) -> str:
        """Render API reference document."""
        content = self.render_header()
        
        # Quick reference section
        if self.quick_reference:
            content += "## Quick Reference\n\n"
            for category, items in self.quick_reference.items():
                content += f"### {category}\n"
                for name, description in items.items():
                    content += f"- **{name}**: {description}\n"
                content += "\n"
        
        # Core classes section
        if self.core_classes:
            content += "## Core Classes\n\n"
            for cls in self.core_classes:
                content += f"### {cls['name']}\n"
                content += f"**Module**: `{cls['module']}`\n\n"
                content += f"{cls['description']}\n\n"
                
                if cls['methods']:
                    content += "**Methods:**\n"
                    for method in cls['methods']:
                        content += f"- `{method['name']}()`: {method['description']}\n"
                    content += "\n"
        
        # API endpoints section
        if self.api_endpoints:
            content += "## API Endpoints\n\n"
            for endpoint in self.api_endpoints:
                content += f"### {endpoint['method']} {endpoint['endpoint']}\n"
                content += f"{endpoint['description']}\n\n"
                
                if endpoint['parameters']:
                    content += "**Parameters:**\n"
                    for param in endpoint['parameters']:
                        content += f"- `{param['name']}` ({param['type']}): {param['description']}\n"
                    content += "\n"
                
                if endpoint['example']:
                    content += "**Example:**\n"
                    content += f"```\n{endpoint['example']}\n```\n\n"
        
        return content


# ============================================================================
# IMPLEMENTATION GUIDE TEMPLATE
# ============================================================================

class ImplementationGuideTemplate(DocumentTemplate):
    """
    Template for implementation guides.
    
    Consolidates patterns from COMPLETE_IMPLEMENTATION_GUIDE.md and similar files.
    """
    
    def __init__(self, title: str = "Implementation Guide"):
        meta = DocumentMeta(
            title=title,
            document_type=DocumentType.IMPLEMENTATION_GUIDE,
            status="Active"
        )
        super().__init__(meta)
        self.problem_statement = ""
        self.solution_overview = ""
        self.implementation_steps = []
        self.code_examples = []
        self.troubleshooting_items = []
    
    def set_problem_statement(self, problem: str):
        """Set the problem statement."""
        self.problem_statement = problem
    
    def set_solution_overview(self, solution: str):
        """Set the solution overview."""
        self.solution_overview = solution
    
    def add_implementation_step(self, title: str, description: str, 
                              code_example: Optional[str] = None,
                              notes: Optional[str] = None):
        """Add implementation step."""
        self.implementation_steps.append({
            'title': title,
            'description': description,
            'code_example': code_example,
            'notes': notes
        })
    
    def add_code_example(self, title: str, code: str, language: str = "python",
                        description: Optional[str] = None):
        """Add code example."""
        self.code_examples.append({
            'title': title,
            'code': code,
            'language': language,
            'description': description
        })
    
    def add_troubleshooting_item(self, issue: str, solution: str):
        """Add troubleshooting item."""
        self.troubleshooting_items.append({
            'issue': issue,
            'solution': solution
        })
    
    def render(self) -> str:
        """Render implementation guide."""
        content = self.render_header()
        
        # Overview section
        content += "## Overview\n\n"
        if self.solution_overview:
            content += f"{self.solution_overview}\n\n"
        
        # Problem statement
        if self.problem_statement:
            content += "## Problem Statement\n\n"
            content += f"{self.problem_statement}\n\n"
        
        # Implementation steps
        if self.implementation_steps:
            content += "## Implementation\n\n"
            for i, step in enumerate(self.implementation_steps, 1):
                content += f"### {i}. {step['title']}\n\n"
                content += f"{step['description']}\n\n"
                
                if step['code_example']:
                    content += "```python\n"
                    content += f"{step['code_example']}\n"
                    content += "```\n\n"
                
                if step['notes']:
                    content += f"**Note:** {step['notes']}\n\n"
        
        # Code examples
        if self.code_examples:
            content += "## Code Examples\n\n"
            for example in self.code_examples:
                content += f"### {example['title']}\n\n"
                if example['description']:
                    content += f"{example['description']}\n\n"
                
                content += f"```{example['language']}\n"
                content += f"{example['code']}\n"
                content += "```\n\n"
        
        # Troubleshooting
        if self.troubleshooting_items:
            content += "## Troubleshooting\n\n"
            for item in self.troubleshooting_items:
                content += f"**Issue:** {item['issue']}\n\n"
                content += f"**Solution:** {item['solution']}\n\n"
        
        return content


# ============================================================================
# PROGRESS REPORT TEMPLATE
# ============================================================================

class ProgressReportTemplate(DocumentTemplate):
    """
    Template for progress reports.
    
    Consolidates patterns from DRY_CONSOLIDATION_COMPLETE_SUMMARY.md and similar files.
    """
    
    def __init__(self, title: str = "Progress Report"):
        meta = DocumentMeta(
            title=title,
            document_type=DocumentType.PROGRESS_REPORT,
            status="Completed"
        )
        super().__init__(meta)
        self.executive_summary = ""
        self.objectives = []
        self.achievements = []
        self.metrics = {}
        self.tasks_completed = []
        self.next_steps = []
        self.challenges = []
    
    def set_executive_summary(self, summary: str):
        """Set executive summary."""
        self.executive_summary = summary
    
    def add_objective(self, objective: str):
        """Add project objective."""
        self.objectives.append(objective)
    
    def add_achievement(self, title: str, description: str, impact: Optional[str] = None):
        """Add achievement."""
        self.achievements.append({
            'title': title,
            'description': description,
            'impact': impact
        })
    
    def add_metric(self, name: str, value: str, improvement: Optional[str] = None):
        """Add quantified metric."""
        self.metrics[name] = {
            'value': value,
            'improvement': improvement
        }
    
    def add_completed_task(self, task: str, outcome: str):
        """Add completed task."""
        self.tasks_completed.append({
            'task': task,
            'outcome': outcome
        })
    
    def add_next_step(self, step: str, timeline: Optional[str] = None):
        """Add next step."""
        self.next_steps.append({
            'step': step,
            'timeline': timeline
        })
    
    def add_challenge(self, challenge: str, mitigation: Optional[str] = None):
        """Add challenge and mitigation."""
        self.challenges.append({
            'challenge': challenge,
            'mitigation': mitigation
        })
    
    def render(self) -> str:
        """Render progress report."""
        content = self.render_header()
        
        # Executive summary
        if self.executive_summary:
            content += "## 🎯 Executive Summary\n\n"
            content += f"{self.executive_summary}\n\n"
        
        # Objectives
        if self.objectives:
            content += "## 📋 Objectives\n\n"
            for obj in self.objectives:
                content += f"- {obj}\n"
            content += "\n"
        
        # Quantified results
        if self.metrics:
            content += "## 📊 Quantified Results\n\n"
            for name, data in self.metrics.items():
                improvement_text = f" ({data['improvement']})" if data['improvement'] else ""
                content += f"- **{name}**: {data['value']}{improvement_text}\n"
            content += "\n"
        
        # Major achievements
        if self.achievements:
            content += "## 🏆 Major Achievements\n\n"
            for achievement in self.achievements:
                content += f"### {achievement['title']}\n"
                content += f"{achievement['description']}\n"
                if achievement['impact']:
                    content += f"\n**Impact:** {achievement['impact']}\n"
                content += "\n"
        
        # Completed tasks
        if self.tasks_completed:
            content += "## ✅ Tasks Completed\n\n"
            for task in self.tasks_completed:
                content += f"- **{task['task']}**: {task['outcome']}\n"
            content += "\n"
        
        # Challenges
        if self.challenges:
            content += "## ⚠️ Challenges & Mitigations\n\n"
            for challenge in self.challenges:
                content += f"**Challenge:** {challenge['challenge']}\n\n"
                if challenge['mitigation']:
                    content += f"**Mitigation:** {challenge['mitigation']}\n"
                content += "\n"
        
        # Next steps
        if self.next_steps:
            content += "## 🔄 Next Steps\n\n"
            for step in self.next_steps:
                timeline_text = f" ({step['timeline']})" if step['timeline'] else ""
                content += f"- {step['step']}{timeline_text}\n"
            content += "\n"
        
        return content


# ============================================================================
# SYSTEM ARCHITECTURE TEMPLATE
# ============================================================================

class SystemArchitectureTemplate(DocumentTemplate):
    """
    Template for system architecture documentation.
    
    Consolidates patterns from COMPLETE_SYSTEM_ARCHITECTURE.md and similar files.
    """
    
    def __init__(self, title: str = "System Architecture"):
        meta = DocumentMeta(
            title=title,
            document_type=DocumentType.SYSTEM_ARCHITECTURE,
            status="Active"
        )
        super().__init__(meta)
        self.overview = ""
        self.architecture_principles = []
        self.components = []
        self.data_flow = ""
        self.security_considerations = []
        self.performance_characteristics = {}
        self.deployment_requirements = []
    
    def set_overview(self, overview: str):
        """Set system overview."""
        self.overview = overview
    
    def add_architecture_principle(self, name: str, description: str, rationale: Optional[str] = None):
        """Add architecture principle."""
        self.architecture_principles.append({
            'name': name,
            'description': description,
            'rationale': rationale
        })
    
    def add_component(self, name: str, description: str, responsibilities: List[str],
                     interfaces: Optional[List[str]] = None, dependencies: Optional[List[str]] = None):
        """Add system component."""
        self.components.append({
            'name': name,
            'description': description,
            'responsibilities': responsibilities,
            'interfaces': interfaces or [],
            'dependencies': dependencies or []
        })
    
    def set_data_flow(self, flow_description: str):
        """Set data flow description."""
        self.data_flow = flow_description
    
    def add_security_consideration(self, area: str, consideration: str, mitigation: Optional[str] = None):
        """Add security consideration."""
        self.security_considerations.append({
            'area': area,
            'consideration': consideration,
            'mitigation': mitigation
        })
    
    def add_performance_characteristic(self, metric: str, value: str, notes: Optional[str] = None):
        """Add performance characteristic."""
        self.performance_characteristics[metric] = {
            'value': value,
            'notes': notes
        }
    
    def add_deployment_requirement(self, requirement: str, details: Optional[str] = None):
        """Add deployment requirement."""
        self.deployment_requirements.append({
            'requirement': requirement,
            'details': details
        })
    
    def render(self) -> str:
        """Render system architecture document."""
        content = self.render_header()
        
        # Overview
        if self.overview:
            content += "## Overview\n\n"
            content += f"{self.overview}\n\n"
        
        # Architecture principles
        if self.architecture_principles:
            content += "## 🏗️ Architecture Principles\n\n"
            for principle in self.architecture_principles:
                content += f"### {principle['name']}\n"
                content += f"{principle['description']}\n"
                if principle['rationale']:
                    content += f"\n**Rationale:** {principle['rationale']}\n"
                content += "\n"
        
        # System components
        if self.components:
            content += "## 🔧 System Components\n\n"
            for component in self.components:
                content += f"### {component['name']}\n"
                content += f"{component['description']}\n\n"
                
                content += "**Responsibilities:**\n"
                for responsibility in component['responsibilities']:
                    content += f"- {responsibility}\n"
                content += "\n"
                
                if component['interfaces']:
                    content += "**Interfaces:**\n"
                    for interface in component['interfaces']:
                        content += f"- {interface}\n"
                    content += "\n"
                
                if component['dependencies']:
                    content += "**Dependencies:**\n"
                    for dependency in component['dependencies']:
                        content += f"- {dependency}\n"
                    content += "\n"
        
        # Data flow
        if self.data_flow:
            content += "## 📊 Data Flow\n\n"
            content += f"{self.data_flow}\n\n"
        
        # Performance characteristics
        if self.performance_characteristics:
            content += "## ⚡ Performance Characteristics\n\n"
            for metric, data in self.performance_characteristics.items():
                notes_text = f" - {data['notes']}" if data['notes'] else ""
                content += f"- **{metric}**: {data['value']}{notes_text}\n"
            content += "\n"
        
        # Security considerations
        if self.security_considerations:
            content += "## 🔒 Security Considerations\n\n"
            for consideration in self.security_considerations:
                content += f"### {consideration['area']}\n"
                content += f"**Consideration:** {consideration['consideration']}\n\n"
                if consideration['mitigation']:
                    content += f"**Mitigation:** {consideration['mitigation']}\n"
                content += "\n"
        
        # Deployment requirements
        if self.deployment_requirements:
            content += "## 🚀 Deployment Requirements\n\n"
            for req in self.deployment_requirements:
                content += f"- **{req['requirement']}**"
                if req['details']:
                    content += f": {req['details']}"
                content += "\n"
            content += "\n"
        
        return content


# ============================================================================
# VALIDATION REPORT TEMPLATE
# ============================================================================

class ValidationReportTemplate(DocumentTemplate):
    """
    Template for validation reports.
    
    Consolidates patterns from validation report files.
    """
    
    def __init__(self, title: str = "Validation Report"):
        meta = DocumentMeta(
            title=title,
            document_type=DocumentType.VALIDATION_REPORT,
            status="Completed"
        )
        super().__init__(meta)
        self.validation_scope = ""
        self.test_results = []
        self.performance_metrics = {}
        self.issues_found = []
        self.recommendations = []
        self.overall_status = "PENDING"
    
    def set_validation_scope(self, scope: str):
        """Set validation scope."""
        self.validation_scope = scope
    
    def add_test_result(self, test_name: str, status: str, details: Optional[str] = None,
                       execution_time: Optional[str] = None):
        """Add test result."""
        self.test_results.append({
            'name': test_name,
            'status': status,
            'details': details,
            'execution_time': execution_time
        })
    
    def add_performance_metric(self, metric: str, value: str, benchmark: Optional[str] = None):
        """Add performance metric."""
        self.performance_metrics[metric] = {
            'value': value,
            'benchmark': benchmark
        }
    
    def add_issue(self, severity: str, description: str, impact: Optional[str] = None,
                 recommendation: Optional[str] = None):
        """Add validation issue."""
        self.issues_found.append({
            'severity': severity,
            'description': description,
            'impact': impact,
            'recommendation': recommendation
        })
    
    def add_recommendation(self, recommendation: str, priority: str = "Medium"):
        """Add recommendation."""
        self.recommendations.append({
            'recommendation': recommendation,
            'priority': priority
        })
    
    def set_overall_status(self, status: str):
        """Set overall validation status."""
        self.overall_status = status
    
    def render(self) -> str:
        """Render validation report."""
        content = self.render_header()
        
        # Overall status
        status_emoji = "✅" if self.overall_status == "PASSED" else "❌" if self.overall_status == "FAILED" else "⏳"
        content += f"**Validation Status:** {status_emoji} {self.overall_status}\n\n"
        
        # Validation scope
        if self.validation_scope:
            content += "## 🎯 Validation Scope\n\n"
            content += f"{self.validation_scope}\n\n"
        
        # Test results
        if self.test_results:
            content += "## 🧪 Test Results\n\n"
            passed = len([t for t in self.test_results if t['status'] == 'PASSED'])
            total = len(self.test_results)
            content += f"**Overall: {passed}/{total} tests passed**\n\n"
            
            for test in self.test_results:
                status_emoji = "✅" if test['status'] == 'PASSED' else "❌"
                content += f"### {status_emoji} {test['name']}\n"
                content += f"**Status:** {test['status']}\n"
                if test['execution_time']:
                    content += f"**Execution Time:** {test['execution_time']}\n"
                if test['details']:
                    content += f"**Details:** {test['details']}\n"
                content += "\n"
        
        # Performance metrics
        if self.performance_metrics:
            content += "## ⚡ Performance Metrics\n\n"
            for metric, data in self.performance_metrics.items():
                benchmark_text = f" (Benchmark: {data['benchmark']})" if data['benchmark'] else ""
                content += f"- **{metric}**: {data['value']}{benchmark_text}\n"
            content += "\n"
        
        # Issues found
        if self.issues_found:
            content += "## ⚠️ Issues Found\n\n"
            for issue in self.issues_found:
                severity_emoji = "🔴" if issue['severity'] == 'HIGH' else "🟡" if issue['severity'] == 'MEDIUM' else "🟢"
                content += f"### {severity_emoji} {issue['severity']} Severity\n"
                content += f"**Description:** {issue['description']}\n"
                if issue['impact']:
                    content += f"**Impact:** {issue['impact']}\n"
                if issue['recommendation']:
                    content += f"**Recommendation:** {issue['recommendation']}\n"
                content += "\n"
        
        # Recommendations
        if self.recommendations:
            content += "## 💡 Recommendations\n\n"
            for rec in self.recommendations:
                priority_emoji = "🔴" if rec['priority'] == 'High' else "🟡" if rec['priority'] == 'Medium' else "🟢"
                content += f"- {priority_emoji} **{rec['priority']}**: {rec['recommendation']}\n"
            content += "\n"
        
        return content


# ============================================================================
# DOCUMENT FACTORY AND UTILITIES
# ============================================================================

class DocumentFactory:
    """Factory for creating document templates."""
    
    @staticmethod
    def create_api_reference(title: str = "API Reference") -> APIReferenceTemplate:
        """Create API reference template."""
        return APIReferenceTemplate(title)
    
    @staticmethod
    def create_implementation_guide(title: str = "Implementation Guide") -> ImplementationGuideTemplate:
        """Create implementation guide template."""
        return ImplementationGuideTemplate(title)
    
    @staticmethod
    def create_progress_report(title: str = "Progress Report") -> ProgressReportTemplate:
        """Create progress report template."""
        return ProgressReportTemplate(title)
    
    @staticmethod
    def create_system_architecture(title: str = "System Architecture") -> SystemArchitectureTemplate:
        """Create system architecture template."""
        return SystemArchitectureTemplate(title)
    
    @staticmethod
    def create_validation_report(title: str = "Validation Report") -> ValidationReportTemplate:
        """Create validation report template."""
        return ValidationReportTemplate(title)
    
    @staticmethod
    def create_document(doc_type: DocumentType, title: str) -> DocumentTemplate:
        """Create document template by type."""
        if doc_type == DocumentType.API_REFERENCE:
            return DocumentFactory.create_api_reference(title)
        elif doc_type == DocumentType.IMPLEMENTATION_GUIDE:
            return DocumentFactory.create_implementation_guide(title)
        elif doc_type == DocumentType.PROGRESS_REPORT:
            return DocumentFactory.create_progress_report(title)
        elif doc_type == DocumentType.SYSTEM_ARCHITECTURE:
            return DocumentFactory.create_system_architecture(title)
        elif doc_type == DocumentType.VALIDATION_REPORT:
            return DocumentFactory.create_validation_report(title)
        else:
            # Create basic document template
            meta = DocumentMeta(title=title, document_type=doc_type)
            return DocumentTemplate(meta)


# ============================================================================
# BULK OPERATIONS
# ============================================================================

def generate_documentation_suite(project_name: str, output_dir: Union[str, Path]) -> List[Path]:
    """
    Generate complete documentation suite.
    
    Args:
        project_name: Name of the project
        output_dir: Output directory
        
    Returns:
        List of generated file paths
        
    Example:
        files = generate_documentation_suite("My Project", "docs/")
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    generated_files = []
    
    # API Reference
    api_doc = DocumentFactory.create_api_reference(f"{project_name} API Reference")
    api_doc.add_quick_reference("Core Functions", {
        "process_data()": "Main data processing function",
        "validate_input()": "Input validation utility",
        "generate_report()": "Report generation function"
    })
    api_file = output_dir / "API_REFERENCE.md"
    if api_doc.save(api_file):
        generated_files.append(api_file)
    
    # Implementation Guide
    impl_doc = DocumentFactory.create_implementation_guide(f"{project_name} Implementation Guide")
    impl_doc.set_problem_statement("This guide covers the implementation of core system features.")
    impl_doc.add_implementation_step(
        "Setup Environment",
        "Configure the development environment with required dependencies.",
        "pip install -r requirements.txt"
    )
    impl_file = output_dir / "IMPLEMENTATION_GUIDE.md"
    if impl_doc.save(impl_file):
        generated_files.append(impl_file)
    
    # System Architecture
    arch_doc = DocumentFactory.create_system_architecture(f"{project_name} System Architecture")
    arch_doc.set_overview(f"This document describes the architecture of {project_name}.")
    arch_doc.add_architecture_principle(
        "Modularity",
        "System is designed with modular components for maintainability and scalability."
    )
    arch_file = output_dir / "SYSTEM_ARCHITECTURE.md"
    if arch_doc.save(arch_file):
        generated_files.append(arch_file)
    
    logger.info(f"Generated {len(generated_files)} documentation files in {output_dir}")
    return generated_files


def consolidate_existing_docs(source_dir: Union[str, Path], 
                            output_file: Union[str, Path],
                            doc_type: DocumentType) -> bool:
    """
    Consolidate existing documentation files.
    
    Args:
        source_dir: Directory containing source files
        output_file: Output consolidated file
        doc_type: Type of document to create
        
    Returns:
        True if successful
        
    Example:
        success = consolidate_existing_docs("docs/api/", "API_CONSOLIDATED.md", DocumentType.API_REFERENCE)
    """
    try:
        source_dir = Path(source_dir)
        if not source_dir.exists():
            logger.error(f"Source directory does not exist: {source_dir}")
            return False
        
        # Find markdown files
        md_files = list(source_dir.glob("*.md"))
        if not md_files:
            logger.warning(f"No markdown files found in {source_dir}")
            return False
        
        # Create consolidated document
        doc = DocumentFactory.create_document(doc_type, f"Consolidated {doc_type.value.replace('_', ' ').title()}")
        
        # Add sections from each file
        for md_file in md_files:
            try:
                with open(md_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                section = DocumentSection(
                    title=md_file.stem.replace('_', ' ').title(),
                    content=content
                )
                doc.add_section(section)
                
            except Exception as e:
                logger.warning(f"Failed to read {md_file}: {e}")
        
        # Save consolidated document
        return doc.save(output_file)
        
    except Exception as e:
        logger.error(f"Failed to consolidate docs: {e}")
        return False


# Example usage and testing
if __name__ == "__main__":
    # Test documentation templates
    logger.info("Testing documentation templates...")
    
    # Test API reference
    api_doc = DocumentFactory.create_api_reference("Test API Reference")
    api_doc.add_quick_reference("Core Classes", {
        "TestManager": "Manages test execution",
        "DataProcessor": "Processes input data"
    })
    api_doc.add_core_class(
        "TestManager",
        "utils.test_manager",
        "Centralized test management class",
        [{"name": "run_tests", "description": "Execute all tests"}]
    )
    
    # Test implementation guide
    impl_doc = DocumentFactory.create_implementation_guide("Test Implementation")
    impl_doc.set_problem_statement("We need to implement a robust testing framework.")
    impl_doc.add_implementation_step(
        "Create Test Structure",
        "Set up the basic test directory structure",
        "mkdir -p tests/{unit,integration,e2e}"
    )
    
    # Test progress report
    progress_doc = DocumentFactory.create_progress_report("Test Progress Report")
    progress_doc.set_executive_summary("All testing framework components completed successfully.")
    progress_doc.add_metric("Test Coverage", "95%", "15% improvement")
    progress_doc.add_achievement(
        "Automated Testing Pipeline",
        "Implemented CI/CD pipeline with automated testing",
        "Reduced deployment time by 50%"
    )
    
    # Save test documents
    test_dir = Path("test_docs")
    test_dir.mkdir(exist_ok=True)
    
    api_doc.save(test_dir / "test_api_reference.md")
    impl_doc.save(test_dir / "test_implementation_guide.md")
    progress_doc.save(test_dir / "test_progress_report.md")
    
    logger.info("✓ Documentation templates module is ready!")