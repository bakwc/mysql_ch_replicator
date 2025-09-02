#!/usr/bin/env python3
"""
Phase 1.75 Test Pattern Validation and Enforcement

This script provides automated validation and enforcement of the insert-before-start
pattern established in Phase 1 fixes. It can be used as:
- Pre-commit hook for pattern validation
- Standalone pattern checker
- Template generator for new tests

Usage:
    python tools/test_pattern_validator.py --validate tests/
    python tools/test_pattern_validator.py --generate-template TestNewFeature
    python tools/test_pattern_validator.py --fix-violations tests/integration/data_types/
"""

import argparse
import re
import subprocess
from pathlib import Path
from typing import List, Tuple, Dict
from dataclasses import dataclass


@dataclass
class PatternViolation:
    """Represents a test pattern violation"""
    file_path: str
    line_number: int
    violation_type: str
    context: str
    suggestion: str


class TestPatternValidator:
    """Validates and enforces insert-before-start test patterns"""
    
    def __init__(self, project_root: str = None):
        self.project_root = Path(project_root) if project_root else Path(__file__).parent.parent
        
        # Regex patterns for detecting anti-patterns
        self.insert_after_start_pattern = re.compile(
            r'(self\.start_replication\(\).*?)(self\.insert_multiple_records.*?)(\n.*?self\.wait_for_table_sync)',
            re.MULTILINE | re.DOTALL
        )
        
        # Pattern for proper insert-before-start
        self.insert_before_start_pattern = re.compile(
            r'(self\.insert_multiple_records.*?)(self\.start_replication\(\))',
            re.MULTILINE | re.DOTALL
        )
        
    def validate_file(self, file_path: Path) -> List[PatternViolation]:
        """Validate a single test file for pattern compliance"""
        violations = []
        
        try:
            content = file_path.read_text(encoding='utf-8')
            
            # Skip files without replication tests
            if 'start_replication' not in content or 'insert_multiple_records' not in content:
                return violations
            
            # Handle both absolute and relative paths
            try:
                relative_path = str(file_path.relative_to(self.project_root))
            except ValueError:
                # If relative_to fails, use the path as-is
                relative_path = str(file_path)
                
            # Check for insert-after-start violations
            for match in self.insert_after_start_pattern.finditer(content):
                line_num = content[:match.start()].count('\n') + 1
                context = self._get_context_lines(content, match.start(), match.end())
                
                violation = PatternViolation(
                    file_path=relative_path,
                    line_number=line_num,
                    violation_type="insert_after_start",
                    context=context,
                    suggestion="Move all data insertion before start_replication() call"
                )
                violations.append(violation)
                
            # Check for missing wait_for_table_sync after start_replication
            start_repl_pattern = re.compile(r'self\.start_replication\(\)(?!\s*\n\s*self\.wait_for_table_sync)')
            for match in start_repl_pattern.finditer(content):
                line_num = content[:match.start()].count('\n') + 1
                # Only flag if there's insert_multiple_records in the same function
                function_context = self._get_function_context(content, match.start())
                if 'insert_multiple_records' in function_context:
                    violation = PatternViolation(
                        file_path=relative_path,
                        line_number=line_num,
                        violation_type="missing_wait_sync",
                        context=self._get_context_lines(content, match.start(), match.end()),
                        suggestion="Add wait_for_table_sync() immediately after start_replication()"
                    )
                    violations.append(violation)
                    
        except Exception as e:
            violation = PatternViolation(
                file_path=relative_path,
                line_number=0,
                violation_type="parse_error",
                context=f"Error reading file: {str(e)}",
                suggestion="Check file encoding and syntax"
            )
            violations.append(violation)
            
        return violations
    
    def validate_directory(self, directory: Path) -> Dict[str, List[PatternViolation]]:
        """Validate all test files in a directory"""
        results = {}
        
        test_files = list(directory.glob('**/*.py'))
        for test_file in test_files:
            if test_file.name.startswith('__') or 'test_' not in test_file.name:
                continue
                
            violations = self.validate_file(test_file)
            if violations:
                try:
                    relative_path = str(test_file.relative_to(self.project_root))
                except ValueError:
                    relative_path = str(test_file)
                results[relative_path] = violations
                
        return results
    
    def generate_test_template(self, class_name: str) -> str:
        """Generate a compliant test template following insert-before-start pattern"""
        template = f'''"""Test template following Phase 1.75 insert-before-start pattern"""

import pytest
from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME
from tests.fixtures import TableSchemas, TestDataGenerator


class {class_name}(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test class following Phase 1.75 best practices"""

    @pytest.mark.integration
    def test_example_scenario(self):
        """Example test following insert-before-start pattern"""
        # 1. Setup - Create schema
        self.create_basic_table(TEST_TABLE_NAME)
        
        # 2. Prepare ALL test data before replication starts
        # ✅ CRITICAL: Insert ALL data before start_replication()
        test_data = [
            {{"name": "test_record_1", "age": 25}},
            {{"name": "test_record_2", "age": 30}},
            {{"name": "test_record_3", "age": 35}}
        ]
        self.insert_multiple_records(TEST_TABLE_NAME, test_data)
        
        # 3. Start replication AFTER all data is ready
        # ✅ PATTERN: start_replication() comes after data insertion
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(test_data))
        
        # 4. Verify results
        self.verify_record_exists(TEST_TABLE_NAME, "name='test_record_1'", {{"age": 25}})
        self.verify_record_exists(TEST_TABLE_NAME, "name='test_record_2'", {{"age": 30}})
        self.verify_record_exists(TEST_TABLE_NAME, "name='test_record_3'", {{"age": 35}})

    @pytest.mark.integration  
    def test_advanced_scenario(self):
        """Advanced test with multiple operations, still using insert-before-start"""
        # 1. Setup
        self.create_basic_table(TEST_TABLE_NAME)
        
        # 2. Prepare complex test data scenario
        # ✅ PATTERN: Even complex scenarios insert ALL data first
        initial_data = [{{"name": f"initial_{{i}}", "age": 20 + i}} for i in range(5)]
        additional_data = [{{"name": f"additional_{{i}}", "age": 30 + i}} for i in range(3)]
        
        # Combine all data that will be needed for the test
        all_test_data = initial_data + additional_data
        self.insert_multiple_records(TEST_TABLE_NAME, all_test_data)
        
        # 3. Start replication with complete dataset
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(all_test_data))
        
        # 4. Verify all data scenarios
        # Test initial data
        for i, record in enumerate(initial_data):
            self.verify_record_exists(TEST_TABLE_NAME, f"name='initial_{{i}}'", {{"age": 20 + i}})
            
        # Test additional data  
        for i, record in enumerate(additional_data):
            self.verify_record_exists(TEST_TABLE_NAME, f"name='additional_{{i}}'", {{"age": 30 + i}})
            
        # Verify total count
        ch_records = self.ch.select(TEST_TABLE_NAME)
        assert len(ch_records) == len(all_test_data), f"Expected {{len(all_test_data)}}, got {{len(ch_records)}}"
'''
        return template
    
    def _get_context_lines(self, content: str, start: int, end: int, context_size: int = 3) -> str:
        """Get context lines around a match for better violation reporting"""
        lines = content.split('\n')
        start_line = content[:start].count('\n')
        end_line = content[:end].count('\n')
        
        context_start = max(0, start_line - context_size)
        context_end = min(len(lines), end_line + context_size + 1)
        
        context_lines = []
        for i in range(context_start, context_end):
            marker = ">>> " if start_line <= i <= end_line else "    "
            context_lines.append(f"{marker}{i+1:3d}: {lines[i]}")
            
        return "\\n".join(context_lines)
    
    def _get_function_context(self, content: str, position: int) -> str:
        """Get the function context around a position"""
        lines = content.split('\\n')
        pos_line = content[:position].count('\\n')
        
        # Find function start (def keyword)
        func_start = pos_line
        while func_start > 0 and not lines[func_start].strip().startswith('def '):
            func_start -= 1
            
        # Find function end (next def or class, or end of file)
        func_end = pos_line
        while func_end < len(lines) - 1:
            func_end += 1
            if lines[func_end].strip().startswith(('def ', 'class ', '@')):
                break
                
        return '\\n'.join(lines[func_start:func_end])
    
    def create_pre_commit_hook(self) -> str:
        """Generate a pre-commit hook script for pattern validation"""
        hook_script = '''#!/usr/bin/env python3
"""
Phase 1.75 Pre-commit Hook for Test Pattern Validation
Ensures all test files follow the insert-before-start pattern
"""

import sys
import subprocess
from pathlib import Path

def main():
    # Get list of staged Python files in tests directory
    result = subprocess.run(['git', 'diff', '--cached', '--name-only'], 
                          capture_output=True, text=True)
    
    staged_files = [f for f in result.stdout.split('\\n') 
                   if f.startswith('tests/') and f.endswith('.py') and 'test_' in f]
    
    if not staged_files:
        return 0  # No test files to check
        
    print("🔍 Phase 1.75: Validating test patterns...")
    
    # Run pattern validator on staged files
    cmd = ['python3', 'tools/test_pattern_validator.py', '--validate'] + staged_files
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print("❌ Test pattern violations detected:")
        print(result.stdout)
        print("\\n💡 Fix violations before committing")
        print("📚 See COMPLETED_TEST_FIXING_GUIDE.md for pattern examples")
        return 1
        
    print("✅ All test patterns validated")
    return 0

if __name__ == "__main__":
    sys.exit(main())
'''
        return hook_script
    
    def format_validation_report(self, results: Dict[str, List[PatternViolation]]) -> str:
        """Format validation results into a readable report"""
        if not results:
            return "✅ No pattern violations detected - all tests follow insert-before-start pattern"
            
        report = []
        report.append("❌ Test Pattern Violations Detected")
        report.append("=" * 50)
        
        total_violations = sum(len(violations) for violations in results.values())
        report.append(f"Found {total_violations} violations across {len(results)} files\\n")
        
        for file_path, violations in results.items():
            report.append(f"📁 {file_path}:")
            for violation in violations:
                report.append(f"  Line {violation.line_number}: {violation.violation_type}")
                report.append(f"  💡 {violation.suggestion}")
                report.append(f"  Context:")
                for line in violation.context.split('\\n'):
                    report.append(f"    {line}")
                report.append("")
                
        report.append("🔧 How to Fix:")
        report.append("1. Move all insert_multiple_records() calls before start_replication()")
        report.append("2. Combine multiple data insertions into single call")
        report.append("3. Add wait_for_table_sync() immediately after start_replication()")
        report.append("4. See COMPLETED_TEST_FIXING_GUIDE.md for examples")
        
        return "\\n".join(report)


def main():
    parser = argparse.ArgumentParser(description="Phase 1.75 Test Pattern Validation and Enforcement")
    parser.add_argument('--validate', nargs='+', help='Validate test files or directories')
    parser.add_argument('--generate-template', help='Generate compliant test template with given class name')
    parser.add_argument('--create-hook', action='store_true', help='Create pre-commit hook script')
    parser.add_argument('--project-root', help='Project root directory')
    
    args = parser.parse_args()
    
    if not any([args.validate, args.generate_template, args.create_hook]):
        parser.print_help()
        return 1
        
    validator = TestPatternValidator(args.project_root)
    
    if args.validate:
        all_results = {}
        for path_str in args.validate:
            path = Path(path_str)
            if path.is_file():
                violations = validator.validate_file(path)
                if violations:
                    all_results[str(path)] = violations
            elif path.is_dir():
                results = validator.validate_directory(path)
                all_results.update(results)
            else:
                print(f"⚠️  Path not found: {path}")
                
        report = validator.format_validation_report(all_results)
        print(report)
        
        return 1 if all_results else 0
        
    elif args.generate_template:
        template = validator.generate_test_template(args.generate_template)
        print(template)
        return 0
        
    elif args.create_hook:
        hook_script = validator.create_pre_commit_hook()
        hook_path = Path('.git/hooks/pre-commit')
        hook_path.write_text(hook_script)
        hook_path.chmod(0o755)
        print(f"✅ Created pre-commit hook: {hook_path}")
        return 0


if __name__ == "__main__":
    exit(main())