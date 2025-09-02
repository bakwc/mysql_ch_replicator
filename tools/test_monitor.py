#!/usr/bin/env python3
"""
Phase 1.75 Test Infrastructure Monitoring System

This script provides proactive monitoring for:
1. Process health detection (binlog replicator process death)
2. Performance baseline tracking (45-second baseline with regression detection)
3. Test pattern validation (insert-before-start pattern compliance)

Usage:
    python tools/test_monitor.py --check-processes
    python tools/test_monitor.py --validate-patterns
    python tools/test_monitor.py --performance-baseline
    python tools/test_monitor.py --full-check
"""

import argparse
import os
import re
import subprocess
import time
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime


@dataclass
class MonitoringResult:
    """Monitoring result with severity and recommendations"""
    check_type: str
    status: str  # 'pass', 'warning', 'fail'
    message: str
    details: Optional[Dict] = None
    recommendations: List[str] = None


class TestInfrastructureMonitor:
    """Phase 1.75 Infrastructure monitoring system"""
    
    def __init__(self, project_root: str = None):
        self.project_root = Path(project_root) if project_root else Path(__file__).parent.parent
        self.baseline_runtime = 45  # seconds - established baseline
        self.warning_threshold = 60  # seconds - 33% increase triggers warning
        self.critical_threshold = 90  # seconds - 100% increase triggers alert
        
    def check_process_health(self) -> MonitoringResult:
        """Monitor for process death patterns and subprocess deadlock indicators"""
        try:
            # Check for running Docker containers
            result = subprocess.run(
                ['docker', 'ps', '--format', 'table {{.Names}}\t{{.Status}}'],
                capture_output=True, text=True, timeout=10
            )
            
            if result.returncode != 0:
                return MonitoringResult(
                    check_type="process_health",
                    status="fail",
                    message="Docker containers not accessible",
                    recommendations=["Run 'docker compose -f docker-compose-tests.yaml up -d'"]
                )
            
            # Check for test infrastructure containers
            required_containers = [
                'mysql_ch_replicator_src-replicator-1',
                'mysql_ch_replicator_src-mysql_db-1', 
                'mysql_ch_replicator_src-clickhouse_db-1'
            ]
            
            running_containers = []
            container_status = {}
            
            for line in result.stdout.split('\n')[1:]:  # Skip header
                if line.strip():
                    # Handle potential whitespace issues in docker output
                    parts = [part.strip() for part in line.split('\t') if part.strip()]
                    if len(parts) >= 2:
                        name, status = parts[0], parts[1]
                        container_status[name] = status
                        if any(req in name for req in required_containers):
                            running_containers.append(name)
                    else:
                        # Fallback: try splitting on multiple spaces for malformed output
                        parts = [part.strip() for part in re.split(r'\s{2,}', line) if part.strip()]
                        if len(parts) >= 2:
                            name, status = parts[0], parts[1]
                            container_status[name] = status
                            if any(req in name for req in required_containers):
                                running_containers.append(name)
            
            missing_containers = []
            unhealthy_containers = []
            
            for required in required_containers:
                found = False
                for running in running_containers:
                    if required in running:
                        found = True
                        if 'Up' not in container_status.get(running, ''):
                            unhealthy_containers.append(running)
                        break
                if not found:
                    missing_containers.append(required)
            
            if missing_containers or unhealthy_containers:
                status = "fail" if missing_containers else "warning"
                details = {
                    "missing_containers": missing_containers,
                    "unhealthy_containers": unhealthy_containers,
                    "all_containers": container_status
                }
                recommendations = [
                    "Restart Docker containers: docker compose -f docker-compose-tests.yaml up --force-recreate -d",
                    "Check container logs: docker logs [container_name]"
                ]
                return MonitoringResult(
                    check_type="process_health",
                    status=status,
                    message=f"Container issues detected: {len(missing_containers)} missing, {len(unhealthy_containers)} unhealthy",
                    details=details,
                    recommendations=recommendations
                )
            
            return MonitoringResult(
                check_type="process_health",
                status="pass",
                message=f"All {len(running_containers)} required containers healthy"
            )
            
        except subprocess.TimeoutExpired:
            return MonitoringResult(
                check_type="process_health",
                status="fail", 
                message="Docker command timeout - possible system overload",
                recommendations=["Check system resources", "Restart Docker daemon"]
            )
        except Exception as e:
            return MonitoringResult(
                check_type="process_health",
                status="fail",
                message=f"Process health check failed: {str(e)}",
                recommendations=["Check Docker installation", "Verify container configuration"]
            )
    
    def validate_test_patterns(self) -> MonitoringResult:
        """Scan test files for insert-before-start pattern compliance"""
        test_files = list(self.project_root.glob('tests/integration/**/*.py'))
        
        violations = []
        compliant_files = 0
        
        # Pattern to detect problematic insert-after-start sequences
        insert_after_start_pattern = re.compile(
            r'self\.start_replication\(\).*?self\.insert_multiple_records',
            re.MULTILINE | re.DOTALL
        )
        
        # Pattern to detect proper insert-before-start sequences  
        insert_before_start_pattern = re.compile(
            r'self\.insert_multiple_records.*?self\.start_replication\(\)',
            re.MULTILINE | re.DOTALL
        )
        
        for test_file in test_files:
            if test_file.name.startswith('__') or test_file.suffix != '.py':
                continue
                
            try:
                content = test_file.read_text(encoding='utf-8')
                
                # Skip files without replication tests
                if 'start_replication' not in content or 'insert_multiple_records' not in content:
                    continue
                
                # Check for violations (insert after start)
                violations_in_file = []
                for match in insert_after_start_pattern.finditer(content):
                    line_num = content[:match.start()].count('\n') + 1
                    violations_in_file.append({
                        'file': str(test_file.relative_to(self.project_root)),
                        'line': line_num,
                        'context': match.group()[:100] + '...'
                    })
                
                if violations_in_file:
                    violations.extend(violations_in_file)
                else:
                    # Verify it uses the correct pattern
                    if insert_before_start_pattern.search(content):
                        compliant_files += 1
                        
            except Exception as e:
                violations.append({
                    'file': str(test_file.relative_to(self.project_root)),
                    'line': 0,
                    'error': f"Failed to analyze: {str(e)}"
                })
        
        if violations:
            return MonitoringResult(
                check_type="pattern_validation",
                status="fail",
                message=f"Found {len(violations)} pattern violations across {len(set(v['file'] for v in violations))} files",
                details={"violations": violations, "compliant_files": compliant_files},
                recommendations=[
                    "Fix violations using insert-before-start pattern",
                    "See COMPLETED_TEST_FIXING_GUIDE.md for examples",
                    "Run pattern validation before commits"
                ]
            )
        
        return MonitoringResult(
            check_type="pattern_validation", 
            status="pass",
            message=f"All {compliant_files} test files use correct insert-before-start pattern"
        )
    
    def check_performance_baseline(self) -> MonitoringResult:
        """Check current test performance against 45-second baseline"""
        try:
            # Run a quick subset of tests to measure performance
            start_time = time.time()
            
            # Run a representative test to measure infrastructure performance
            result = subprocess.run([
                'docker', 'exec', '-i', 
                'mysql_ch_replicator_src-replicator-1',  # Try common container name
                'python3', '-m', 'pytest', 
                'tests/integration/data_integrity/test_data_consistency.py::TestDataConsistency::test_checksum_validation_basic_data',
                '-v', '--tb=short'
            ], capture_output=True, text=True, timeout=120)
            
            runtime = time.time() - start_time
            
            if result.returncode != 0:
                # Try alternative container name
                alt_result = subprocess.run([
                    'docker', 'ps', '--format', '{{.Names}}'
                ], capture_output=True, text=True)
                
                replicator_container = None
                for line in alt_result.stdout.split('\n'):
                    if 'replicator' in line and 'mysql_ch_replicator' in line:
                        replicator_container = line.strip()
                        break
                
                if replicator_container:
                    result = subprocess.run([
                        'docker', 'exec', '-i', replicator_container,
                        'python3', '-m', 'pytest',
                        'tests/integration/data_integrity/test_data_consistency.py::TestDataConsistency::test_checksum_validation_basic_data', 
                        '-v', '--tb=short'
                    ], capture_output=True, text=True, timeout=120)
                    
                    runtime = time.time() - start_time
            
            if result.returncode != 0:
                return MonitoringResult(
                    check_type="performance_baseline",
                    status="warning",
                    message=f"Performance test failed (runtime: {runtime:.1f}s), but infrastructure may still be functional",
                    details={"runtime": runtime, "error_output": result.stderr[:500]},
                    recommendations=[
                        "Check container health with: docker ps",
                        "Run full test suite to verify: ./run_tests.sh"
                    ]
                )
            
            # Evaluate performance against baseline
            if runtime <= self.warning_threshold:
                status = "pass"
                message = f"Performance within acceptable range: {runtime:.1f}s (baseline: {self.baseline_runtime}s)"
            elif runtime <= self.critical_threshold:
                status = "warning" 
                message = f"Performance degraded: {runtime:.1f}s (>{self.warning_threshold}s threshold)"
            else:
                status = "fail"
                message = f"Critical performance regression: {runtime:.1f}s (>{self.critical_threshold}s threshold)"
            
            recommendations = []
            if runtime > self.warning_threshold:
                recommendations = [
                    "Check system resources (CPU, memory, disk)",
                    "Restart Docker containers",
                    "Review recent changes that may impact performance"
                ]
            
            return MonitoringResult(
                check_type="performance_baseline",
                status=status,
                message=message,
                details={"runtime": runtime, "baseline": self.baseline_runtime},
                recommendations=recommendations
            )
            
        except subprocess.TimeoutExpired:
            return MonitoringResult(
                check_type="performance_baseline",
                status="fail",
                message="Performance test timed out (>120s) - critical regression detected",
                recommendations=[
                    "Investigate infrastructure deadlock issues",
                    "Check for process death patterns",
                    "Review recent infrastructure changes"
                ]
            )
        except Exception as e:
            return MonitoringResult(
                check_type="performance_baseline",
                status="fail", 
                message=f"Performance monitoring failed: {str(e)}",
                recommendations=["Check Docker setup", "Verify test environment"]
            )
    
    def full_monitoring_check(self) -> List[MonitoringResult]:
        """Run all monitoring checks and return comprehensive results"""
        print("🔍 Running Phase 1.75 Infrastructure Monitoring...")
        
        results = []
        
        print("  Checking process health...")
        results.append(self.check_process_health())
        
        print("  Validating test patterns...")
        results.append(self.validate_test_patterns())
        
        print("  Checking performance baseline...")
        results.append(self.check_performance_baseline())
        
        return results
    
    def format_monitoring_report(self, results: List[MonitoringResult]) -> str:
        """Format monitoring results into a readable report"""
        report = []
        report.append("=" * 80)
        report.append("Phase 1.75 Infrastructure Monitoring Report")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 80)
        
        # Summary
        pass_count = sum(1 for r in results if r.status == 'pass')
        warning_count = sum(1 for r in results if r.status == 'warning') 
        fail_count = sum(1 for r in results if r.status == 'fail')
        
        report.append(f"\nSUMMARY: {pass_count} passed, {warning_count} warnings, {fail_count} failures")
        
        if fail_count == 0 and warning_count == 0:
            report.append("✅ ALL CHECKS PASSED - Infrastructure is stable")
        elif fail_count == 0:
            report.append("⚠️  WARNINGS DETECTED - Review recommendations") 
        else:
            report.append("❌ FAILURES DETECTED - Immediate action required")
        
        # Detailed results
        for result in results:
            report.append(f"\n{'-' * 60}")
            status_icon = {"pass": "✅", "warning": "⚠️", "fail": "❌"}[result.status]
            report.append(f"{status_icon} {result.check_type.upper()}: {result.message}")
            
            if result.details:
                report.append(f"Details: {json.dumps(result.details, indent=2)}")
            
            if result.recommendations:
                report.append("Recommendations:")
                for rec in result.recommendations:
                    report.append(f"  • {rec}")
        
        report.append("\n" + "=" * 80)
        return "\n".join(report)


def main():
    parser = argparse.ArgumentParser(description="Phase 1.75 Test Infrastructure Monitoring")
    parser.add_argument('--check-processes', action='store_true', help='Check process health only')
    parser.add_argument('--validate-patterns', action='store_true', help='Validate test patterns only')
    parser.add_argument('--performance-baseline', action='store_true', help='Check performance baseline only')
    parser.add_argument('--full-check', action='store_true', help='Run all monitoring checks')
    parser.add_argument('--project-root', help='Project root directory')
    
    args = parser.parse_args()
    
    if not any([args.check_processes, args.validate_patterns, args.performance_baseline, args.full_check]):
        args.full_check = True  # Default to full check
    
    monitor = TestInfrastructureMonitor(args.project_root)
    results = []
    
    if args.check_processes or args.full_check:
        results.append(monitor.check_process_health())
    
    if args.validate_patterns or args.full_check:
        results.append(monitor.validate_test_patterns())
    
    if args.performance_baseline or args.full_check:
        results.append(monitor.check_performance_baseline())
    
    # Print report
    report = monitor.format_monitoring_report(results)
    print(report)
    
    # Exit with appropriate code
    has_failures = any(r.status == 'fail' for r in results)
    has_warnings = any(r.status == 'warning' for r in results)
    
    if has_failures:
        exit(1)
    elif has_warnings:
        exit(2)
    else:
        exit(0)


if __name__ == "__main__":
    main()