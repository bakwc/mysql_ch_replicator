#!/usr/bin/env python3
"""
Phase 1.75 Infrastructure Rollback and Recovery System

This script provides automated rollback and recovery capabilities for:
1. Infrastructure state recovery after failed tests
2. Process cleanup and restart procedures  
3. Configuration reset to known-good state
4. Emergency infrastructure reset

Usage:
    python tools/infrastructure_rollback.py --reset-processes
    python tools/infrastructure_rollback.py --cleanup-containers  
    python tools/infrastructure_rollback.py --emergency-reset
    python tools/infrastructure_rollback.py --validate-recovery
"""

import argparse
import subprocess
import time
import shutil
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass


@dataclass
class RecoveryAction:
    """Represents a recovery action with success/failure status"""
    action: str
    status: str  # 'success', 'failed', 'skipped'
    message: str
    duration_seconds: float = 0.0


class InfrastructureRecoveryManager:
    """Phase 1.75 infrastructure rollback and recovery system"""
    
    def __init__(self, project_root: str = None):
        self.project_root = Path(project_root) if project_root else Path(__file__).parent.parent
        self.compose_file = self.project_root / "docker-compose-tests.yaml"
        
    def reset_processes(self) -> List[RecoveryAction]:
        """Reset all replication processes to clean state"""
        actions = []
        
        # Stop any running processes
        start_time = time.time()
        try:
            result = subprocess.run([
                'docker', 'compose', '-f', str(self.compose_file), 'stop'
            ], capture_output=True, text=True, timeout=60)
            
            duration = time.time() - start_time
            if result.returncode == 0:
                actions.append(RecoveryAction(
                    action="stop_containers",
                    status="success", 
                    message="All containers stopped successfully",
                    duration_seconds=duration
                ))
            else:
                actions.append(RecoveryAction(
                    action="stop_containers",
                    status="failed",
                    message=f"Failed to stop containers: {result.stderr}",
                    duration_seconds=duration
                ))
                
        except subprocess.TimeoutExpired:
            actions.append(RecoveryAction(
                action="stop_containers",
                status="failed", 
                message="Container stop operation timed out",
                duration_seconds=60.0
            ))
        
        # Force kill any remaining processes
        start_time = time.time()
        try:
            result = subprocess.run([
                'docker', 'compose', '-f', str(self.compose_file), 'kill'
            ], capture_output=True, text=True, timeout=30)
            
            duration = time.time() - start_time
            actions.append(RecoveryAction(
                action="force_kill_containers",
                status="success",
                message="Force kill completed",
                duration_seconds=duration
            ))
            
        except Exception as e:
            actions.append(RecoveryAction(
                action="force_kill_containers", 
                status="failed",
                message=f"Force kill failed: {str(e)}",
                duration_seconds=time.time() - start_time
            ))
        
        # Remove containers
        start_time = time.time()
        try:
            result = subprocess.run([
                'docker', 'compose', '-f', str(self.compose_file), 'rm', '-f'
            ], capture_output=True, text=True, timeout=30)
            
            duration = time.time() - start_time
            actions.append(RecoveryAction(
                action="remove_containers",
                status="success",
                message="Containers removed",
                duration_seconds=duration
            ))
            
        except Exception as e:
            actions.append(RecoveryAction(
                action="remove_containers",
                status="failed", 
                message=f"Container removal failed: {str(e)}",
                duration_seconds=time.time() - start_time
            ))
            
        return actions
    
    def cleanup_filesystem(self) -> List[RecoveryAction]:
        """Clean up test filesystem artifacts"""
        actions = []
        
        # Clean up binlog directories (Phase 1.5 fix - /tmp/binlog paths)
        binlog_patterns = [
            "/tmp/binlog*",
            "binlog*",  # Local directory cleanup
            "*.log",
            "*.pid"
        ]
        
        for pattern in binlog_patterns:
            start_time = time.time()
            try:
                if pattern.startswith('/tmp/'):
                    # System temp cleanup
                    result = subprocess.run([
                        'find', '/tmp', '-name', pattern.split('/')[-1], '-type', 'd', '-exec', 'rm', '-rf', '{}', '+'
                    ], capture_output=True, text=True, timeout=10)
                else:
                    # Local project cleanup
                    import glob
                    matches = glob.glob(str(self.project_root / pattern))
                    for match in matches:
                        path = Path(match)
                        if path.exists():
                            if path.is_dir():
                                shutil.rmtree(path)
                            else:
                                path.unlink()
                
                duration = time.time() - start_time
                actions.append(RecoveryAction(
                    action=f"cleanup_{pattern}",
                    status="success",
                    message=f"Cleaned up {pattern} artifacts",
                    duration_seconds=duration
                ))
                
            except Exception as e:
                actions.append(RecoveryAction(
                    action=f"cleanup_{pattern}",
                    status="failed",
                    message=f"Failed to clean {pattern}: {str(e)}",
                    duration_seconds=time.time() - start_time
                ))
        
        return actions
    
    def restart_infrastructure(self) -> List[RecoveryAction]:
        """Restart infrastructure with fresh containers"""
        actions = []
        
        # Start containers with force recreate
        start_time = time.time()
        try:
            result = subprocess.run([
                'docker', 'compose', '-f', str(self.compose_file), 
                'up', '--force-recreate', '--wait', '-d'
            ], capture_output=True, text=True, timeout=300)  # 5 minute timeout
            
            duration = time.time() - start_time
            if result.returncode == 0:
                actions.append(RecoveryAction(
                    action="restart_infrastructure",
                    status="success",
                    message="Infrastructure restarted successfully",
                    duration_seconds=duration
                ))
            else:
                actions.append(RecoveryAction(
                    action="restart_infrastructure", 
                    status="failed",
                    message=f"Infrastructure restart failed: {result.stderr}",
                    duration_seconds=duration
                ))
                
        except subprocess.TimeoutExpired:
            actions.append(RecoveryAction(
                action="restart_infrastructure",
                status="failed",
                message="Infrastructure restart timed out",
                duration_seconds=300.0
            ))
        except Exception as e:
            actions.append(RecoveryAction(
                action="restart_infrastructure",
                status="failed", 
                message=f"Infrastructure restart error: {str(e)}",
                duration_seconds=time.time() - start_time
            ))
            
        return actions
    
    def validate_recovery(self) -> List[RecoveryAction]:
        """Validate that recovery was successful"""
        actions = []
        
        # Check container health
        start_time = time.time()
        try:
            result = subprocess.run([
                'docker', 'ps', '--format', 'table {{.Names}}\t{{.Status}}'
            ], capture_output=True, text=True, timeout=30)
            
            duration = time.time() - start_time
            if result.returncode == 0:
                required_containers = [
                    'mysql_ch_replicator_src-replicator-1',
                    'mysql_ch_replicator_src-mysql_db-1',
                    'mysql_ch_replicator_src-clickhouse_db-1'
                ]
                
                running_containers = []
                for line in result.stdout.split('\n')[1:]:  # Skip header
                    if line.strip():
                        parts = line.split('\t')
                        if len(parts) >= 2:
                            name, status = parts[0], parts[1]
                            if any(req in name for req in required_containers) and 'Up' in status:
                                running_containers.append(name)
                
                if len(running_containers) >= 3:  # At least the core containers
                    actions.append(RecoveryAction(
                        action="validate_containers",
                        status="success",
                        message=f"Found {len(running_containers)} healthy containers",
                        duration_seconds=duration
                    ))
                else:
                    actions.append(RecoveryAction(
                        action="validate_containers",
                        status="failed",
                        message=f"Only {len(running_containers)} containers healthy, expected 3+",
                        duration_seconds=duration
                    ))
            else:
                actions.append(RecoveryAction(
                    action="validate_containers",
                    status="failed",
                    message=f"Container validation failed: {result.stderr}",
                    duration_seconds=duration
                ))
                
        except Exception as e:
            actions.append(RecoveryAction(
                action="validate_containers",
                status="failed",
                message=f"Container validation error: {str(e)}",
                duration_seconds=time.time() - start_time
            ))
        
        # Test basic connectivity
        start_time = time.time()
        try:
            # Try to run a simple command in the replicator container
            result = subprocess.run([
                'docker', 'exec', 
                'mysql_ch_replicator_src-replicator-1',  
                'python3', '-c', 'print("Infrastructure connectivity test")'
            ], capture_output=True, text=True, timeout=30)
            
            duration = time.time() - start_time
            if result.returncode == 0:
                actions.append(RecoveryAction(
                    action="validate_connectivity",
                    status="success",
                    message="Container connectivity verified",
                    duration_seconds=duration
                ))
            else:
                actions.append(RecoveryAction(
                    action="validate_connectivity",
                    status="failed",
                    message=f"Connectivity test failed: {result.stderr}",
                    duration_seconds=duration
                ))
                
        except Exception as e:
            actions.append(RecoveryAction(
                action="validate_connectivity",
                status="failed",
                message=f"Connectivity validation error: {str(e)}",
                duration_seconds=time.time() - start_time
            ))
            
        return actions
    
    def emergency_reset(self) -> List[RecoveryAction]:
        """Perform complete emergency infrastructure reset"""
        print("🚨 Performing emergency infrastructure reset...")
        
        all_actions = []
        
        print("Step 1: Resetting processes...")
        all_actions.extend(self.reset_processes())
        
        print("Step 2: Cleaning filesystem...")
        all_actions.extend(self.cleanup_filesystem())
        
        # Wait for cleanup to settle
        time.sleep(2)
        
        print("Step 3: Restarting infrastructure...")
        all_actions.extend(self.restart_infrastructure())
        
        # Wait for services to initialize
        print("Waiting for services to initialize...")
        time.sleep(10)
        
        print("Step 4: Validating recovery...")
        all_actions.extend(self.validate_recovery())
        
        return all_actions
    
    def format_recovery_report(self, actions: List[RecoveryAction]) -> str:
        """Format recovery actions into readable report"""
        report = []
        report.append("=" * 80)
        report.append("Phase 1.75 Infrastructure Recovery Report")
        report.append("=" * 80)
        
        # Summary
        success_count = sum(1 for a in actions if a.status == 'success')
        failed_count = sum(1 for a in actions if a.status == 'failed')
        total_time = sum(a.duration_seconds for a in actions)
        
        report.append(f"Actions: {success_count} successful, {failed_count} failed")
        report.append(f"Total time: {total_time:.1f}s")
        
        if failed_count == 0:
            report.append("✅ RECOVERY SUCCESSFUL")
        else:
            report.append("❌ RECOVERY PARTIAL - Manual intervention may be required")
        
        # Detailed actions
        report.append("\nDetailed Actions:")
        for action in actions:
            status_icon = {"success": "✅", "failed": "❌", "skipped": "⏭️"}[action.status]
            report.append(f"{status_icon} {action.action}: {action.message} ({action.duration_seconds:.1f}s)")
        
        if failed_count > 0:
            report.append("\n🔧 Manual Recovery Steps:")
            report.append("1. Check Docker daemon status: systemctl status docker")
            report.append("2. Check available disk space: df -h")
            report.append("3. Check Docker logs: docker compose logs")
            report.append("4. Manual restart: docker compose -f docker-compose-tests.yaml up --force-recreate -d")
        
        report.append("=" * 80)
        return "\n".join(report)


def main():
    parser = argparse.ArgumentParser(description="Phase 1.75 Infrastructure Recovery")
    parser.add_argument('--reset-processes', action='store_true', help='Reset replication processes')
    parser.add_argument('--cleanup-containers', action='store_true', help='Clean up containers and filesystem')  
    parser.add_argument('--restart-infrastructure', action='store_true', help='Restart infrastructure')
    parser.add_argument('--validate-recovery', action='store_true', help='Validate recovery success')
    parser.add_argument('--emergency-reset', action='store_true', help='Perform complete emergency reset')
    parser.add_argument('--project-root', help='Project root directory')
    
    args = parser.parse_args()
    
    if not any([args.reset_processes, args.cleanup_containers, args.restart_infrastructure, 
                args.validate_recovery, args.emergency_reset]):
        args.emergency_reset = True  # Default to emergency reset
    
    recovery_manager = InfrastructureRecoveryManager(args.project_root)
    actions = []
    
    if args.emergency_reset:
        actions = recovery_manager.emergency_reset()
    else:
        if args.reset_processes:
            actions.extend(recovery_manager.reset_processes())
        if args.cleanup_containers:
            actions.extend(recovery_manager.cleanup_filesystem())
        if args.restart_infrastructure:
            actions.extend(recovery_manager.restart_infrastructure())
        if args.validate_recovery:
            actions.extend(recovery_manager.validate_recovery())
    
    # Print report
    report = recovery_manager.format_recovery_report(actions)
    print(report)
    
    # Exit with appropriate code
    has_failures = any(a.status == 'failed' for a in actions)
    exit(1 if has_failures else 0)


if __name__ == "__main__":
    main()