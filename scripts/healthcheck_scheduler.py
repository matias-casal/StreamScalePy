#!/usr/bin/env python3
"""
Health check script for scheduler service.
Script de verificación de salud para servicio scheduler.
"""
import sys
import psutil
import os

def check_scheduler_health():
    """Check if scheduler process is healthy"""
    try:
        # Check if current process is running
        current_pid = os.getpid()
        process = psutil.Process(current_pid)
        
        # Check CPU and memory usage
        cpu_percent = process.cpu_percent(interval=0.1)
        memory_percent = process.memory_percent()
        
        # Check if process is zombie
        if process.status() == psutil.STATUS_ZOMBIE:
            print("Scheduler is zombie")
            return False
        
        # Check if process is responding
        if process.status() in [psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING, psutil.STATUS_IDLE]:
            # Scheduler is healthy if not using excessive resources
            if cpu_percent < 95 and memory_percent < 90:
                print(f"Scheduler healthy - CPU: {cpu_percent:.1f}%, Memory: {memory_percent:.1f}%")
                return True
            else:
                print(f"Scheduler overloaded - CPU: {cpu_percent:.1f}%, Memory: {memory_percent:.1f}%")
                return False
        
        print(f"Scheduler unhealthy - Status: {process.status()}")
        return False
        
    except Exception as e:
        print(f"Health check failed: {str(e)}")
        return False

if __name__ == "__main__":
    if check_scheduler_health():
        sys.exit(0)
    else:
        sys.exit(1)