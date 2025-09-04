#!/usr/bin/env python3
"""
Health check script for worker services.
Script de verificación de salud para servicios worker.
"""
import sys
import psutil
import os

def check_worker_health():
    """Check if worker process is healthy"""
    try:
        # Check if current process is running
        current_pid = os.getpid()
        process = psutil.Process(current_pid)
        
        # Check CPU and memory usage
        cpu_percent = process.cpu_percent(interval=0.1)
        memory_percent = process.memory_percent()
        
        # Check if process is zombie
        if process.status() == psutil.STATUS_ZOMBIE:
            print("Worker is zombie")
            return False
        
        # Check if process is responding
        if process.status() in [psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING, psutil.STATUS_IDLE]:
            # Worker is healthy if not using excessive resources
            if cpu_percent < 95 and memory_percent < 90:
                print(f"Worker healthy - CPU: {cpu_percent:.1f}%, Memory: {memory_percent:.1f}%")
                return True
            else:
                print(f"Worker overloaded - CPU: {cpu_percent:.1f}%, Memory: {memory_percent:.1f}%")
                return False
        
        print(f"Worker unhealthy - Status: {process.status()}")
        return False
        
    except Exception as e:
        print(f"Health check failed: {str(e)}")
        return False

if __name__ == "__main__":
    if check_worker_health():
        sys.exit(0)
    else:
        sys.exit(1)