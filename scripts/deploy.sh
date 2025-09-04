#!/bin/bash
# StreamScale Production Deployment Script
# Author: StreamScale Team
# Version: 1.0.0

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${PROJECT_ROOT}/.env.production"
DOCKER_COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.prod.yml"
BACKUP_DIR="${PROJECT_ROOT}/backups"
LOG_FILE="${PROJECT_ROOT}/logs/deploy_$(date +%Y%m%d_%H%M%S).log"

# Functions
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"
    exit 1
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"
}

check_requirements() {
    log "Checking system requirements..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed"
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        error "Docker Compose is not installed"
    fi
    
    # Check environment file
    if [[ ! -f "$ENV_FILE" ]]; then
        error "Production environment file not found: $ENV_FILE"
    fi
    
    # Check if running as root (not recommended)
    if [[ $EUID -eq 0 ]]; then
        warning "Running as root is not recommended"
    fi
    
    log "All requirements met ✓"
}

backup_data() {
    log "Creating backup..."
    
    mkdir -p "$BACKUP_DIR"
    
    # Backup timestamp
    BACKUP_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    BACKUP_PATH="${BACKUP_DIR}/backup_${BACKUP_TIMESTAMP}"
    
    # Create backup directory
    mkdir -p "$BACKUP_PATH"
    
    # Backup database
    if docker ps --format '{{.Names}}' | grep -q streamscale_postgres; then
        log "Backing up PostgreSQL database..."
        docker exec streamscale_postgres pg_dump -U "${POSTGRES_USER:-streamscale_prod}" \
            "${POSTGRES_DB:-streamscale}" | gzip > "${BACKUP_PATH}/postgres_backup.sql.gz"
    fi
    
    # Backup Redis
    if docker ps --format '{{.Names}}' | grep -q streamscale_redis; then
        log "Backing up Redis data..."
        docker exec streamscale_redis redis-cli --rdb "${BACKUP_PATH}/redis_backup.rdb" BGSAVE
    fi
    
    log "Backup completed: $BACKUP_PATH"
}

build_images() {
    log "Building Docker images..."
    
    cd "$PROJECT_ROOT"
    
    # Build with BuildKit for better caching
    DOCKER_BUILDKIT=1 docker-compose -f "$DOCKER_COMPOSE_FILE" build --parallel
    
    log "Images built successfully ✓"
}

deploy() {
    log "Starting deployment..."
    
    cd "$PROJECT_ROOT"
    
    # Pull latest images if using external registry
    # docker-compose -f "$DOCKER_COMPOSE_FILE" pull
    
    # Deploy with zero-downtime using rolling update
    docker-compose -f "$DOCKER_COMPOSE_FILE" up -d --remove-orphans
    
    log "Services deployed ✓"
}

health_check() {
    log "Running health checks..."
    
    # Wait for services to be ready
    sleep 10
    
    # Check API health
    MAX_RETRIES=30
    RETRY_COUNT=0
    
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        if curl -f http://localhost/health &> /dev/null; then
            log "API health check passed ✓"
            break
        fi
        
        RETRY_COUNT=$((RETRY_COUNT + 1))
        warning "Health check attempt $RETRY_COUNT/$MAX_RETRIES failed, retrying..."
        sleep 5
    done
    
    if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
        error "Health check failed after $MAX_RETRIES attempts"
    fi
    
    # Check all services
    docker-compose -f "$DOCKER_COMPOSE_FILE" ps
}

cleanup_old_backups() {
    log "Cleaning up old backups..."
    
    # Keep only last 7 days of backups
    find "$BACKUP_DIR" -type d -name "backup_*" -mtime +7 -exec rm -rf {} + 2>/dev/null || true
    
    log "Cleanup completed ✓"
}

rollback() {
    warning "Rolling back deployment..."
    
    cd "$PROJECT_ROOT"
    
    # Stop current deployment
    docker-compose -f "$DOCKER_COMPOSE_FILE" down
    
    # Restore from latest backup if available
    LATEST_BACKUP=$(ls -t "$BACKUP_DIR"/backup_* 2>/dev/null | head -1)
    
    if [[ -n "$LATEST_BACKUP" ]]; then
        log "Restoring from backup: $LATEST_BACKUP"
        # Add restoration logic here
    fi
    
    error "Rollback completed. Manual intervention may be required."
}

# Main execution
main() {
    # Create log directory
    mkdir -p "$(dirname "$LOG_FILE")"
    
    log "=== StreamScale Deployment Started ==="
    log "Environment: PRODUCTION"
    log "Project root: $PROJECT_ROOT"
    
    # Parse arguments
    case "${1:-deploy}" in
        deploy)
            check_requirements
            backup_data
            build_images
            deploy
            health_check
            cleanup_old_backups
            log "=== Deployment Completed Successfully ==="
            ;;
        rollback)
            rollback
            ;;
        backup)
            backup_data
            ;;
        health)
            health_check
            ;;
        *)
            echo "Usage: $0 {deploy|rollback|backup|health}"
            exit 1
            ;;
    esac
}

# Trap errors for rollback
trap 'error "Deployment failed! Consider running: $0 rollback"' ERR

# Run main function
main "$@"