# PowerShell script to back up SQL Server database and prune old backups
# Author: Jeremy (Systems Architect)
# Date: 2025-09-30

# Parameters
$DatabaseName = "p2cdubuque"
$BackupPath   = "C:\INFRA\SQLBackup"
$RetentionDays = 14
$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$BackupFile = Join-Path $BackupPath "$DatabaseName-$Timestamp.bak"

# Ensure backup directory exists
if (-not (Test-Path $BackupPath)) {
    New-Item -Path $BackupPath -ItemType Directory | Out-Null
}

# SQL Server backup command
$SqlBackup = @"
BACKUP DATABASE [$DatabaseName]
TO DISK = N'$BackupFile'
WITH INIT, COMPRESSION, STATS = 10;
"@

try {
    # Invoke SQL Server backup
    Invoke-Sqlcmd -Query $SqlBackup -ServerInstance "localhost" -ErrorAction Stop
    Write-Host "[OK] Backup completed: $BackupFile"
} catch {
    Write-Host "[ERROR] Backup failed: $($_.Exception.Message)"
    exit 1
}

# Prune backups older than retention period
try {
    Get-ChildItem -Path $BackupPath -Filter "$DatabaseName-*.bak" |
        Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$RetentionDays) } |
        ForEach-Object {
            Remove-Item $_.FullName -Force
            Write-Host "[INFO] Deleted old backup: $($_.Name)"
        }
} catch {
    Write-Host "[ERROR] Cleanup failed: $($_.Exception.Message)"
    exit 1
}