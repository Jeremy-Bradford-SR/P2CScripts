# Parameters
$serverInstance = "localhost"  # Change if using a named instance like "localhost\SQLEXPRESS"
$databaseName = "p2cdubuque"
$username = "sa"
$password = "Thugitout09!"
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

# SQL Update Statement
$query = @"
UPDATE dbo.DailyBulletinArrests
SET event_time = TRY_CONVERT(datetime, 
    REPLACE(REPLACE(LTRIM(RTRIM(time)), 'on ', ''), '.', '')
)
WHERE event_time IS NULL
  AND time IS NOT NULL
  AND TRY_CONVERT(datetime, 
    REPLACE(REPLACE(LTRIM(RTRIM(time)), 'on ', ''), '.', '')
) IS NOT NULL;
"@

# Create SQL credential object
$securePassword = ConvertTo-SecureString $password -AsPlainText -Force
$credential = New-Object System.Management.Automation.PSCredential ($username, $securePassword)

# Execute the query
try {
    Write-Host "[$timestamp] Executing update on $databaseName..."
    Invoke-Sqlcmd [-TrustServerCertificate -ServerInstance $serverInstance -Database $databaseName -Credential $credential -Query $query
    Write-Host "[$timestamp] Update completed successfully."
} catch {
    Write-Error "[$timestamp] Update failed: $_"
}