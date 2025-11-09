# This script finds rows in DailyBulletinArrests with no coordinates,
# authenticates against the proxy, geocodes the 'location' string,
# converts the coordinates to EPSG:102672, and updates the table.
#
# It is designed to be run in a container and reads credentials from
# environment variables.
#
# Requires:
# 1. Python and 'pyproj' (pip install pyproj)
# 2. 'alter_arrests_table.sql' to have been run
# 3. Your p2cfrontend proxy server must be running

# --- CONFIG (from Environment Variables) ---
$server       = "192.168.0.43"
$database     = "p2cdubuque"
$username     = "sa"
$password     = "Thugitout09!"
$table        = "dbo.DailyBulletinArrests"

# --- PROXY CONFIG (from Environment Variables) ---
$proxyBaseUrl  = "http://192.168.0.4:9000"
$proxyLdapUser = "bosplaya"
$proxyLdapPass = "boshog"
# --------------------
# Note: When running in Podman, 'localhost' in the $proxyBaseUrl will refer
# to the container itself. To reach the host, you might need to use
# 'host.containers.internal' (for Podman 4.0+) or the host's actual IP.
# I have set 'http://host.containers.internal:9000' as a sensible default.
# --------------------

# --- SQL Connection ---
$connStr = "Server=$server;Database=$database;User ID=$username;Password=$password;TrustServerCertificate=True;"
Write-Host "Connecting to $server..."
$connection = New-Object System.Data.SqlClient.SqlConnection $connStr
$connection.Open()
Write-Host "✅ Connection successful."

# --- Helper function for URL encoding ---
Add-Type -AssemblyName System.Web
function Get-UrlEncodedString {
    param ($string)
    return [System.Web.HttpUtility]::UrlEncode($string)
}

# --- Python conversion function (Lat/Lon -> EPSG:102672) ---
function Convert-LatLonToProjected {
    param ($lat, $lon)
    # Note: 'python' must be in the container's PATH. 'python3' is also common.
    
    # --- THIS IS THE FIX ---
    # We are converting to EPSG:102672 (NAD83 / Iowa North)
    # to match the coordinate system used in your cadHandler table.
    # We use EPSG:4326 (WGS84) as the source, which is what Nominatim provides.
    $pyCode = "from pyproj import Transformer; transformer = Transformer.from_crs('EPSG:4326', 'EPSG:102672', always_xy=True); x, y = transformer.transform($lon, $lat); print(f'{x},{y}')"
    # --- END FIX ---
    
    $result = python -c $pyCode
    if ($result) {
        $parts = $result.Trim().Split(",")
        return @{ X = $parts[0]; Y = $parts[1] }
    } else {
        Write-Host "Conversion failed for Lat=$lat Lon=$lon"
        return @{ X = $null; Y = $null }
    }
}

# --- Authentication function ---
function Get-AuthToken {
    param (
        [string]$loginUrl,
        [string]$username,
        [string]$password
    )
    
    Write-Host "Authenticating to proxy at $loginUrl..."
    $body = @{
        username = $username
        password = $password
    } | ConvertTo-Json
    
    try {
        $response = Invoke-RestMethod -Uri $loginUrl -Method Post -Body $body -ContentType "application/json"
        Write-Host "✅ Authentication successful."
        return $response.token
    } catch {
        Write-Host "❌ FATAL: Could not authenticate against the proxy. Error: $($_.Exception.Message)"
        return $null
    }
}

# --- START SCRIPT ---

# 1. Get Auth Token
$authToken = Get-AuthToken -loginUrl "$proxyBaseUrl/login" -username $proxyLdapUser -password $proxyLdapPass

if ($null -eq $authToken) {
    Write-Host "Script cannot continue without an auth token."
    exit
}

$authHeader = @{
    "Authorization" = "Bearer $authToken"
}

# 2. Query rows with NULL coordinates
Write-Host "Finding rows to geocode..."
$selectCmd = $connection.CreateCommand()
$selectCmd.CommandText = @"
SELECT invid, location
FROM $table
WHERE geox is NULL
"@
$reader = $selectCmd.ExecuteReader()

$updates = @()
while ($reader.Read()) {
    $updates += [PSCustomObject]@{
        Invid    = $reader["invid"]
        Location = $reader["location"].ToString()
    }
}
$reader.Close()
Write-Host "Found $($updates.Count) arrest records to geocode."

# 3. Prepare update command
$updateCmd = $connection.CreateCommand()
$updateCmd.CommandText = @"
UPDATE $table
SET geox = @geox, geoy = @geoy
WHERE invid = @invid
"@
$updateCmd.Parameters.Add("@geox", [System.Data.SqlDbType]::Decimal, 18, 2) | Out-Null
$updateCmd.Parameters.Add("@geoy", [System.Data.SqlDbType]::Decimal, 18, 2) | Out-Null
$updateCmd.Parameters.Add("@invid", [System.Data.SqlDbType]::Int) | Out-Null

# 4. Process each row
$geocodedCount = 0
foreach ($row in $updates) {
    $encodedLocation = Get-UrlEncodedString -string $row.Location
    $fullUrl = "$proxyBaseUrl/geocode?q=$encodedLocation"
    
    try {
        # 1. Call your proxy's geocoder *with the auth token*
        $geoResult = Invoke-RestMethod -Uri $fullUrl -Method Get -Headers $authHeader
        
        if ($null -eq $geoResult -or $null -eq $geoResult.lat -or $null -eq $geoResult.lon) {
            Write-Host "⚠️ Geocoder returned no coordinates for invid $($row.Invid)"
            continue
        }

        # 2. Convert coordinates
        $projected = Convert-LatLonToProjected -lat $geoResult.lat -lon $geoResult.lon
        
        if ($null -eq $projected.X) {
            Write-Host "❌ pyproj conversion failed for invid $($row.Invid)"
            continue
        }

        # 3. Update the database
        $updateCmd.Parameters["@geox"].Value = $projected.X
        $updateCmd.Parameters["@geoy"].Value = $projected.Y
        $updateCmd.Parameters["@invid"].Value = $row.Invid
        
        $updateCmd.ExecuteNonQuery() | Out-Null
        Write-Host "✅ Geocoded invid $($row.Invid) → $($projected.X), $($projected.Y)"
        $geocodedCount++

    } catch {
        # Check for 401/403 errors specifically
        if ($_.Exception.Response.StatusCode -eq 401 -or $_.Exception.Response.StatusCode -eq 403) {
            Write-Host "❌ FATAL: Authentication token failed. Please check credentials or token."
            break # Stop the loop
        }
        Write-Host "❌ Failed to process invid $($row.Invid). Error: $($_.Exception.Message)"
    }
}

$connection.Close()
Write-Host "� Geocoding complete. $geocodedCount records updated."


