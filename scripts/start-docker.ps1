[CmdletBinding()]
param(
    [Parameter()]
    [string]$AdvertiseIP = "",

    [Parameter()]
    [string]$PublicBaseURL = "",

    [Parameter()]
    [string]$PublicWebBaseURL = "",

    [Parameter()]
    [string]$AdminBindIP = "",

    [Parameter()]
    [switch]$SFUHostNetwork,

    [Parameter()]
    [switch]$SFUBridgeNetwork,

    [Parameter()]
    [switch]$AllowInsecureDevelopmentAuth,

    [Parameter()]
    [switch]$Build
)

$ErrorActionPreference = "Stop"

if ($SFUHostNetwork -and $SFUBridgeNetwork) {
    throw "SFUHostNetwork and SFUBridgeNetwork are mutually exclusive."
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$dockerDir = Join-Path $repoRoot "deploy\docker"
$composePath = Join-Path $dockerDir "compose.yaml"
$envPath = Join-Path $dockerDir ".env"
$generatorPath = Join-Path $PSScriptRoot "new-docker-env.ps1"

if (-not (Test-Path -LiteralPath $envPath -PathType Leaf)) {
    if ([string]::IsNullOrWhiteSpace($AdvertiseIP)) {
        $AdvertiseIP = "127.0.0.1"
    }
    $generatorArguments = @{
        AdvertiseIP = $AdvertiseIP
    }
    if (-not [string]::IsNullOrWhiteSpace($PublicBaseURL)) {
        $generatorArguments.PublicBaseURL = $PublicBaseURL
    }
    if (-not [string]::IsNullOrWhiteSpace($PublicWebBaseURL)) {
        $generatorArguments.PublicWebBaseURL = $PublicWebBaseURL
    }
    if (-not [string]::IsNullOrWhiteSpace($AdminBindIP)) {
        $generatorArguments.AdminBindIP = $AdminBindIP
    }
    if ($SFUHostNetwork) {
        $generatorArguments.SFUHostNetwork = $true
    }
    if ($SFUBridgeNetwork) {
        $generatorArguments.SFUBridgeNetwork = $true
    }
    if ($AllowInsecureDevelopmentAuth) {
        $generatorArguments.AllowInsecureDevelopmentAuth = $true
    }
    & $generatorPath @generatorArguments
}
elseif ($PSBoundParameters.ContainsKey("AdvertiseIP") -or
        $PSBoundParameters.ContainsKey("PublicBaseURL") -or
        $PSBoundParameters.ContainsKey("PublicWebBaseURL") -or
        $PSBoundParameters.ContainsKey("AdminBindIP") -or
        $PSBoundParameters.ContainsKey("SFUHostNetwork") -or
        $PSBoundParameters.ContainsKey("SFUBridgeNetwork") -or
        $PSBoundParameters.ContainsKey("AllowInsecureDevelopmentAuth")) {
    Write-Warning "deploy/docker/.env already exists; address parameters are ignored so existing credentials and deployment identity stay unchanged."
}

$deployment = @{}
foreach ($line in Get-Content -LiteralPath $envPath) {
    if ($line -match '^([A-Z0-9_]+)=(.*)$') {
        $deployment[$Matches[1]] = $Matches[2]
    }
}

$composeBase = @(
    "compose",
    "--project-directory", $dockerDir,
    "--env-file", $envPath,
    "--file", $composePath
)
$sfuHostNetworkValue = $deployment['TELESRV_SFU_HOST_NETWORK']
if (-not [string]::IsNullOrWhiteSpace($sfuHostNetworkValue) -and
    $sfuHostNetworkValue -ne 'true' -and $sfuHostNetworkValue -ne 'false') {
    throw "TELESRV_SFU_HOST_NETWORK must be true or false."
}
if ($deployment.ContainsKey('TELESRV_SFU_HOST_NETWORK') -and
    $deployment['TELESRV_SFU_HOST_NETWORK'] -eq 'false') {
    $composeBase += @("--file", (Join-Path $dockerDir "compose.sfu-bridge-network.yaml"))
}

function Invoke-Compose {
    param([string[]]$Arguments)

    & docker @composeBase @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

Invoke-Compose -Arguments @("config", "--quiet")

if ($Build) {
    Invoke-Compose -Arguments @("build", "--pull")
}
else {
    Invoke-Compose -Arguments @("pull")
}

try {
    Invoke-Compose -Arguments @("up", "--detach", "--no-build", "--wait", "--wait-timeout", "600")
}
catch {
    & docker @composeBase logs --no-color --tail 120
    throw
}

Invoke-Compose -Arguments @("ps", "--all")
Write-Host "telesrv Docker stack is ready. Configuration: $envPath"

if ($deployment['TELESRV_PHONE_CODE_DELIVERY_PROVIDER'] -eq 'development' -and
    $deployment['TELESRV_DEV_AUTH_CODE'] -match '^[0-9]{5,6}$') {
    Write-Host "Development login code: $($deployment['TELESRV_DEV_AUTH_CODE'])"
}
if ($deployment['TELESRV_TURN_ENABLE'] -eq 'true') {
    Write-Host "TURN/STUN: udp://$($deployment['TELESRV_TURN_ADVERTISE_IP']):$($deployment['TELESRV_TURN_UDP_PORT'])"
    Write-Host "TURN relay UDP range: $($deployment['TELESRV_TURN_RELAY_MIN_PORT'])-$($deployment['TELESRV_TURN_RELAY_MAX_PORT'])"
}
if ($deployment['TELESRV_LIVESTREAM_ENABLE'] -eq 'true') {
    Write-Host "RTMP ingest: $($deployment['TELESRV_LIVESTREAM_RTMP_URL']) (stream key is provided by the client)"
}
$adminBindIP = $deployment['TELESRV_ADMIN_BIND_IP']
$adminPort = $deployment['TELESRV_ADMIN_PORT']
if ([string]::IsNullOrWhiteSpace($adminBindIP)) {
    $adminBindIP = "127.0.0.1"
}
if ([string]::IsNullOrWhiteSpace($adminPort)) {
    $adminPort = "2600"
}
$adminURLHost = $adminBindIP
if ($adminURLHost -eq "0.0.0.0" -or $adminURLHost -eq "::") {
    $adminURLHost = $deployment['TELESRV_ADVERTISE_IP']
}
if ($adminURLHost.Contains(":")) {
    $adminURLHost = "[${adminURLHost}]"
}
Write-Host "Admin UI: http://${adminURLHost}:${adminPort} (login password is stored in $envPath)"
