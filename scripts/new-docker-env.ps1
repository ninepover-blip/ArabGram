[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$AdvertiseIP,

    [Parameter()]
    [string]$PublicBaseURL = "",

    [Parameter()]
    [string]$PublicWebBaseURL = "",

    [Parameter()]
    [string]$AdminBindIP = "127.0.0.1",

    [Parameter()]
    [switch]$SFUHostNetwork,

    [Parameter()]
    [switch]$SFUBridgeNetwork,

    [Parameter()]
    [switch]$AllowInsecureDevelopmentAuth
)

$ErrorActionPreference = "Stop"

if ($SFUHostNetwork -and $SFUBridgeNetwork) {
    throw "SFUHostNetwork and SFUBridgeNetwork are mutually exclusive."
}

$parsedIP = $null
if (-not [System.Net.IPAddress]::TryParse($AdvertiseIP, [ref]$parsedIP)) {
    throw "AdvertiseIP must be an IPv4 or IPv6 address, not a DNS name."
}
$isLoopback = [System.Net.IPAddress]::IsLoopback($parsedIP)
$isIPv6 = $parsedIP.AddressFamily -eq [System.Net.Sockets.AddressFamily]::InterNetworkV6

$parsedAdminBindIP = $null
if (-not [System.Net.IPAddress]::TryParse($AdminBindIP, [ref]$parsedAdminBindIP)) {
    throw "AdminBindIP must be an IPv4 or IPv6 address."
}

if ([string]::IsNullOrWhiteSpace($PublicBaseURL)) {
    if (-not $isLoopback) {
        throw "PublicBaseURL is required when AdvertiseIP is not loopback."
    }
    $loopbackHost = "127.0.0.1"
    if ($isIPv6) {
        $loopbackHost = "[::1]"
    }
    $PublicBaseURL = "http://${loopbackHost}:2401"
}
if ([string]::IsNullOrWhiteSpace($PublicWebBaseURL)) {
    $PublicWebBaseURL = $PublicBaseURL
}

function Assert-HTTPURL {
    param(
        [string]$Name,
        [string]$Value
    )

    $uri = $null
    if (-not [Uri]::TryCreate($Value, [UriKind]::Absolute, [ref]$uri) -or
        ($uri.Scheme -ne "http" -and $uri.Scheme -ne "https") -or
        -not [string]::IsNullOrEmpty($uri.UserInfo)) {
        throw "$Name must be an absolute HTTP(S) URL without embedded credentials."
    }
}

Assert-HTTPURL -Name "PublicBaseURL" -Value $PublicBaseURL
Assert-HTTPURL -Name "PublicWebBaseURL" -Value $PublicWebBaseURL

$repoRoot = Split-Path -Parent $PSScriptRoot
$dockerDir = Join-Path $repoRoot "deploy\docker"
$templatePath = Join-Path $dockerDir ".env.example"
$outputPath = Join-Path $dockerDir ".env"

if (-not (Test-Path -LiteralPath $templatePath -PathType Leaf)) {
    throw "Docker environment template not found: $templatePath"
}
if (Test-Path -LiteralPath $outputPath) {
    throw "$outputPath already exists. Initialization must not overwrite live credentials; rotate them explicitly or move this file aside only for a new empty deployment."
}

function New-RandomBytes {
    param([int]$Bytes)

    $buffer = New-Object byte[] $Bytes
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $generator.GetBytes($buffer)
    }
    finally {
        $generator.Dispose()
    }
    return ,$buffer
}

function New-HexSecret {
    param([int]$Bytes = 32)

    $buffer = New-RandomBytes -Bytes $Bytes
    return [BitConverter]::ToString($buffer).Replace("-", "").ToLowerInvariant()
}

function Get-GitValue {
    param([string[]]$GitArguments)

    try {
        $value = & git -C $repoRoot @GitArguments 2>$null
        if ($LASTEXITCODE -eq 0) {
            return ($value | Select-Object -First 1).Trim()
        }
    }
    catch {
        return "unknown"
    }
    return "unknown"
}

$buildCommit = Get-GitValue -GitArguments @("rev-parse", "HEAD")
$buildBranch = Get-GitValue -GitArguments @("rev-parse", "--abbrev-ref", "HEAD")
$treeState = "unknown"
try {
    $treeOutput = & git -C $repoRoot status --porcelain 2>$null
    if ($LASTEXITCODE -eq 0) {
        $treeState = "clean"
        if ($null -ne $treeOutput -and @($treeOutput).Count -gt 0) {
            $treeState = "dirty"
        }
    }
}
catch {
    $treeState = "unknown"
}

$publicBindIP = "0.0.0.0"
$localBindIP = "127.0.0.1"
if ($isIPv6) {
    $publicBindIP = "::"
    $localBindIP = "::1"
}
if ($isLoopback) {
    $publicBindIP = $parsedIP.ToString()
    $localBindIP = $parsedIP.ToString()
}
elseif (([Uri]$PublicBaseURL).Scheme -eq "http") {
    $publicLinkIP = $null
    $publicLinkHost = ([Uri]$PublicBaseURL).Host.Trim([char[]]"[]")
    if ([System.Net.IPAddress]::TryParse($publicLinkHost, [ref]$publicLinkIP) -and $publicLinkIP.Equals($parsedIP)) {
        # Direct LAN HTTP mode: make the advertised public-link endpoint
        # reachable without requiring a reverse proxy for the first run.
        $localBindIP = $parsedIP.ToString()
    }
}

# The SFU-owned TURN listener is udp4. Keep IPv6-only deployments usable by
# disabling TURN there while leaving RTMP and the rest of the stack enabled.
$turnEnabled = (-not $isIPv6).ToString().ToLowerInvariant()
$turnAdvertiseIP = "127.0.0.1"
$turnBindIP = "127.0.0.1"
if (-not $isIPv6) {
    $turnAdvertiseIP = $parsedIP.ToString()
    if (-not $isLoopback) {
        $turnBindIP = "0.0.0.0"
    }
}
$rtmpHost = $parsedIP.ToString()
if ($isIPv6) {
    $rtmpHost = "[${rtmpHost}]"
}
$rtmpURL = "rtmp://${rtmpHost}:2400/live"

$postgresPassword = New-HexSecret 24
$values = [ordered]@{
    TELESRV_BUILD_COMMIT                    = $buildCommit
    TELESRV_BUILD_BRANCH                    = $buildBranch
    TELESRV_BUILD_TREE_STATE                = $treeState
    TELESRV_BUILD_DATE                      = [DateTime]::UtcNow.ToString("o")
    POSTGRES_PASSWORD                       = $postgresPassword
    TELESRV_POSTGRES_DSN                    = "postgres://telesrv:${postgresPassword}@postgres:5432/telesrv_v2?sslmode=disable"
    TELESRV_REDIS_PASSWORD                  = New-HexSecret 32
    TELESRV_CORE_EXEC_TOKEN                 = New-HexSecret 32
    TELESRV_FILE_TOKEN                      = New-HexSecret 32
    TELESRV_EGRESS_DELIVERY_TOKEN                = New-HexSecret 32
    TELESRV_GROUPCALL_CONTROL_TOKEN         = New-HexSecret 32
    TELESRV_SFU_CONTROL_TOKEN               = New-HexSecret 32
    TELESRV_ADMIN_API_TOKEN                 = New-HexSecret 32
    TELESRV_ADMIN_UI_PASSWORD               = New-HexSecret 24
    TELESRV_ADMIN_SESSION_KEY               = New-HexSecret 32
    TELESRV_TURN_SECRET                      = New-HexSecret 32
    TELESRV_OTP_WEBHOOK_SECRET              = New-HexSecret 32
    TELESRV_DEV_AUTH_CODE                   = "12345"
    TELESRV_ALLOW_INSECURE_DEVELOPMENT_AUTH = ($isLoopback -or $AllowInsecureDevelopmentAuth).ToString().ToLowerInvariant()
    TELESRV_ADVERTISE_IP                    = $parsedIP.ToString()
    TELESRV_PUBLIC_BASE_URL                 = $PublicBaseURL
    TELESRV_PUBLIC_WEB_BASE_URL             = $PublicWebBaseURL
    TELESRV_TURN_ENABLE                      = $turnEnabled
    TELESRV_TURN_ADVERTISE_IP                = $turnAdvertiseIP
    TELESRV_TURN_BIND_IP                     = $turnBindIP
    TELESRV_SFU_BIND_IP                      = $turnBindIP
    TELESRV_LIVESTREAM_RTMP_URL              = $rtmpURL
    TELESRV_PUBLIC_BIND_IP                  = $publicBindIP
    TELESRV_LOCAL_BIND_IP                   = $localBindIP
    TELESRV_ADMIN_BIND_IP                   = $parsedAdminBindIP.ToString()
    TELESRV_SFU_HOST_NETWORK                = (-not $SFUBridgeNetwork.IsPresent).ToString().ToLowerInvariant()
}

$content = [IO.File]::ReadAllText($templatePath)
foreach ($entry in $values.GetEnumerator()) {
    $pattern = "(?m)^$([Regex]::Escape($entry.Key))=.*$"
    if (-not [Regex]::IsMatch($content, $pattern)) {
        throw "Template is missing $($entry.Key)."
    }
    $replacement = ("{0}={1}" -f $entry.Key, $entry.Value).Replace('$', '$$')
    $content = [Regex]::Replace($content, $pattern, $replacement)
}

function Protect-SecretFile {
    param([string]$Path)

    if ($env:OS -eq "Windows_NT") {
        $owner = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
        $acl = New-Object System.Security.AccessControl.FileSecurity
        $acl.SetAccessRuleProtection($true, $false)
        $acl.SetOwner($owner)
        $identities = @(
            $owner,
            (New-Object System.Security.Principal.SecurityIdentifier("S-1-5-18")),
            (New-Object System.Security.Principal.SecurityIdentifier("S-1-5-32-544"))
        )
        foreach ($identity in $identities) {
            $rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
                $identity,
                [System.Security.AccessControl.FileSystemRights]::FullControl,
                [System.Security.AccessControl.AccessControlType]::Allow
            )
            [void]$acl.AddAccessRule($rule)
        }
        Set-Acl -LiteralPath $Path -AclObject $acl
        return
    }

    & chmod 600 -- $Path
    if ($LASTEXITCODE -ne 0) {
        throw "chmod 600 failed for $Path"
    }
}

if ($PSCmdlet.ShouldProcess($outputPath, "write owner-only Docker deployment environment")) {
    $temporaryPath = "$outputPath.tmp.$PID"
    try {
        [IO.File]::WriteAllText($temporaryPath, $content, (New-Object Text.UTF8Encoding($false)))
        Protect-SecretFile -Path $temporaryPath
        Move-Item -LiteralPath $temporaryPath -Destination $outputPath
    }
    finally {
        if (Test-Path -LiteralPath $temporaryPath) {
            Remove-Item -LiteralPath $temporaryPath -Force
        }
    }
    Write-Host "Created $outputPath with restricted permissions."
    Write-Host "Review public URLs and authentication delivery settings before startup."
    if (-not $SFUBridgeNetwork) {
        Write-Host "Next: docker compose --project-directory deploy/docker -f deploy/docker/compose.yaml config --quiet"
    }
    else {
        Write-Host "Next: docker compose --project-directory deploy/docker -f deploy/docker/compose.yaml -f deploy/docker/compose.sfu-bridge-network.yaml config --quiet"
    }
}
