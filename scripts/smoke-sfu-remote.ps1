<#
.SYNOPSIS
Builds and smoke-tests standalone telesrv-sfu remote media mode.

.DESCRIPTION
This script starts the standalone cmd/telesrv-sfu process, verifies the SFU
control endpoint, verifies the UDP media port is bound, and keeps
separate stdout/stderr logs. It requires a reachable Redis because telesrv-sfu
publishes its SFU instance heartbeat through Redis. The Core groupcall control
URL only needs to be syntactically valid for this smoke; it is contacted later
when media liveness events are produced. Before starting the media process, the
script also runs the Redis SFU registry gate that proves instance discovery,
capacity-aware and health-aware owner selection, call owner stickiness, remote
owner control routing, and record-expiry fail-closed behavior against the
same Redis endpoint. The smoke always starts the standalone SFU with the gRPC
owner-control plane, bearer tokens, and a negative invalid-auth health check. With
-MultiInstance it starts two standalone media processes on separate control/UDP
ports and verifies both instance records are visible in Redis. Optional SFU gRPC
control TLS/mTLS parameters are passed through the same
TELESRV_SFU_CONTROL_GRPC_TLS_* variables used in production;
-GenerateSFUControlGRPCTestCerts creates a temporary CA, server certificate, and
client certificate for local mTLS smoke runs. With
-RunCoreRemoteConfigGate it also proves cmd/telesrv-core refuses to start
without the Core/SFU bearer tokens required by standalone SFU topology.
With -RunRemoteOwnerProbe it also drives a Core-style remote-only OwnerService through
Redis owner selection and real standalone SFU Join/Leave/CloseRoom calls,
including two users joining the same call without splitting the SFU owner.
With -RunRemoteOwnerCapacityProbe it also verifies a full standalone SFU instance
is skipped for a new call; use -MaxActiveCalls 1 -SecondMaxActiveCalls 1 for
that gate.
With -RunRemoteOwnerFailoverProbe it force-stops one standalone SFU while its
Redis instance heartbeat record is still fresh, then verifies Core-style owner
selection skips that unhealthy instance for a new call.
With -RunRemoteOwnerTTLProbe it runs a short remote owner heartbeat and verifies
the same call stays on the same owner after the owner TTL would otherwise expire.
With -RunMediaE2EProbe it drives two simulated tgcalls clients through the real
standalone SFU process and verifies ICE/DTLS/SRTP plus RTP forwarding. Use
-MediaE2EMinPackets and -MediaE2EDuration to turn the default single-packet
probe into a longer media-forwarding canary.
-RunLogSafetyGate scans standalone SFU stderr logs after a successful smoke and
fails on unexpected protocol/internal/error signatures. -RunPortCleanupGate
verifies SFU control TCP and media UDP ports are released after cleanup.
-PreflightOnly runs the Redis SFU registry gate, and optionally the Core remote
configuration gate, then exits without building or starting telesrv-sfu media
processes.
#>
[CmdletBinding()]
param(
    [string]$RedisAddr,
    [string]$RedisPassword,
    [int]$RedisDB = 0,
    [string]$SFUControlGRPCAddr = "127.0.0.1:2450",
    [string]$SFUControlGRPCURL,
    [string]$SFUControlToken = "sfu-smoke",
    [string]$SFUControlGRPCTLSServerCertFile,
    [string]$SFUControlGRPCTLSServerKeyFile,
    [string]$SFUControlGRPCTLSClientCAFile,
    [string]$SFUControlGRPCTLSCAFile,
    [string]$SFUControlGRPCTLSServerName,
    [string]$SFUControlGRPCTLSClientCertFile,
    [string]$SFUControlGRPCTLSClientKeyFile,
    [switch]$GenerateSFUControlGRPCTestCerts,
    [int]$SFUUDPPort = 12398,
    [string]$AdvertiseIP = "127.0.0.1",
    [string]$SFUAdvertiseIP,
    [string]$GroupCallControlURL = "http://127.0.0.1:9",
    [string]$GroupCallControlToken = "core-smoke",
    [string]$InstanceID,
    [int]$MaxActiveCalls = 8,
    [switch]$MultiInstance,
    [string]$SecondSFUControlGRPCAddr = "127.0.0.1:2451",
    [string]$SecondSFUControlGRPCURL,
    [int]$SecondSFUUDPPort = 12397,
    [string]$SecondInstanceID,
    [int]$SecondMaxActiveCalls = 8,
    [switch]$RunCoreRemoteConfigGate,
    [switch]$RunRemoteOwnerProbe,
    [switch]$RunRemoteOwnerCapacityProbe,
    [switch]$RunRemoteOwnerFailoverProbe,
    [switch]$RunRemoteOwnerTTLProbe,
    [switch]$RunMediaE2EProbe,
    [int]$MediaE2EMinPackets = 1,
    [string]$MediaE2EDuration = "0s",
    [string]$RemoteOwnerProbeOwnerTTL = "2s",
    [string]$SFUInstanceHeartbeatInterval = "1s",
    [string]$ExePath,
    [string]$CoreExePath,
    [string]$WorkDir,
    [string]$LogDir,
    [int]$StartupTimeoutSeconds = 45,
    [int]$Tail = 120,
    [switch]$SkipBuild,
    [switch]$BuildOnly,
    [switch]$PreflightOnly,
    [switch]$KeepRunning,
    [switch]$RunLogSafetyGate,
    [switch]$RunPortCleanupGate
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
if (-not $WorkDir) {
    $WorkDir = $RepoRoot
}
if (-not $ExePath) {
    $ExePath = Join-Path $RepoRoot "tmp\sfu-smoke\telesrv-sfu.exe"
}
if (-not $CoreExePath) {
    $CoreExePath = Join-Path $RepoRoot "tmp\sfu-smoke\telesrv-core.exe"
}
if (-not $LogDir) {
    $LogDir = Join-Path $RepoRoot "tmp\sfu-smoke\logs"
}
if (-not $RedisAddr) {
    $RedisAddr = [Environment]::GetEnvironmentVariable("TELESRV_REDIS_ADDR", "Process")
}
if (-not $RedisAddr -and -not $BuildOnly) {
    throw "-RedisAddr is required unless TELESRV_REDIS_ADDR is set"
}
if (-not $BuildOnly -and [string]::IsNullOrWhiteSpace($SFUControlToken)) {
    throw "-SFUControlToken is required for standalone SFU smoke"
}
if (-not $BuildOnly -and [string]::IsNullOrWhiteSpace($GroupCallControlToken)) {
    throw "-GroupCallControlToken is required for standalone SFU smoke"
}
if (-not $SFUControlGRPCURL) {
    $SFUControlGRPCURL = "grpc://$SFUControlGRPCAddr"
}
if (-not $SecondSFUControlGRPCURL) {
    $SecondSFUControlGRPCURL = "grpc://$SecondSFUControlGRPCAddr"
}
if (-not $SFUAdvertiseIP) {
    $SFUAdvertiseIP = $AdvertiseIP
}
if (-not $InstanceID) {
    $InstanceID = "smoke-sfu-$PID"
}
if (-not $SecondInstanceID) {
    $SecondInstanceID = "$InstanceID-b"
}
if ($MultiInstance -and $SecondInstanceID -eq $InstanceID) {
    throw "-SecondInstanceID must differ from -InstanceID"
}
if ($RunRemoteOwnerCapacityProbe) {
    $RunRemoteOwnerProbe = $true
    if (-not $MultiInstance) {
        throw "-RunRemoteOwnerCapacityProbe requires -MultiInstance"
    }
    if ($MaxActiveCalls -ne 1 -or $SecondMaxActiveCalls -ne 1) {
        throw "-RunRemoteOwnerCapacityProbe requires -MaxActiveCalls 1 and -SecondMaxActiveCalls 1"
    }
}
if ($RunRemoteOwnerFailoverProbe) {
    $RunRemoteOwnerProbe = $true
    if (-not $MultiInstance) {
        throw "-RunRemoteOwnerFailoverProbe requires -MultiInstance"
    }
}
if ($RunRemoteOwnerTTLProbe) {
    $RunRemoteOwnerProbe = $true
}
if ($MediaE2EMinPackets -le 0) {
    throw "-MediaE2EMinPackets must be positive"
}
if ([string]::IsNullOrWhiteSpace($MediaE2EDuration)) {
    throw "-MediaE2EDuration must not be empty"
}
if ($GenerateSFUControlGRPCTestCerts) {
    $manualTLSFiles = $SFUControlGRPCTLSServerCertFile -or
        $SFUControlGRPCTLSServerKeyFile -or
        $SFUControlGRPCTLSClientCAFile -or
        $SFUControlGRPCTLSCAFile -or
        $SFUControlGRPCTLSClientCertFile -or
        $SFUControlGRPCTLSClientKeyFile
    if ($manualTLSFiles) {
        throw "-GenerateSFUControlGRPCTestCerts cannot be combined with explicit SFU gRPC control TLS certificate file parameters"
    }
}
if (($SFUControlGRPCTLSServerCertFile -and -not $SFUControlGRPCTLSServerKeyFile) -or
    ($SFUControlGRPCTLSServerKeyFile -and -not $SFUControlGRPCTLSServerCertFile)) {
    throw "-SFUControlGRPCTLSServerCertFile and -SFUControlGRPCTLSServerKeyFile must be configured together"
}
if ($SFUControlGRPCTLSClientCAFile -and (-not $SFUControlGRPCTLSServerCertFile -or -not $SFUControlGRPCTLSServerKeyFile)) {
    throw "-SFUControlGRPCTLSClientCAFile requires server cert/key TLS options"
}
if (($SFUControlGRPCTLSClientCertFile -and -not $SFUControlGRPCTLSClientKeyFile) -or
    ($SFUControlGRPCTLSClientKeyFile -and -not $SFUControlGRPCTLSClientCertFile)) {
    throw "-SFUControlGRPCTLSClientCertFile and -SFUControlGRPCTLSClientKeyFile must be configured together"
}
$sfuControlGRPCServerTLSEnabled = [bool]($GenerateSFUControlGRPCTestCerts -or $SFUControlGRPCTLSServerCertFile -or $SFUControlGRPCTLSServerKeyFile -or $SFUControlGRPCTLSClientCAFile)
$sfuControlGRPCClientTLSEnabled = [bool]($GenerateSFUControlGRPCTestCerts -or $SFUControlGRPCTLSCAFile -or $SFUControlGRPCTLSServerName -or $SFUControlGRPCTLSClientCertFile -or $SFUControlGRPCTLSClientKeyFile)
if ($sfuControlGRPCServerTLSEnabled -and -not $sfuControlGRPCClientTLSEnabled) {
    throw "SFU gRPC control server TLS requires at least one Core/probe client TLS option, such as -SFUControlGRPCTLSCAFile or -SFUControlGRPCTLSServerName"
}
if ($sfuControlGRPCClientTLSEnabled -and -not $sfuControlGRPCServerTLSEnabled) {
    throw "SFU gRPC control client TLS options require server cert/key TLS options in this smoke script"
}
if ($SFUControlGRPCTLSClientCAFile -and (-not $SFUControlGRPCTLSClientCertFile -or -not $SFUControlGRPCTLSClientKeyFile)) {
    throw "-SFUControlGRPCTLSClientCAFile enables mTLS and requires client cert/key TLS options"
}
if ($BuildOnly -and ($RunLogSafetyGate -or $RunPortCleanupGate)) {
    throw "-RunLogSafetyGate and -RunPortCleanupGate cannot be combined with -BuildOnly"
}
if ($PreflightOnly -and $BuildOnly) {
    throw "-PreflightOnly cannot be combined with -BuildOnly"
}
if ($PreflightOnly -and $KeepRunning) {
    throw "-PreflightOnly cannot be combined with -KeepRunning"
}
if ($PreflightOnly -and ($MultiInstance -or
        $RunRemoteOwnerProbe -or
        $RunRemoteOwnerCapacityProbe -or
        $RunRemoteOwnerFailoverProbe -or
        $RunRemoteOwnerTTLProbe -or
        $RunMediaE2EProbe -or
        $RunLogSafetyGate -or
        $RunPortCleanupGate -or
        $GenerateSFUControlGRPCTestCerts)) {
    throw "-PreflightOnly only supports the Redis registry gate and optional -RunCoreRemoteConfigGate"
}
if ($KeepRunning -and ($RunLogSafetyGate -or $RunPortCleanupGate)) {
    throw "-RunLogSafetyGate and -RunPortCleanupGate require the smoke helper to clean up processes; remove -KeepRunning"
}

$WorkDir = [System.IO.Path]::GetFullPath($WorkDir)
$ExePath = [System.IO.Path]::GetFullPath($ExePath)
$CoreExePath = [System.IO.Path]::GetFullPath($CoreExePath)
$LogDir = [System.IO.Path]::GetFullPath($LogDir)
$BinDir = Split-Path -Parent $ExePath
$CoreBinDir = Split-Path -Parent $CoreExePath
$StartedProcesses = @()
$GeneratedSFUControlGRPCTestCertDir = ""

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "== $Message =="
}

function Invoke-External {
    param(
        [string]$FilePath,
        [string[]]$Arguments,
        [switch]$AllowFailure
    )
    $oldErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & $FilePath @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $oldErrorActionPreference
    }
    $text = ($output | ForEach-Object { $_.ToString() }) -join "`n"
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "$FilePath $($Arguments -join ' ') failed with exit code ${exitCode}:`n$text"
    }
    [pscustomobject]@{
        ExitCode = $exitCode
        Output = $text
    }
}

function Get-GitOutput {
    param([string[]]$Arguments, [string]$Default = "unknown")
    $res = Invoke-External "git" $Arguments -AllowFailure
    if ($res.ExitCode -ne 0) {
        return $Default
    }
    $text = $res.Output.Trim()
    if ($text.Length -eq 0) {
        return $Default
    }
    return $text
}

function Build-GoBinary {
    param([string]$Package, [string]$OutputPath, [string]$Name)
    $commit = Get-GitOutput @("rev-parse", "HEAD")
    $branch = Get-GitOutput @("branch", "--show-current")
    $dirty = Get-GitOutput @("status", "--porcelain", "--untracked-files=no") -Default ""
    $treeState = "clean"
    if ($dirty.Length -gt 0) {
        $treeState = "dirty"
    }
    $buildTime = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $ldflags = "-X main.gitCommit=$commit -X main.gitBranch=$branch -X main.gitTreeState=$treeState -X main.buildTime=$buildTime"
    Invoke-External "go" @("build", "-ldflags", $ldflags, "-o", $OutputPath, $Package) | Out-Null
    Write-Host "[ok] built $Name $OutputPath"
    Write-Host "[ok] $Name commit=$commit branch=$branch tree=$treeState build_time=$buildTime"
}

function Invoke-SFURedisRegistryGate {
    param([string]$Address, [string]$Password, [int]$DB)
    if (-not $Address) {
        throw "SFU Redis registry gate requires -RedisAddr"
    }
    $saved = @{
        TELESRV_TEST_REDIS_ADDR = [Environment]::GetEnvironmentVariable("TELESRV_TEST_REDIS_ADDR", "Process")
        TELESRV_TEST_REDIS_PASSWORD = [Environment]::GetEnvironmentVariable("TELESRV_TEST_REDIS_PASSWORD", "Process")
        TELESRV_TEST_REDIS_DB = [Environment]::GetEnvironmentVariable("TELESRV_TEST_REDIS_DB", "Process")
    }
    [Environment]::SetEnvironmentVariable("TELESRV_TEST_REDIS_ADDR", $Address, "Process")
    [Environment]::SetEnvironmentVariable("TELESRV_TEST_REDIS_PASSWORD", $Password, "Process")
    [Environment]::SetEnvironmentVariable("TELESRV_TEST_REDIS_DB", [string]$DB, "Process")
    try {
        Invoke-External "go" @(
            "test",
            "./internal/sfu",
            "-run",
            "TestRedisSFU(InstanceRegistrySelectorAndOwnerClaim|OwnerServiceRoutesSelectedRemoteInstanceThroughHTTP|InstanceRegistryReplacesSameControlAddr|OwnerServiceSkipsUnhealthyAndFullRemoteInstances|RegistriesExpireByRecordTimestamp)",
            "-count=1",
            "-v"
        ) | Out-Null
    } finally {
        foreach ($key in $saved.Keys) {
            [Environment]::SetEnvironmentVariable($key, $saved[$key], "Process")
        }
    }
    Write-Host "[ok] sfu redis registry gate passed: $Address db=$DB"
}

function Invoke-CoreRemoteConfigGate {
    param([string]$Stamp)
    $cases = @(
        [pscustomobject]@{
            Name = "missing-sfu-control-token"
            Expected = "TELESRV_SFU_CONTROL_TOKEN is required by cmd/telesrv-core and cmd/telesrv-sfu"
            Env = @{
                TELESRV_SFU_CONTROL_TOKEN = ""
                TELESRV_GROUPCALL_CONTROL_TOKEN = $GroupCallControlToken
            }
        },
        [pscustomobject]@{
            Name = "missing-groupcall-control-token"
            Expected = "TELESRV_GROUPCALL_CONTROL_TOKEN is required by cmd/telesrv-core and cmd/telesrv-sfu"
            Env = @{
                TELESRV_SFU_CONTROL_TOKEN = $SFUControlToken
                TELESRV_GROUPCALL_CONTROL_TOKEN = ""
            }
        }
    )
    foreach ($case in $cases) {
        $stdout = Join-Path $LogDir "core-remote-config-$($case.Name)-$Stamp.out.log"
        $stderr = Join-Path $LogDir "core-remote-config-$($case.Name)-$Stamp.err.log"
        $configPath = Join-Path $LogDir "core-remote-config-$($case.Name)-$Stamp.yaml"
        @'
version: 1
public:
  dc: 2
  advertise:
    ip: 127.0.0.1
    port: 2398
postgres:
  dsn: postgres://telesrv:telesrv@127.0.0.1:5432/telesrv?sslmode=disable
redis:
  addr: 127.0.0.1:6399
group_call:
  control_addr: ${TELESRV_GROUPCALL_CONTROL_ADDR}
  control_token: ${TELESRV_GROUPCALL_CONTROL_TOKEN}
core_exec:
  addr: ${TELESRV_CORE_EXEC_GRPC_ADDR}
  token: ${TELESRV_CORE_EXEC_TOKEN}
file_data:
  resolver: static
  targets:
    - 127.0.0.1:2520
  token: test-file-token
sfu:
  control:
    token: ${TELESRV_SFU_CONTROL_TOKEN}
'@ | Set-Content -LiteralPath $configPath -Encoding UTF8
        $env = @{
            TELESRV_CONFIG = $configPath
            TELESRV_GROUPCALL_CONTROL_ADDR = "127.0.0.1:2420"
            TELESRV_CORE_EXEC_GRPC_ADDR = "127.0.0.1:2440"
            TELESRV_CORE_EXEC_TOKEN = "sfu-remote-coreexec"
        }
        foreach ($key in $case.Env.Keys) {
            $env[$key] = $case.Env[$key]
        }
        $saved = @{}
        foreach ($key in $env.Keys) {
            $saved[$key] = [Environment]::GetEnvironmentVariable($key, "Process")
            [Environment]::SetEnvironmentVariable($key, [string]$env[$key], "Process")
        }
        $proc = $null
        try {
            $proc = Start-Process -FilePath $CoreExePath `
                -WorkingDirectory $WorkDir `
                -RedirectStandardOutput $stdout `
                -RedirectStandardError $stderr `
                -PassThru `
                -WindowStyle Hidden
            if (-not $proc.WaitForExit(10000)) {
                Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
                throw "cmd/telesrv-core remote SFU config gate '$($case.Name)' did not exit within 10s"
            }
            $output = ((Get-LogTail $stdout $Tail), (Get-LogTail $stderr $Tail)) -join "`n"
            if ($proc.ExitCode -eq 0) {
                throw "cmd/telesrv-core remote SFU config gate '$($case.Name)' unexpectedly exited 0:`n$output"
            }
            if ($output -notmatch [regex]::Escape($case.Expected)) {
                throw "cmd/telesrv-core remote SFU config gate '$($case.Name)' did not emit expected error '$($case.Expected)':`n$output"
            }
            Write-Host "[ok] core remote config gate $($case.Name): $($case.Expected)"
        } finally {
            foreach ($key in $env.Keys) {
                [Environment]::SetEnvironmentVariable($key, $saved[$key], "Process")
            }
            if ($proc) {
                try {
                    $proc.Refresh()
                    if (-not $proc.HasExited) {
                        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
                    }
                } catch {
                    # Process already exited.
                }
            }
        }
    }
}

function Invoke-SFURegistryProbe {
    param(
        [string]$Address,
        [string]$Password,
        [int]$DB,
        [string[]]$ExpectedInstances,
        [switch]$RequireControlHealth,
        [switch]$ExpectControlFailure,
        [AllowEmptyString()][string]$ControlTokenOverride = $SFUControlToken
    )
    if (-not $Address) {
        throw "SFU registry probe requires -RedisAddr"
    }
    if ($ExpectedInstances.Count -eq 0) {
        throw "SFU registry probe requires expected instances"
    }
    $args = @(
        "run",
        "./scripts/sfu-registry-probe",
        "-redis-addr",
        $Address,
        "-redis-db",
        [string]$DB,
        "-expect-instances",
        ($ExpectedInstances -join ","),
        "-timeout",
        "${StartupTimeoutSeconds}s"
    )
    if ($RequireControlHealth) {
        $args += @(
            "-require-control-health",
            "-control-token",
            $ControlTokenOverride
        )
    }
    if ($ExpectControlFailure) {
        $args += @("-expect-control-failure")
    }
    $args = Add-SFUControlGRPCClientTLSArgs $args
    if ($Password) {
        $args += @("-redis-password", $Password)
    }
    $result = Invoke-External "go" $args
    if ($result.Output.Trim().Length -gt 0) {
        Write-Host $result.Output.Trim()
    }
}

function Invoke-SFURemoteOwnerProbe {
    param(
        [string]$Address,
        [string]$Password,
        [int]$DB,
        [string[]]$ExpectedInstances,
        [string[]]$ForbiddenInstances,
        [bool]$RunCapacityProbe,
        [bool]$RunTTLProbe
    )
    if (-not $Address) {
        throw "SFU remote owner probe requires -RedisAddr"
    }
    if ([string]::IsNullOrWhiteSpace($SFUControlToken)) {
        throw "SFU remote owner probe requires -SFUControlToken"
    }
    if ($ExpectedInstances.Count -eq 0) {
        throw "SFU remote owner probe requires expected instances"
    }
    $args = @(
        "run",
        "./scripts/sfu-remote-owner-probe",
        "-redis-addr",
        $Address,
        "-redis-db",
        [string]$DB,
        "-control-token",
        $SFUControlToken,
        "-expect-instances",
        ($ExpectedInstances -join ","),
        "-timeout",
        "${StartupTimeoutSeconds}s"
    )
    if ($RunTTLProbe) {
        $args += @("-run-owner-ttl-probe", "-owner-ttl", $RemoteOwnerProbeOwnerTTL)
    }
    if ($Password) {
        $args += @("-redis-password", $Password)
    }
    if ($ForbiddenInstances -and $ForbiddenInstances.Count -gt 0) {
        $args += @("-forbid-instances", ($ForbiddenInstances -join ","))
    }
    if ($RunCapacityProbe) {
        $args += @("-run-capacity-probe")
    }
    $args = Add-SFUControlGRPCClientTLSArgs $args
    $result = Invoke-External "go" $args
    if ($result.Output.Trim().Length -gt 0) {
        Write-Host $result.Output.Trim()
    }
}

function Invoke-SFUMediaE2EProbe {
    param(
        [string]$ControlURL,
        [string]$Name
    )
    if ([string]::IsNullOrWhiteSpace($SFUControlToken)) {
        throw "SFU media E2E probe requires -SFUControlToken"
    }
    if ([string]::IsNullOrWhiteSpace($ControlURL)) {
        throw "SFU media E2E probe requires control URL"
    }
    $args = @(
        "run",
        "./scripts/sfu-media-probe",
        "-control-addr",
        $ControlURL,
        "-control-token",
        $SFUControlToken,
        "-min-forwarded-packets",
        $MediaE2EMinPackets,
        "-media-duration",
        $MediaE2EDuration,
        "-timeout",
        "${StartupTimeoutSeconds}s"
    )
    $args = Add-SFUControlGRPCClientTLSArgs $args
    $result = Invoke-External "go" $args
    $output = $result.Output.Trim()
    if ($output.Length -gt 0) {
        Write-Host "[ok] $Name sfu media e2e: $output"
    } else {
        Write-Host "[ok] $Name sfu media e2e passed"
    }
}

function Get-ListenPort {
    param([string]$Address)
    if ($Address -match '^\[.+\]:(\d+)$') {
        return [int]$Matches[1]
    }
    if ($Address -match ':(\d+)$') {
        return [int]$Matches[1]
    }
    throw "Cannot parse listen port from '$Address'"
}

function Assert-TCPPortFree {
    param([int]$Port, [string]$Name)
    $listeners = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)
    if ($listeners.Count -ne 0) {
        throw "$Name TCP port $Port is already listening"
    }
}

function Assert-UDPPortFree {
    param([int]$Port, [string]$Name)
    $listeners = @(Get-NetUDPEndpoint -LocalPort $Port -ErrorAction SilentlyContinue)
    if ($listeners.Count -ne 0) {
        throw "$Name UDP port $Port is already bound"
    }
}

function Invoke-SFULogSafetyGate {
    param([string[]]$Paths)
    $pathsToScan = @($Paths | Where-Object { $_ } | Select-Object -Unique)
    if ($pathsToScan.Count -eq 0) {
        throw "-RunLogSafetyGate requested but no SFU role logs were recorded"
    }
    $pattern = '\b(ERROR|FATAL|PANIC)\b|\bpanic\b|\bfatal\b|\bfailed\b|not ready|NOT_IMPLEMENTED|Unhandled RPC|bad_msg|RPC internal error|500 INTERNAL|DeadlineExceeded|ENCRYPTED_MESSAGE_INVALID|FLOOD_WAIT'
    $hits = @()
    foreach ($path in $pathsToScan) {
        if (-not (Test-Path -LiteralPath $path)) {
            $hits += [pscustomobject]@{ Path = $path; LineNumber = 0; Line = "missing log file" }
            continue
        }
        $matches = @(Select-String -LiteralPath $path -Pattern $pattern -ErrorAction SilentlyContinue)
        foreach ($match in $matches) {
            $hits += [pscustomobject]@{
                Path = $path
                LineNumber = $match.LineNumber
                Line = $match.Line.Trim()
            }
        }
    }
    if ($hits.Count -gt 0) {
        $summary = ($hits | Select-Object -First 40 | ForEach-Object {
            "$($_.Path):$($_.LineNumber): $($_.Line)"
        }) -join "`n"
        throw "SFU log safety gate failed with $($hits.Count) dangerous log matches:`n$summary"
    }
    Write-Host "[ok] sfu log safety gate passed: scanned $($pathsToScan.Count) role logs"
}

function Invoke-SFUPortCleanupGate {
    param([int[]]$TCPPorts, [int[]]$UDPPorts)
    $tcp = @($TCPPorts | Where-Object { $_ -gt 0 } | Select-Object -Unique)
    $udp = @($UDPPorts | Where-Object { $_ -gt 0 } | Select-Object -Unique)
    if ($tcp.Count -eq 0 -and $udp.Count -eq 0) {
        throw "-RunPortCleanupGate requested but no SFU ports were recorded"
    }
    $deadline = (Get-Date).AddSeconds(5)
    $remainingTCP = @()
    $remainingUDP = @()
    do {
        $remainingTCP = @()
        foreach ($port in $tcp) {
            $listeners = @(Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue)
            if ($listeners.Count -gt 0) {
                $remainingTCP += $port
            }
        }
        $remainingUDP = @()
        foreach ($port in $udp) {
            $listeners = @(Get-NetUDPEndpoint -LocalPort $port -ErrorAction SilentlyContinue)
            if ($listeners.Count -gt 0) {
                $remainingUDP += $port
            }
        }
        if ($remainingTCP.Count -eq 0 -and $remainingUDP.Count -eq 0) {
            $labels = @()
            if ($tcp.Count -gt 0) {
                $labels += "tcp=$($tcp -join ',')"
            }
            if ($udp.Count -gt 0) {
                $labels += "udp=$($udp -join ',')"
            }
            Write-Host "[ok] sfu port cleanup gate passed: $($labels -join ' ')"
            return
        }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $deadline)
    throw "SFU port cleanup gate failed: tcp=$($remainingTCP -join ',') udp=$($remainingUDP -join ',') still bound"
}

function Get-LogTail {
    param([string]$Path, [int]$LineCount)
    if (-not (Test-Path -LiteralPath $Path)) {
        return ""
    }
    return (Get-Content -LiteralPath $Path -Tail $LineCount -ErrorAction SilentlyContinue) -join "`n"
}

function Resolve-OptionalFile {
    param([string]$Path, [string]$Name)
    if (-not $Path) {
        return ""
    }
    $fullPath = [System.IO.Path]::GetFullPath($Path)
    if (-not (Test-Path -LiteralPath $fullPath -PathType Leaf)) {
        throw "$Name file does not exist: $fullPath"
    }
    return $fullPath
}

function Add-SFUControlGRPCServerTLSEnv {
    param([hashtable]$Environment)
    if ($SFUControlGRPCTLSServerCertFile) {
        $Environment["TELESRV_SFU_CONTROL_GRPC_TLS_CERT_FILE"] = $SFUControlGRPCTLSServerCertFile
    }
    if ($SFUControlGRPCTLSServerKeyFile) {
        $Environment["TELESRV_SFU_CONTROL_GRPC_TLS_KEY_FILE"] = $SFUControlGRPCTLSServerKeyFile
    }
    if ($SFUControlGRPCTLSClientCAFile) {
        $Environment["TELESRV_SFU_CONTROL_GRPC_TLS_CLIENT_CA_FILE"] = $SFUControlGRPCTLSClientCAFile
    }
}

function Add-SFUControlGRPCClientTLSArgs {
    param([object[]]$Arguments)
    $out = @($Arguments)
    if ($SFUControlGRPCTLSCAFile) {
        $out += @("-control-grpc-tls-ca-file", $SFUControlGRPCTLSCAFile)
    }
    if ($SFUControlGRPCTLSServerName) {
        $out += @("-control-grpc-tls-server-name", $SFUControlGRPCTLSServerName)
    }
    if ($SFUControlGRPCTLSClientCertFile) {
        $out += @("-control-grpc-tls-client-cert-file", $SFUControlGRPCTLSClientCertFile)
    }
    if ($SFUControlGRPCTLSClientKeyFile) {
        $out += @("-control-grpc-tls-client-key-file", $SFUControlGRPCTLSClientKeyFile)
    }
    return $out
}

function Test-ProcessExited {
    param([System.Diagnostics.Process]$Process)
    $Process.Refresh()
    return $Process.HasExited
}

function Wait-LogContains {
    param(
        [System.Diagnostics.Process]$Process,
        [string]$Path,
        [string]$Pattern,
        [datetime]$Deadline
    )
    while ((Get-Date) -lt $Deadline) {
        if (Test-ProcessExited $Process) {
            $tailText = Get-LogTail $Path $Tail
            throw "telesrv-sfu exited during startup with code $($Process.ExitCode):`n$tailText"
        }
        $tailText = Get-LogTail $Path $Tail
        if ($tailText -match $Pattern) {
            return
        }
        Start-Sleep -Milliseconds 250
    }
    $lastTail = Get-LogTail $Path $Tail
    throw "telesrv-sfu did not write expected log pattern '$Pattern' within ${StartupTimeoutSeconds}s:`n$lastTail"
}

function Wait-UDPPortForProcess {
    param(
        [System.Diagnostics.Process]$Process,
        [int]$Port,
        [string]$LogPath,
        [datetime]$Deadline
    )
    while ((Get-Date) -lt $Deadline) {
        if (Test-ProcessExited $Process) {
            $tailText = Get-LogTail $LogPath $Tail
            throw "telesrv-sfu exited during UDP startup with code $($Process.ExitCode):`n$tailText"
        }
        $listeners = @(Get-NetUDPEndpoint -LocalPort $Port -ErrorAction SilentlyContinue |
            Where-Object { $_.OwningProcess -eq $Process.Id })
        if ($listeners.Count -gt 0) {
            return
        }
        Start-Sleep -Milliseconds 250
    }
    $tailText = Get-LogTail $LogPath $Tail
    throw "telesrv-sfu PID $($Process.Id) did not bind UDP port $Port within ${StartupTimeoutSeconds}s:`n$tailText"
}

function Start-SFU {
    param([hashtable]$Environment, [string]$StdoutPath, [string]$StderrPath)
    $saved = @{}
    foreach ($key in $Environment.Keys) {
        $saved[$key] = [Environment]::GetEnvironmentVariable($key, "Process")
        [Environment]::SetEnvironmentVariable($key, [string]$Environment[$key], "Process")
    }
    try {
        $proc = Start-Process -FilePath $ExePath `
            -WorkingDirectory $WorkDir `
            -RedirectStandardOutput $StdoutPath `
            -RedirectStandardError $StderrPath `
            -PassThru `
            -WindowStyle Hidden
        $script:StartedProcesses += $proc
        Write-Host "[ok] started telesrv-sfu PID $($proc.Id)"
        Write-Host "[ok] stdout: $StdoutPath"
        Write-Host "[ok] stderr: $StderrPath"
        return $proc
    } finally {
        foreach ($key in $Environment.Keys) {
            [Environment]::SetEnvironmentVariable($key, $saved[$key], "Process")
        }
    }
}

function Stop-SFU {
    if (-not $StartedProcesses -or $StartedProcesses.Count -eq 0) {
        return
    }
    foreach ($process in $StartedProcesses) {
        try {
            $process.Refresh()
            if (-not $process.HasExited) {
                Write-Host "[stop] PID $($process.Id) $($process.ProcessName)"
                Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
            }
        } catch {
            # Process already exited.
        }
    }
}

function Stop-VerifiedSFU {
    param([pscustomobject]$Started)
    if (-not $Started) {
        return
    }
    try {
        $proc = Get-Process -Id $Started.PID -ErrorAction SilentlyContinue
        if ($proc) {
            Write-Host "[stop] failover probe stopping $($Started.Name) PID $($Started.PID)"
            Stop-Process -Id $Started.PID -Force -ErrorAction SilentlyContinue
            $deadline = (Get-Date).AddSeconds(10)
            while ((Get-Date) -lt $deadline) {
                $again = Get-Process -Id $Started.PID -ErrorAction SilentlyContinue
                if (-not $again) {
                    return
                }
                Start-Sleep -Milliseconds 200
            }
            throw "SFU failover probe could not stop PID $($Started.PID)"
        }
    } catch {
        throw
    }
}

function Start-VerifiedSFU {
    param(
        [string]$Name,
        [string]$CurrentInstanceID,
        [string]$CurrentControlAddr,
        [string]$CurrentControlURL,
        [int]$CurrentUDPPort,
        [int]$CurrentMaxActiveCalls,
        [string]$Stamp
    )
    $stdout = Join-Path $LogDir "sfu-$Name-$Stamp.out.log"
    $stderr = Join-Path $LogDir "sfu-$Name-$Stamp.err.log"
    $configPath = Join-Path $LogDir "sfu-$Name-$Stamp.yaml"
    @'
version: 1
instance:
  id: ${TELESRV_INSTANCE_ID}
public:
  advertise:
    ip: ${TELESRV_ADVERTISE_IP}
redis:
  addr: ${TELESRV_REDIS_ADDR}
  password: ${TELESRV_REDIS_PASSWORD}
  db: ${TELESRV_REDIS_DB}
sfu:
  udp_port: ${TELESRV_SFU_UDP_PORT}
  advertise_ip: ${TELESRV_SFU_ADVERTISE_IP}
  instance_heartbeat_interval: ${TELESRV_SFU_INSTANCE_HEARTBEAT_INTERVAL}
  max_active_calls: ${TELESRV_SFU_INSTANCE_MAX_ACTIVE_CALLS}
  control:
    addr: ${TELESRV_SFU_CONTROL_GRPC_ADDR}
    url: ${TELESRV_SFU_CONTROL_GRPC_URL}
    token: ${TELESRV_SFU_CONTROL_TOKEN}
    tls:
      cert_file: ${TELESRV_SFU_CONTROL_GRPC_TLS_CERT_FILE}
      key_file: ${TELESRV_SFU_CONTROL_GRPC_TLS_KEY_FILE}
      client_ca_file: ${TELESRV_SFU_CONTROL_GRPC_TLS_CLIENT_CA_FILE}
group_call:
  control_url: ${TELESRV_GROUPCALL_CONTROL_URL}
  control_token: ${TELESRV_GROUPCALL_CONTROL_TOKEN}
'@ | Set-Content -LiteralPath $configPath -Encoding UTF8
    $env = @{
        TELESRV_CONFIG = $configPath
        TELESRV_INSTANCE_ID = $CurrentInstanceID
        TELESRV_REDIS_ADDR = $RedisAddr
        TELESRV_REDIS_PASSWORD = $RedisPassword
        TELESRV_REDIS_DB = $RedisDB
        TELESRV_ADVERTISE_IP = $AdvertiseIP
        TELESRV_SFU_ADVERTISE_IP = $SFUAdvertiseIP
        TELESRV_SFU_CONTROL_GRPC_ADDR = $CurrentControlAddr
        TELESRV_SFU_CONTROL_GRPC_URL = $CurrentControlURL
        TELESRV_SFU_CONTROL_TOKEN = $SFUControlToken
        TELESRV_SFU_UDP_PORT = $CurrentUDPPort
        TELESRV_SFU_INSTANCE_MAX_ACTIVE_CALLS = $CurrentMaxActiveCalls
        TELESRV_SFU_INSTANCE_HEARTBEAT_INTERVAL = $SFUInstanceHeartbeatInterval
        TELESRV_GROUPCALL_CONTROL_URL = $GroupCallControlURL
        TELESRV_GROUPCALL_CONTROL_TOKEN = $GroupCallControlToken
    }
    Add-SFUControlGRPCServerTLSEnv $env

    $proc = Start-SFU $env $stdout $stderr
    $deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
    Wait-UDPPortForProcess $proc $CurrentUDPPort $stderr $deadline
    Write-Host "[ok] $Name sfu udp listening: $CurrentUDPPort"
    Wait-LogContains $proc $stderr "telesrv-sfu started" $deadline
    Invoke-SFURegistryProbe $RedisAddr $RedisPassword $RedisDB @($CurrentInstanceID) -RequireControlHealth
    Write-Host "[ok] $Name sfu grpc control health: $CurrentControlURL"
    Invoke-SFURegistryProbe $RedisAddr $RedisPassword $RedisDB @($CurrentInstanceID) -RequireControlHealth -ExpectControlFailure -ControlTokenOverride "$SFUControlToken-invalid"
    Write-Host "[ok] $Name sfu grpc control rejects invalid bearer token"
    if ($RunMediaE2EProbe) {
        Invoke-SFUMediaE2EProbe $CurrentControlURL $Name
        Start-Sleep -Seconds 2
    }

    [pscustomobject]@{
        PID = $proc.Id
        Name = $Name
        InstanceID = $CurrentInstanceID
        ControlURL = $CurrentControlURL
        UDPPort = $CurrentUDPPort
        Log = $stderr
    }
}

$controlPort = Get-ListenPort $SFUControlGRPCAddr
$secondControlPort = Get-ListenPort $SecondSFUControlGRPCAddr
if ($SFUUDPPort -le 0 -or $SFUUDPPort -gt 65535) {
    throw "-SFUUDPPort must be in 1..65535"
}
if ($SecondSFUUDPPort -le 0 -or $SecondSFUUDPPort -gt 65535) {
    throw "-SecondSFUUDPPort must be in 1..65535"
}
if ($MultiInstance -and $controlPort -eq $secondControlPort) {
    throw "-SecondSFUControlGRPCAddr must use a different port from -SFUControlGRPCAddr"
}
if ($MultiInstance -and $SFUUDPPort -eq $SecondSFUUDPPort) {
    throw "-SecondSFUUDPPort must differ from -SFUUDPPort"
}
New-Item -ItemType Directory -Force -Path $BinDir | Out-Null
New-Item -ItemType Directory -Force -Path $CoreBinDir | Out-Null
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

if ($GenerateSFUControlGRPCTestCerts) {
    if (-not $SFUControlGRPCTLSServerName) {
        $SFUControlGRPCTLSServerName = "sfu.internal"
    }
    $GeneratedSFUControlGRPCTestCertDir = Join-Path $LogDir "sfu-control-mtls-certs"
    $certGenPath = Join-Path $RepoRoot "scripts\coreexec-smoke-certgen.go"
    Invoke-External "go" @(
        "run",
        $certGenPath,
        "-out",
        $GeneratedSFUControlGRPCTestCertDir,
        "-server-name",
        $SFUControlGRPCTLSServerName
    ) | Out-Null
    $SFUControlGRPCTLSServerCertFile = Join-Path $GeneratedSFUControlGRPCTestCertDir "server.pem"
    $SFUControlGRPCTLSServerKeyFile = Join-Path $GeneratedSFUControlGRPCTestCertDir "server-key.pem"
    $SFUControlGRPCTLSClientCAFile = Join-Path $GeneratedSFUControlGRPCTestCertDir "ca.pem"
    $SFUControlGRPCTLSCAFile = Join-Path $GeneratedSFUControlGRPCTestCertDir "ca.pem"
    $SFUControlGRPCTLSClientCertFile = Join-Path $GeneratedSFUControlGRPCTestCertDir "client.pem"
    $SFUControlGRPCTLSClientKeyFile = Join-Path $GeneratedSFUControlGRPCTestCertDir "client-key.pem"
    Write-Host "[ok] generated SFU control mTLS smoke certs: $GeneratedSFUControlGRPCTestCertDir"
    Write-Host "[ok] generated SFU control mTLS server name: $SFUControlGRPCTLSServerName"
}

$SFUControlGRPCTLSServerCertFile = Resolve-OptionalFile $SFUControlGRPCTLSServerCertFile "SFU gRPC control server TLS certificate"
$SFUControlGRPCTLSServerKeyFile = Resolve-OptionalFile $SFUControlGRPCTLSServerKeyFile "SFU gRPC control server TLS key"
$SFUControlGRPCTLSClientCAFile = Resolve-OptionalFile $SFUControlGRPCTLSClientCAFile "SFU gRPC control client CA"
$SFUControlGRPCTLSCAFile = Resolve-OptionalFile $SFUControlGRPCTLSCAFile "SFU gRPC control root CA"
$SFUControlGRPCTLSClientCertFile = Resolve-OptionalFile $SFUControlGRPCTLSClientCertFile "SFU gRPC control client certificate"
$SFUControlGRPCTLSClientKeyFile = Resolve-OptionalFile $SFUControlGRPCTLSClientKeyFile "SFU gRPC control client key"
$SFUControlGRPCTLSEnabled = [bool]($SFUControlGRPCTLSServerCertFile)
$SFUControlGRPCMTLSEnabled = [bool]($SFUControlGRPCTLSClientCAFile)
$smokeSucceeded = $false
$cleanupTCPPorts = @($controlPort)
$cleanupUDPPorts = @($SFUUDPPort)
if ($MultiInstance) {
    $cleanupTCPPorts += $secondControlPort
    $cleanupUDPPorts += $SecondSFUUDPPort
}

Push-Location $RepoRoot
try {
    if ($PreflightOnly) {
        if ($RunCoreRemoteConfigGate) {
            if (-not $SkipBuild) {
                Write-Step "Build telesrv-core"
                Build-GoBinary ".\cmd\telesrv-core" $CoreExePath "telesrv-core"
            } elseif (-not (Test-Path -LiteralPath $CoreExePath)) {
                throw "Executable not found: $CoreExePath"
            }
            $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
            Write-Step "Core remote SFU config gate"
            Invoke-CoreRemoteConfigGate $stamp
        }
        Write-Step "SFU Redis registry gate"
        Invoke-SFURedisRegistryGate $RedisAddr $RedisPassword $RedisDB
        Write-Host "[ok] PreflightOnly requested; telesrv-sfu media runtime was not built or started"
        [pscustomobject]@{
            PreflightOnly = $true
            RedisAddr = $RedisAddr
            RedisDB = $RedisDB
            CoreRemoteConfigGateRun = [bool]$RunCoreRemoteConfigGate
            SFURedisRegistryGateRun = $true
        }
        return
    }

    if (-not $SkipBuild) {
        Write-Step "Build telesrv-sfu"
        Build-GoBinary ".\cmd\telesrv-sfu" $ExePath "telesrv-sfu"
        if ($RunCoreRemoteConfigGate) {
            Write-Step "Build telesrv-core"
            Build-GoBinary ".\cmd\telesrv-core" $CoreExePath "telesrv-core"
        }
    } elseif (-not (Test-Path -LiteralPath $ExePath)) {
        throw "Executable not found: $ExePath"
    }
    if ($SkipBuild -and $RunCoreRemoteConfigGate -and -not (Test-Path -LiteralPath $CoreExePath)) {
        throw "Executable not found: $CoreExePath"
    }

    if ($BuildOnly) {
        Write-Host "[ok] BuildOnly requested; sfu smoke runtime was not started"
        return
    }

    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    if ($RunCoreRemoteConfigGate) {
        Write-Step "Core remote SFU config gate"
        Invoke-CoreRemoteConfigGate $stamp
    }

    Write-Step "Preflight"
    if ($SFUControlGRPCTLSEnabled) {
        Write-Host "[ok] SFU grpc control TLS enabled; mTLS=$SFUControlGRPCMTLSEnabled"
    }
    Assert-TCPPortFree $controlPort "SFU control"
    Assert-UDPPortFree $SFUUDPPort "SFU media"
    Write-Host "[ok] SFU control TCP port $controlPort is free"
    Write-Host "[ok] SFU UDP port $SFUUDPPort is free"
    if ($MultiInstance) {
        Assert-TCPPortFree $secondControlPort "second SFU control"
        Assert-UDPPortFree $SecondSFUUDPPort "second SFU media"
        Write-Host "[ok] second SFU control TCP port $secondControlPort is free"
        Write-Host "[ok] second SFU UDP port $SecondSFUUDPPort is free"
    }
    Invoke-SFURedisRegistryGate $RedisAddr $RedisPassword $RedisDB

    Write-Step "Start telesrv-sfu"
    $started = @()
    $started += Start-VerifiedSFU "primary" $InstanceID $SFUControlGRPCAddr $SFUControlGRPCURL $SFUUDPPort $MaxActiveCalls $stamp
    if ($MultiInstance) {
        $started += Start-VerifiedSFU "secondary" $SecondInstanceID $SecondSFUControlGRPCAddr $SecondSFUControlGRPCURL $SecondSFUUDPPort $SecondMaxActiveCalls $stamp
        Write-Step "SFU Redis process registry probe"
        Invoke-SFURegistryProbe $RedisAddr $RedisPassword $RedisDB @($InstanceID, $SecondInstanceID)
    }
    if ($RunRemoteOwnerProbe) {
        Write-Step "SFU remote owner control-plane probe"
        Invoke-SFURemoteOwnerProbe `
            -Address $RedisAddr `
            -Password $RedisPassword `
            -DB $RedisDB `
            -ExpectedInstances (@($started) | ForEach-Object { $_.InstanceID }) `
            -ForbiddenInstances @() `
            -RunCapacityProbe ([bool]$RunRemoteOwnerCapacityProbe) `
            -RunTTLProbe ([bool]$RunRemoteOwnerTTLProbe)
    }
    if ($RunRemoteOwnerFailoverProbe) {
        Write-Step "SFU remote owner unhealthy-instance failover probe"
        $failed = @($started)[0]
        Stop-VerifiedSFU $failed
        Invoke-SFURemoteOwnerProbe `
            -Address $RedisAddr `
            -Password $RedisPassword `
            -DB $RedisDB `
            -ExpectedInstances (@($started) | ForEach-Object { $_.InstanceID }) `
            -ForbiddenInstances @($failed.InstanceID) `
            -RunCapacityProbe $false `
            -RunTTLProbe $false
    }

    Write-Step "Smoke result"
    Write-Host "[ok] standalone sfu smoke passed"
    Write-Host "[ok] instances: $((@($started) | ForEach-Object { $_.InstanceID }) -join ',')"
    Write-Host "[ok] redis: $RedisAddr db=$RedisDB"
    Write-Host "[ok] control_plane: grpc"
    Write-Host "[ok] control_grpc_tls: $SFUControlGRPCTLSEnabled mtls=$SFUControlGRPCMTLSEnabled"
    Write-Host "[ok] control_urls: $((@($started) | ForEach-Object { $_.ControlURL }) -join ',')"
    Write-Host "[ok] control_auth: bearer required"
    Write-Host "[ok] core_remote_config_gate: $RunCoreRemoteConfigGate"
    Write-Host "[ok] remote_owner_probe: $RunRemoteOwnerProbe"
    Write-Host "[ok] remote_owner_capacity_probe: $RunRemoteOwnerCapacityProbe"
    Write-Host "[ok] remote_owner_failover_probe: $RunRemoteOwnerFailoverProbe"
    Write-Host "[ok] remote_owner_ttl_probe: $RunRemoteOwnerTTLProbe"
    Write-Host "[ok] media_e2e_probe: $RunMediaE2EProbe min_packets=$MediaE2EMinPackets duration=$MediaE2EDuration"
    Write-Host "[ok] udp_ports: $((@($started) | ForEach-Object { $_.UDPPort }) -join ',')"
    Write-Host "[ok] logs: $((@($started) | ForEach-Object { $_.Log }) -join ',')"
    Write-Host "[ok] post-smoke log safety gate requested: $RunLogSafetyGate"
    Write-Host "[ok] post-smoke port cleanup gate requested: $RunPortCleanupGate"
    if ($KeepRunning) {
        Write-Host "[ok] KeepRunning requested; leave PIDs $((@($started) | ForEach-Object { $_.PID }) -join ',') running"
    }

    [pscustomobject]@{
        PIDs = @($started) | ForEach-Object { $_.PID }
        InstanceIDs = @($started) | ForEach-Object { $_.InstanceID }
        RedisAddr = $RedisAddr
        RedisDB = $RedisDB
        ControlPlane = "grpc"
        ControlGRPCTLSEnabled = [bool]$SFUControlGRPCTLSEnabled
        ControlGRPCMTLSEnabled = [bool]$SFUControlGRPCMTLSEnabled
        GeneratedSFUControlGRPCTestCerts = [bool]$GenerateSFUControlGRPCTestCerts
        GeneratedSFUControlGRPCTestCertDir = $GeneratedSFUControlGRPCTestCertDir
        SFUControlGRPCTLSServerName = $SFUControlGRPCTLSServerName
        SFUControlGRPCURLs = @($started) | ForEach-Object { $_.ControlURL }
        SFUUDPPorts = @($started) | ForEach-Object { $_.UDPPort }
        Logs = @($started) | ForEach-Object { $_.Log }
        CoreRemoteConfigGateRun = [bool]$RunCoreRemoteConfigGate
        RemoteOwnerProbeRun = [bool]$RunRemoteOwnerProbe
        RemoteOwnerCapacityProbeRun = [bool]$RunRemoteOwnerCapacityProbe
        RemoteOwnerFailoverProbeRun = [bool]$RunRemoteOwnerFailoverProbe
        RemoteOwnerTTLProbeRun = [bool]$RunRemoteOwnerTTLProbe
        MediaE2EProbeRun = [bool]$RunMediaE2EProbe
        MediaE2EMinPackets = $MediaE2EMinPackets
        MediaE2EDuration = $MediaE2EDuration
        LogSafetyGateRun = [bool]$RunLogSafetyGate
        PortCleanupGateRun = [bool]$RunPortCleanupGate
        KeptRunning = [bool]$KeepRunning
    }
    $smokeSucceeded = $true
} finally {
    Pop-Location
    if (-not $KeepRunning) {
        Stop-SFU
        if ($smokeSucceeded) {
            if ($RunPortCleanupGate) {
                Invoke-SFUPortCleanupGate -TCPPorts $cleanupTCPPorts -UDPPorts $cleanupUDPPorts
            }
            if ($RunLogSafetyGate) {
                Invoke-SFULogSafetyGate -Paths (@($started) | ForEach-Object { $_.Log })
            }
        }
    }
}
