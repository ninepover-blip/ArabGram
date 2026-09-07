<#
.SYNOPSIS
Runs fast package/Redis preflight gates for the Edge/Core/SFU microservice track.

.DESCRIPTION
This script is a quick CI/deployment sanity gate. It does not build or start
Core, Edge, or telesrv-sfu role processes. It delegates to the checked-in
PreflightOnly modes:

- smoke-edge-core-split.ps1 for Redis fabric and CoreExec fixed failure
  classification.
- smoke-sfu-remote.ps1 for Core remote SFU config fail-fast and SFU
  owner/instance Redis registry semantics.

It is not a replacement for the 2 Edge + 2 Core process smoke, MTProto probes,
CoreExec rolling restart, SFU media E2E, log safety, port cleanup, or real
TDesktop/TDLib acceptance.
#>
[CmdletBinding()]
param(
    [string]$RedisAddr,
    [string]$RedisPassword,
    [int]$RedisDB = 0,
    [switch]$SkipEdgeCorePreflight,
    [switch]$SkipSFUPreflight,
    [switch]$SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
if (-not $RedisAddr) {
    $RedisAddr = [Environment]::GetEnvironmentVariable("TELESRV_REDIS_ADDR", "Process")
}
if (-not $RedisAddr) {
    throw "-RedisAddr is required unless TELESRV_REDIS_ADDR is set"
}
if ($SkipEdgeCorePreflight -and $SkipSFUPreflight) {
    throw "At least one preflight must run"
}

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "== $Message =="
}

Push-Location $RepoRoot
try {
    if (-not $SkipEdgeCorePreflight) {
        Write-Step "Edge/Core Redis + CoreExec preflight"
        $edgeArgs = @{
            PreflightOnly = $true
            RunRedisFabricGate = $true
            RunCoreExecFailureClassificationGate = $true
            RedisAddr = $RedisAddr
        }
        & (Join-Path $PSScriptRoot "smoke-edge-core-split.ps1") @edgeArgs
    }

    if (-not $SkipSFUPreflight) {
        Write-Step "SFU remote config + registry preflight"
        $sfuArgs = @{
            PreflightOnly = $true
            RunCoreRemoteConfigGate = $true
            RedisAddr = $RedisAddr
            RedisDB = $RedisDB
        }
        if ($RedisPassword) {
            $sfuArgs["RedisPassword"] = $RedisPassword
        }
        if ($SkipBuild) {
            $sfuArgs["SkipBuild"] = $true
        }
        & (Join-Path $PSScriptRoot "smoke-sfu-remote.ps1") @sfuArgs
    }

    Write-Host ""
    Write-Host "[ok] microservice preflight passed"
    [pscustomobject]@{
        MicroservicePreflight = $true
        RedisAddr = $RedisAddr
        RedisDB = $RedisDB
        EdgeCorePreflightRun = -not [bool]$SkipEdgeCorePreflight
        RedisFabricGateRun = -not [bool]$SkipEdgeCorePreflight
        CoreExecFailureClassificationGateRun = -not [bool]$SkipEdgeCorePreflight
        SFUPreflightRun = -not [bool]$SkipSFUPreflight
        CoreRemoteConfigGateRun = -not [bool]$SkipSFUPreflight
        SFURedisRegistryGateRun = -not [bool]$SkipSFUPreflight
    }
} finally {
    Pop-Location
}
