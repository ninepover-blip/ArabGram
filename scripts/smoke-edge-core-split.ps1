<#
.SYNOPSIS
Builds and smoke-tests telesrv core/edge split mode.

.DESCRIPTION
This script builds the dedicated cmd/telesrv-core binary for Core, the
cmd/telesrv-egress binary for Durable Egress, and the cmd/telesrv-edge binary for
Edge. It verifies the selected CoreExec transport, the Egress Delivery boundary, each
edge MTProto listener, and writes separate logs for every role. By default it
starts one Core, one Egress, and one Edge; with -MultiInstance it starts two
CoreExec gRPC listeners, two Egress Delivery gRPC listeners, and two Edge listeners.
Optional CoreExec gRPC TLS/mTLS parameters are passed to the
core and edge roles through the same TELESRV_CORE_EXEC_GRPC_TLS_* variables used
in production. -GenerateCoreExecGRPCTestCerts creates a temporary CA, server
certificate, and client certificate for local mTLS smoke runs. With
-RunCoreExecUnavailableGate it first starts an edge pointed only at an
unavailable CoreExec target and verifies the edge exits without opening its
MTProto listener. With -InjectBadCoreExecTarget it appends one intentionally
unstarted CoreExec gRPC endpoint to the Edge target list, proving the static
resolver plus health checking can tolerate a bad endpoint during startup.
-RunCoreExecReadinessFlapGate runs the CoreExec package-level readiness flap
gate before role startup, proving that a static multi-target client converges
away from a NOT_SERVING Core and uses it again after it returns to SERVING.
-RunCoreExecDNSResolverGate runs the CoreExec package-level DNS resolver gates
before role startup, proving that the gRPC built-in DNS resolver can reach a
loopback host:port target and that DNS multi-A records can fan dispatch across
two CoreExec servers.
-RunCoreExecDNSMultiAProcessGate starts a local DNS authority before the real
role processes, points Edge at a generated dns://authority/name:port target,
and enables a process probe that must observe all Core instance IDs through DNS.
-RunCoreExecCrossCoreAuthGate runs the CoreExec package-level authenticated
multi-Core gate before role startup, proving that one auth key/session can sign
up through a static target list and then run users.getUsers on both CoreExec
servers without Core stickiness.
-RunCoreExecNoImplicitRetryGate runs the CoreExec package-level transport
semantic gate before role startup, proving that DispatchAdmitted does not gain
an implicit gRPC retry policy even if a retryPolicy service config is present.
-RunCoreExecFailureClassificationGate runs the CoreExec package-level
observability gate before role startup, proving that transport, protocol,
business RPC, and response-level control failures produce distinct fixed
outcomes.
-RunCoreExecProcessProbeGate runs a real CoreExec gRPC probe after role startup;
with -CoreExecProcessProbeExpectInstances it fails unless GetInfo observes the
requested number of distinct Core instance IDs through the configured resolver.
Use -CoreExecProcessProbeDuration and -CoreExecProcessProbeInterval to keep
dispatching over a sustained observation window instead of accepting one short
burst.
-RunEgressDeliveryProcessProbeGate runs a real Egress Delivery gRPC probe after role
startup; with -EgressDeliveryProcessProbeExpectInstances it fails unless GetInfo
observes the requested number of distinct Egress instance IDs through the
configured resolver.
-RunEgressOutboxCrashRecoveryGate runs a real PostgreSQL durable outbox gate
before role startup, proving stale dispatching leases are reclaimed and stale
late client ACK attempts are fenced without timer-dependent sleeps.
-RunEgressOutboxMultiProcessGate runs a real multi-process Durable Egress gate
after role startup. It creates probe users, registers a fake online Edge in
Redis, appends transactional durable outbox events, requires multiple real
telesrv-egress SourceInstanceID values to push through Redis fabric, reports
client ACKs through Egress Delivery gRPC, and verifies dispatch_outbox drains.
-RunEgressOutboxProcessCrashGate runs a real Durable Egress crash/reclaim gate
after role startup. It withholds the first Redis fabric service ACK, waits until
the row remains dispatching, stops the source
telesrv-egress process, then requires another Egress process to reclaim the same
outbox row with attempt+1 and drain it through a strict physical-write receipt.
-RunCoreExecPendingMetricsGate scrapes each Edge /metrics endpoint and verifies
the CoreExec pending admission gauges are exported with fixed transport-only
labels.
-RunCoreExecRollingRestartGate runs a process-level gate after the multi-instance
roles start: it probes CoreExec dispatch through the shared target list, stops
one Core process, proves dispatch still succeeds, restarts it, then stops the
other Core process to prove the restarted Core can serve traffic.
-RunMTProtoEdgeProbeGate uses a gotd telegram.Client to call help.getConfig
through every Edge listener after startup. In multi-instance rolling restart
mode it also probes through the Edge listeners while one Core process is down,
covering the real MTProto TCP -> Edge -> CoreExec -> Core -> Edge response path.
-RunMTProtoEdgeAuthProbeGate signs up two development-code accounts through
two Edge listeners, sends private messages both directions, and verifies
messages.getHistory from both clients. It also verifies offline
updates.getDifference by disconnecting Bob, sending a private message while he
is offline, reconnecting with the same auth/session storage, and reading the
message from the old pts/date/qts baseline. In multi-instance rolling restart
mode it repeats the authenticated probe while one Core process is down. Use
-MTProtoEdgeAuthProbeRuns to repeat independent account/session cycles in each
auth gate invocation.
-CoreExecGRPCResolver selects the Edge resolver provider; static accepts a
comma-separated target list, while dns accepts exactly one host:port target or
one explicit dns:// authority URI. It can also run the Redis fabric integration
gate before starting role processes,
using the same Redis address that the smoke runtime will use. This verifies
location registry, cross-Edge outbox/session-control pub-sub ACKs, auth
invalidation pub/sub, and callback/login-token short-state CAS.
-RunLogSafetyGate scans the role stderr logs after a successful smoke and fails
on unexpected RPC/protocol/internal-error terms. -RunPortCleanupGate verifies
that the CoreExec and Edge listen ports are free after cleanup.
-PreflightOnly runs only selected package/Redis/PostgreSQL preflight gates and exits
without building or starting Core/Egress/Edge role processes. It is limited to
RunRedisFabricGate, RunEgressOutboxCrashRecoveryGate, and CoreExec package-level
gates that do not require role processes.
It expects the real local PostgreSQL and Redis dependencies to be reachable through
the normal TELESRV_* configuration or the optional parameters below.
#>
[CmdletBinding()]
param(
    [string]$CoreExecGRPCAddr = "127.0.0.1:2440",
    [ValidateSet("static", "dns")]
    [string]$CoreExecGRPCResolver = "static",
    [string]$CoreExecGRPCTargets,
    [string[]]$CoreExecGRPCAddrs,
    [switch]$RunCoreExecUnavailableGate,
    [switch]$InjectBadCoreExecTarget,
    [switch]$RunCoreExecReadinessFlapGate,
    [switch]$RunCoreExecDNSResolverGate,
    [switch]$RunCoreExecDNSMultiAProcessGate,
    [string]$CoreExecDNSMultiAName = "coreexec.test.",
    [switch]$RunCoreExecCrossCoreAuthGate,
    [switch]$RunCoreExecNoImplicitRetryGate,
    [switch]$RunCoreExecFailureClassificationGate,
    [switch]$RunCoreExecProcessProbeGate,
    [int]$CoreExecProcessProbeExpectInstances = 0,
    [string]$CoreExecProcessProbeDuration = "0s",
    [string]$CoreExecProcessProbeInterval = "0s",
    [switch]$RunCoreExecPendingMetricsGate,
    [switch]$RunCoreExecRollingRestartGate,
    [switch]$RunMTProtoEdgeProbeGate,
    [int]$MTProtoEdgeProbeCount = 2,
    [switch]$MTProtoEdgeProbeObfuscated,
    [switch]$RunMTProtoEdgeAuthProbeGate,
    [string]$MTProtoEdgeAuthPhonePrefix = "+15559",
    [string]$MTProtoEdgeAuthCode = "12345",
    [int]$MTProtoEdgeAuthProbeRuns = 1,
    [string]$BadCoreExecGRPCAddr = "127.0.0.1:2449",
    [string]$CoreExecToken = "edge-core-smoke",
    [string]$CoreExecGRPCTLSServerCertFile,
    [string]$CoreExecGRPCTLSServerKeyFile,
    [string]$CoreExecGRPCTLSClientCAFile,
    [string]$CoreExecGRPCTLSCAFile,
    [string]$CoreExecGRPCTLSServerName,
    [string]$CoreExecGRPCTLSClientCertFile,
    [string]$CoreExecGRPCTLSClientKeyFile,
    [switch]$GenerateCoreExecGRPCTestCerts,
    [string]$EgressDeliveryGRPCAddr = "127.0.0.1:2510",
    [ValidateSet("static", "dns")]
    [string]$EgressDeliveryGRPCResolver = "static",
    [string]$EgressDeliveryGRPCTargets,
    [string[]]$EgressDeliveryGRPCAddrs,
    [switch]$RunEgressDeliveryProcessProbeGate,
    [int]$EgressDeliveryProcessProbeExpectInstances = 0,
    [string]$EgressDeliveryProcessProbeDuration = "0s",
    [string]$EgressDeliveryProcessProbeInterval = "0s",
    [switch]$RunEgressDeliveryRollingRestartGate,
    [switch]$RunEgressOutboxCrashRecoveryGate,
    [string]$EgressOutboxCrashRecoveryLeaseTimeout = "1s",
    [string]$EgressOutboxCrashRecoveryStaleAge = "2s",
    [switch]$RunEgressOutboxMultiProcessGate,
    [int]$EgressOutboxMultiProcessUsers = 16,
    [int]$EgressOutboxMultiProcessEventsPerUser = 2,
    [int]$EgressOutboxMultiProcessExpectInstances = 0,
    [string]$EgressOutboxMultiProcessAdmissionDelay = "50ms",
    [string]$EgressOutboxMultiProcessClientAckDelay = "25ms",
    [string]$EgressOutboxMultiProcessTimeout = "30s",
    [switch]$RunEgressOutboxProcessCrashGate,
    [string]$EgressOutboxProcessCrashLeaseTimeout = "1s",
    [string]$EgressOutboxProcessCrashAdmissionDelay = "0s",
    [string]$EgressOutboxProcessCrashClientAckDelay = "25ms",
    [string]$EgressOutboxProcessCrashTimeout = "45s",
    [string]$EgressDeliveryToken = "edge-core-smoke-egress",
    [string]$CoreListen = "127.0.0.1:2397",
    [string]$EdgeListen = "127.0.0.1:2398",
    [string[]]$EdgeListens,
    [string]$AdvertiseIP = "127.0.0.1",
    [string]$PostgresDSN,
    [string]$RedisAddr,
    [switch]$RunRedisFabricGate,
    [string]$RSAKeyPath,
    [string]$ExePath,
    [string]$CoreExePath,
    [string]$EgressExePath,
    [string]$WorkDir,
    [string]$LogDir,
    [int]$StartupTimeoutSeconds = 90,
    [int]$Tail = 120,
    [switch]$SkipBuild,
    [switch]$BuildOnly,
    [switch]$MultiInstance,
    [switch]$RunLogSafetyGate,
    [switch]$RunPortCleanupGate,
    [switch]$PreflightOnly,
    [switch]$KeepRunning
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$CoreExecTransport = "grpc"
$CoreExecGRPCTargetsExplicit = $PSBoundParameters.ContainsKey("CoreExecGRPCTargets")

$RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
if (-not $WorkDir) {
    $WorkDir = $RepoRoot
}
if (-not $ExePath) {
    $ExePath = Join-Path $RepoRoot "tmp\edge-core-smoke\telesrv-edge.exe"
}
if (-not $CoreExePath) {
    $CoreExePath = Join-Path $RepoRoot "tmp\edge-core-smoke\telesrv-core.exe"
}
if (-not $LogDir) {
    $LogDir = Join-Path $RepoRoot "tmp\edge-core-smoke\logs"
}
if ($RunCoreExecDNSMultiAProcessGate -and -not $MultiInstance) {
    throw "-RunCoreExecDNSMultiAProcessGate requires -MultiInstance"
}
if ($RunCoreExecDNSMultiAProcessGate -and $CoreExecGRPCTargetsExplicit) {
    throw "-RunCoreExecDNSMultiAProcessGate generates -CoreExecGRPCTargets automatically; do not pass both"
}
if ($RunCoreExecDNSMultiAProcessGate -and $InjectBadCoreExecTarget) {
    throw "-RunCoreExecDNSMultiAProcessGate cannot be combined with -InjectBadCoreExecTarget; DNS targets must come only from A records"
}
if ($CoreExecProcessProbeExpectInstances -lt 0) {
    throw "-CoreExecProcessProbeExpectInstances must be non-negative"
}
if ([string]::IsNullOrWhiteSpace($CoreExecProcessProbeDuration)) {
    throw "-CoreExecProcessProbeDuration must not be empty"
}
if ([string]::IsNullOrWhiteSpace($CoreExecProcessProbeInterval)) {
    throw "-CoreExecProcessProbeInterval must not be empty"
}
if ($RunCoreExecRollingRestartGate -and -not $MultiInstance) {
    throw "-RunCoreExecRollingRestartGate requires -MultiInstance"
}
if ($RunCoreExecDNSMultiAProcessGate -and $RunCoreExecRollingRestartGate) {
    throw "-RunCoreExecDNSMultiAProcessGate currently cannot combine with -RunCoreExecRollingRestartGate because its Core listeners share one service port across loopback IPs"
}
if ($null -eq $CoreExecGRPCAddrs -or $CoreExecGRPCAddrs.Count -eq 0) {
    if ($MultiInstance) {
        if ($RunCoreExecDNSMultiAProcessGate) {
            $CoreExecGRPCAddrs = @("127.0.0.1:2462", "127.0.0.2:2462")
        } else {
            $CoreExecGRPCAddrs = @("127.0.0.1:2440", "127.0.0.1:2441")
        }
    } else {
        $CoreExecGRPCAddrs = @($CoreExecGRPCAddr)
    }
}
if ($null -eq $EdgeListens -or $EdgeListens.Count -eq 0) {
    if ($MultiInstance) {
        $EdgeListens = @("127.0.0.1:2398", "127.0.0.1:2399")
    } else {
        $EdgeListens = @($EdgeListen)
    }
}
if ($null -eq $EgressDeliveryGRPCAddrs -or $EgressDeliveryGRPCAddrs.Count -eq 0) {
    if ($MultiInstance) {
        $EgressDeliveryGRPCAddrs = @("127.0.0.1:2510", "127.0.0.1:2511")
    } else {
        $EgressDeliveryGRPCAddrs = @($EgressDeliveryGRPCAddr)
    }
}
if ($MultiInstance) {
    $CoreExecGRPCAddr = $CoreExecGRPCAddrs[0]
    $EdgeListen = $EdgeListens[0]
    $EgressDeliveryGRPCAddr = $EgressDeliveryGRPCAddrs[0]
}
if ($RunCoreExecDNSMultiAProcessGate) {
    if ($CoreExecGRPCAddrs.Count -lt 2) {
        throw "-RunCoreExecDNSMultiAProcessGate requires at least two -CoreExecGRPCAddrs"
    }
    $servicePort = ""
    foreach ($addr in $CoreExecGRPCAddrs) {
        if ($addr -notmatch '^([^:\[\]]+):(\d+)$') {
            throw "-RunCoreExecDNSMultiAProcessGate requires IPv4 host:port CoreExec addrs; got $addr"
        }
        $hostText = $Matches[1]
        $portText = $Matches[2]
        $parsedIP = [System.Net.IPAddress]::None
        if (-not [System.Net.IPAddress]::TryParse($hostText, [ref]$parsedIP) -or
            $parsedIP.AddressFamily -ne [System.Net.Sockets.AddressFamily]::InterNetwork) {
            throw "-RunCoreExecDNSMultiAProcessGate requires IPv4 A-record addresses; got $addr"
        }
        if ($servicePort -eq "") {
            $servicePort = $portText
        } elseif ($servicePort -ne $portText) {
            throw "-RunCoreExecDNSMultiAProcessGate requires all CoreExec addrs to share one service port; got $($CoreExecGRPCAddrs -join ',')"
        }
    }
    $CoreExecGRPCResolver = "dns"
    $RunCoreExecProcessProbeGate = $true
    if ($CoreExecProcessProbeExpectInstances -eq 0) {
        $CoreExecProcessProbeExpectInstances = $CoreExecGRPCAddrs.Count
    }
}
if (-not $CoreExecGRPCTargets) {
    $CoreExecGRPCTargets = ($CoreExecGRPCAddrs -join ",")
}
if (-not $EgressDeliveryGRPCTargets) {
    $EgressDeliveryGRPCTargets = ($EgressDeliveryGRPCAddrs -join ",")
}
if ($InjectBadCoreExecTarget) {
    $badTarget = $BadCoreExecGRPCAddr.Trim()
    if ($badTarget.Length -eq 0) {
        throw "-BadCoreExecGRPCAddr must not be empty when -InjectBadCoreExecTarget is set"
    }
    $BadCoreExecGRPCAddr = $badTarget
    $existingTargets = @($CoreExecGRPCTargets.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })
    if ($existingTargets -notcontains $badTarget) {
        $existingTargets += $badTarget
    }
    $CoreExecGRPCTargets = ($existingTargets -join ",")
}
if ($CoreExecGRPCResolver -eq "dns" -and -not $RunCoreExecDNSMultiAProcessGate) {
    $dnsTargets = @($CoreExecGRPCTargets.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })
    if ($dnsTargets.Count -ne 1) {
        throw "-CoreExecGRPCResolver dns requires exactly one -CoreExecGRPCTargets value; use static for comma-separated endpoints"
    }
}
if ($EgressDeliveryGRPCResolver -eq "dns") {
    $dnsTargets = @($EgressDeliveryGRPCTargets.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })
    if ($dnsTargets.Count -ne 1) {
        throw "-EgressDeliveryGRPCResolver dns requires exactly one -EgressDeliveryGRPCTargets value; use static for comma-separated endpoints"
    }
}
if ($GenerateCoreExecGRPCTestCerts) {
    $manualTLSFiles = $CoreExecGRPCTLSServerCertFile -or
        $CoreExecGRPCTLSServerKeyFile -or
        $CoreExecGRPCTLSClientCAFile -or
        $CoreExecGRPCTLSCAFile -or
        $CoreExecGRPCTLSClientCertFile -or
        $CoreExecGRPCTLSClientKeyFile
    if ($manualTLSFiles) {
        throw "-GenerateCoreExecGRPCTestCerts cannot be combined with explicit CoreExec gRPC TLS certificate file parameters"
    }
}
if (($CoreExecGRPCTLSServerCertFile -and -not $CoreExecGRPCTLSServerKeyFile) -or
    ($CoreExecGRPCTLSServerKeyFile -and -not $CoreExecGRPCTLSServerCertFile)) {
    throw "-CoreExecGRPCTLSServerCertFile and -CoreExecGRPCTLSServerKeyFile must be configured together"
}
if ($CoreExecGRPCTLSClientCAFile -and (-not $CoreExecGRPCTLSServerCertFile -or -not $CoreExecGRPCTLSServerKeyFile)) {
    throw "-CoreExecGRPCTLSClientCAFile requires server cert/key TLS options"
}
if (($CoreExecGRPCTLSClientCertFile -and -not $CoreExecGRPCTLSClientKeyFile) -or
    ($CoreExecGRPCTLSClientKeyFile -and -not $CoreExecGRPCTLSClientCertFile)) {
    throw "-CoreExecGRPCTLSClientCertFile and -CoreExecGRPCTLSClientKeyFile must be configured together"
}
$coreExecGRPCServerTLSEnabled = [bool]($GenerateCoreExecGRPCTestCerts -or $CoreExecGRPCTLSServerCertFile -or $CoreExecGRPCTLSServerKeyFile -or $CoreExecGRPCTLSClientCAFile)
$coreExecGRPCClientTLSEnabled = [bool]($GenerateCoreExecGRPCTestCerts -or $CoreExecGRPCTLSCAFile -or $CoreExecGRPCTLSServerName -or $CoreExecGRPCTLSClientCertFile -or $CoreExecGRPCTLSClientKeyFile)
if ($coreExecGRPCServerTLSEnabled -and -not $coreExecGRPCClientTLSEnabled) {
    throw "CoreExec gRPC server TLS requires at least one Edge client TLS option, such as -CoreExecGRPCTLSCAFile or -CoreExecGRPCTLSServerName"
}
if ($coreExecGRPCClientTLSEnabled -and -not $coreExecGRPCServerTLSEnabled) {
    throw "CoreExec gRPC Edge client TLS options require server cert/key TLS options in this smoke script"
}
if ($CoreExecGRPCTLSClientCAFile -and (-not $CoreExecGRPCTLSClientCertFile -or -not $CoreExecGRPCTLSClientKeyFile)) {
    throw "-CoreExecGRPCTLSClientCAFile enables mTLS and requires edge client cert/key TLS options"
}
if ($RunRedisFabricGate -and $BuildOnly) {
    throw "-RunRedisFabricGate cannot be combined with -BuildOnly"
}
if ($RunCoreExecReadinessFlapGate -and $BuildOnly) {
    throw "-RunCoreExecReadinessFlapGate cannot be combined with -BuildOnly"
}
if ($RunCoreExecDNSResolverGate -and $BuildOnly) {
    throw "-RunCoreExecDNSResolverGate cannot be combined with -BuildOnly"
}
if ($RunCoreExecDNSMultiAProcessGate -and $BuildOnly) {
    throw "-RunCoreExecDNSMultiAProcessGate cannot be combined with -BuildOnly"
}
if ($RunCoreExecCrossCoreAuthGate -and $BuildOnly) {
    throw "-RunCoreExecCrossCoreAuthGate cannot be combined with -BuildOnly"
}
if ($RunCoreExecNoImplicitRetryGate -and $BuildOnly) {
    throw "-RunCoreExecNoImplicitRetryGate cannot be combined with -BuildOnly"
}
if ($RunCoreExecFailureClassificationGate -and $BuildOnly) {
    throw "-RunCoreExecFailureClassificationGate cannot be combined with -BuildOnly"
}
if ($RunCoreExecProcessProbeGate -and $BuildOnly) {
    throw "-RunCoreExecProcessProbeGate cannot be combined with -BuildOnly"
}
if ($RunCoreExecPendingMetricsGate -and $BuildOnly) {
    throw "-RunCoreExecPendingMetricsGate cannot be combined with -BuildOnly"
}
if ($RunCoreExecRollingRestartGate -and $BuildOnly) {
    throw "-RunCoreExecRollingRestartGate cannot be combined with -BuildOnly"
}
if ($RunEgressDeliveryProcessProbeGate -and $BuildOnly) {
    throw "-RunEgressDeliveryProcessProbeGate cannot be combined with -BuildOnly"
}
if ($RunEgressDeliveryRollingRestartGate -and $BuildOnly) {
    throw "-RunEgressDeliveryRollingRestartGate cannot be combined with -BuildOnly"
}
if ($RunEgressOutboxCrashRecoveryGate -and $BuildOnly) {
    throw "-RunEgressOutboxCrashRecoveryGate cannot be combined with -BuildOnly"
}
if ($RunEgressOutboxMultiProcessGate -and $BuildOnly) {
    throw "-RunEgressOutboxMultiProcessGate cannot be combined with -BuildOnly"
}
if ($RunEgressOutboxProcessCrashGate -and $BuildOnly) {
    throw "-RunEgressOutboxProcessCrashGate cannot be combined with -BuildOnly"
}
if ($RunMTProtoEdgeProbeGate -and $BuildOnly) {
    throw "-RunMTProtoEdgeProbeGate cannot be combined with -BuildOnly"
}
if ($RunMTProtoEdgeAuthProbeGate -and $BuildOnly) {
    throw "-RunMTProtoEdgeAuthProbeGate cannot be combined with -BuildOnly"
}
if ($PreflightOnly -and $BuildOnly) {
    throw "-PreflightOnly cannot be combined with -BuildOnly"
}
if ($PreflightOnly -and $KeepRunning) {
    throw "-PreflightOnly cannot be combined with -KeepRunning"
}
if ($PreflightOnly -and ($RunCoreExecUnavailableGate -or
        $RunCoreExecDNSMultiAProcessGate -or
        $RunCoreExecProcessProbeGate -or
        $RunCoreExecPendingMetricsGate -or
        $RunCoreExecRollingRestartGate -or
        $RunEgressDeliveryProcessProbeGate -or
        $RunEgressDeliveryRollingRestartGate -or
        $RunEgressOutboxMultiProcessGate -or
        $RunEgressOutboxProcessCrashGate -or
        $RunMTProtoEdgeProbeGate -or
        $RunMTProtoEdgeAuthProbeGate -or
        $InjectBadCoreExecTarget -or
        $RunLogSafetyGate -or
        $RunPortCleanupGate -or
        $GenerateCoreExecGRPCTestCerts)) {
    throw "-PreflightOnly only supports package/Redis/PostgreSQL preflight gates; remove process, MTProto, cert, log, port, and bad-target gates"
}
if ($PreflightOnly -and -not ($RunRedisFabricGate -or
        $RunCoreExecReadinessFlapGate -or
        $RunCoreExecDNSResolverGate -or
        $RunCoreExecCrossCoreAuthGate -or
        $RunCoreExecNoImplicitRetryGate -or
        $RunCoreExecFailureClassificationGate -or
        $RunEgressOutboxCrashRecoveryGate)) {
    throw "-PreflightOnly requires at least one package/Redis/PostgreSQL preflight gate"
}
if ($MTProtoEdgeProbeCount -le 0) {
    throw "-MTProtoEdgeProbeCount must be positive"
}
if ($RunMTProtoEdgeAuthProbeGate -and -not $MTProtoEdgeAuthCode) {
    throw "-MTProtoEdgeAuthCode must not be empty"
}
if ($MTProtoEdgeAuthProbeRuns -le 0) {
    throw "-MTProtoEdgeAuthProbeRuns must be positive"
}
if ($EgressDeliveryProcessProbeExpectInstances -lt 0) {
    throw "-EgressDeliveryProcessProbeExpectInstances must be non-negative"
}
if ([string]::IsNullOrWhiteSpace($EgressDeliveryProcessProbeDuration)) {
    throw "-EgressDeliveryProcessProbeDuration must not be empty"
}
if ([string]::IsNullOrWhiteSpace($EgressDeliveryProcessProbeInterval)) {
    throw "-EgressDeliveryProcessProbeInterval must not be empty"
}
if ($RunEgressDeliveryRollingRestartGate -and -not $MultiInstance) {
    throw "-RunEgressDeliveryRollingRestartGate requires -MultiInstance"
}
if ($RunEgressDeliveryRollingRestartGate -and $EgressDeliveryGRPCAddrs.Count -lt 2) {
    throw "-RunEgressDeliveryRollingRestartGate requires at least two -EgressDeliveryGRPCAddrs"
}
if ($RunEgressDeliveryRollingRestartGate -and $EgressDeliveryGRPCResolver -ne "static") {
    throw "-RunEgressDeliveryRollingRestartGate currently requires -EgressDeliveryGRPCResolver static"
}
if ($RunEgressDeliveryRollingRestartGate) {
    $RunEgressDeliveryProcessProbeGate = $true
}
if ($RunEgressDeliveryProcessProbeGate -and $MultiInstance -and $EgressDeliveryProcessProbeExpectInstances -eq 0) {
    $EgressDeliveryProcessProbeExpectInstances = $EgressDeliveryGRPCAddrs.Count
}
if ([string]::IsNullOrWhiteSpace($EgressOutboxCrashRecoveryLeaseTimeout)) {
    throw "-EgressOutboxCrashRecoveryLeaseTimeout must not be empty"
}
if ([string]::IsNullOrWhiteSpace($EgressOutboxCrashRecoveryStaleAge)) {
    throw "-EgressOutboxCrashRecoveryStaleAge must not be empty"
}
if ($RunEgressOutboxMultiProcessGate -and -not $MultiInstance) {
    throw "-RunEgressOutboxMultiProcessGate requires -MultiInstance"
}
if ($RunEgressOutboxMultiProcessGate -and $EgressDeliveryGRPCAddrs.Count -lt 2) {
    throw "-RunEgressOutboxMultiProcessGate requires at least two -EgressDeliveryGRPCAddrs"
}
if ($EgressOutboxMultiProcessUsers -le 0) {
    throw "-EgressOutboxMultiProcessUsers must be positive"
}
if ($EgressOutboxMultiProcessEventsPerUser -le 0) {
    throw "-EgressOutboxMultiProcessEventsPerUser must be positive"
}
if ($EgressOutboxMultiProcessExpectInstances -lt 0) {
    throw "-EgressOutboxMultiProcessExpectInstances must be non-negative"
}
if ([string]::IsNullOrWhiteSpace($EgressOutboxMultiProcessAdmissionDelay)) {
    throw "-EgressOutboxMultiProcessAdmissionDelay must not be empty"
}
if ([string]::IsNullOrWhiteSpace($EgressOutboxMultiProcessClientAckDelay)) {
    throw "-EgressOutboxMultiProcessClientAckDelay must not be empty"
}
if ([string]::IsNullOrWhiteSpace($EgressOutboxMultiProcessTimeout)) {
    throw "-EgressOutboxMultiProcessTimeout must not be empty"
}
if ($RunEgressOutboxMultiProcessGate) {
    $RunEgressDeliveryProcessProbeGate = $true
    if ($EgressDeliveryProcessProbeExpectInstances -eq 0) {
        $EgressDeliveryProcessProbeExpectInstances = $EgressDeliveryGRPCAddrs.Count
    }
    if ($EgressOutboxMultiProcessExpectInstances -eq 0) {
        $EgressOutboxMultiProcessExpectInstances = $EgressDeliveryGRPCAddrs.Count
    }
    if ($EgressOutboxMultiProcessUsers -lt $EgressOutboxMultiProcessExpectInstances) {
        throw "-EgressOutboxMultiProcessUsers must be >= expected Egress instances"
    }
}
if ($RunEgressOutboxProcessCrashGate -and -not $MultiInstance) {
    throw "-RunEgressOutboxProcessCrashGate requires -MultiInstance"
}
if ($RunEgressOutboxProcessCrashGate -and $EgressDeliveryGRPCAddrs.Count -lt 2) {
    throw "-RunEgressOutboxProcessCrashGate requires at least two -EgressDeliveryGRPCAddrs"
}
if ([string]::IsNullOrWhiteSpace($EgressOutboxProcessCrashLeaseTimeout)) {
    throw "-EgressOutboxProcessCrashLeaseTimeout must not be empty"
}
if ([string]::IsNullOrWhiteSpace($EgressOutboxProcessCrashAdmissionDelay)) {
    throw "-EgressOutboxProcessCrashAdmissionDelay must not be empty"
}
if ([string]::IsNullOrWhiteSpace($EgressOutboxProcessCrashClientAckDelay)) {
    throw "-EgressOutboxProcessCrashClientAckDelay must not be empty"
}
if ([string]::IsNullOrWhiteSpace($EgressOutboxProcessCrashTimeout)) {
    throw "-EgressOutboxProcessCrashTimeout must not be empty"
}
if ($RunEgressOutboxProcessCrashGate) {
    $RunEgressDeliveryProcessProbeGate = $true
    if ($EgressDeliveryProcessProbeExpectInstances -eq 0) {
        $EgressDeliveryProcessProbeExpectInstances = $EgressDeliveryGRPCAddrs.Count
    }
}
if ([string]::IsNullOrWhiteSpace($EgressDeliveryToken)) {
    throw "-EgressDeliveryToken must not be empty"
}
if ($KeepRunning -and ($RunLogSafetyGate -or $RunPortCleanupGate)) {
    throw "-RunLogSafetyGate and -RunPortCleanupGate require the smoke helper to clean up processes; remove -KeepRunning"
}
if ($RunRedisFabricGate -and -not $RedisAddr) {
    throw "-RunRedisFabricGate requires -RedisAddr"
}

$WorkDir = [System.IO.Path]::GetFullPath($WorkDir)
$ExePath = [System.IO.Path]::GetFullPath($ExePath)
$CoreExePath = [System.IO.Path]::GetFullPath($CoreExePath)
if (-not $EgressExePath) {
    $EgressExePath = Join-Path $RepoRoot "tmp\edge-core-smoke\telesrv-egress.exe"
}
$EgressExePath = [System.IO.Path]::GetFullPath($EgressExePath)
$LogDir = [System.IO.Path]::GetFullPath($LogDir)
$BinDir = Split-Path -Parent $ExePath
$CoreBinDir = Split-Path -Parent $CoreExePath
$EgressBinDir = Split-Path -Parent $EgressExePath
$GeneratedCoreExecGRPCTestCertDir = ""
$CoreExecDNSMultiATarget = ""
$CoreExecDNSMultiARecords = ""
$CoreExecDNSMultiAAuthorityStdout = ""
$CoreExecDNSMultiAAuthorityStderr = ""
$SmokeSucceeded = $false
$script:SmokeLogPaths = @()
$script:SmokeCleanupPorts = @()
$StartedProcesses = New-Object System.Collections.Generic.List[object]

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

function Invoke-RedisFabricGate {
    param([string]$Address)
    if (-not $Address) {
        throw "-RunRedisFabricGate requires -RedisAddr"
    }
    $previous = [Environment]::GetEnvironmentVariable("TELESRV_TEST_REDIS_ADDR", "Process")
    [Environment]::SetEnvironmentVariable("TELESRV_TEST_REDIS_ADDR", $Address, "Process")
    try {
        Invoke-External "go" @(
            "test",
            "./internal/edgecontrol/redisbus",
            "-run",
            "TestRedisFabricRoutesOutboxAndSessionControlAcrossEdges",
            "-count=1",
            "-v"
        ) | Out-Null
        Invoke-External "go" @(
            "test",
            "./internal/store/redisstore",
            "-run",
            "TestRedis(AuthInvalidationBrokerCrossInstancePubSub|BotCallbackRegistryCrossInstanceCASAndPubSub|LoginTokenRegistryCrossInstanceCAS|RateLimiterWindow)",
            "-count=1",
            "-v"
        ) | Out-Null
    } finally {
        [Environment]::SetEnvironmentVariable("TELESRV_TEST_REDIS_ADDR", $previous, "Process")
    }
    Write-Host "[ok] redis fabric gate passed: edgecontrol + auth/callback/login-token/rate-limit short state ($Address)"
}

function Invoke-CoreExecReadinessFlapGate {
    Invoke-External "go" @(
        "test",
        "./internal/coreexec",
        "-run",
        "TestGRPCRemoteStaticTargetsTracksReadinessFlaps",
        "-count=1",
        "-v"
    ) | Out-Null
    Write-Host "[ok] CoreExec readiness flap gate passed"
}

function Invoke-CoreExecDNSResolverGate {
    Invoke-External "go" @(
        "test",
        "./internal/coreexec",
        "-run",
        "TestGRPCRemoteDNSResolver(DispatchesThroughBuiltInResolver|RoundRobinAcrossARecords)",
        "-count=1",
        "-v"
    ) | Out-Null
    Write-Host "[ok] CoreExec DNS resolver gate passed"
}

function Invoke-CoreExecCrossCoreAuthGate {
    Invoke-External "go" @(
        "test",
        "./internal/coreexec",
        "-run",
        "TestGRPCRemoteStaticTargets(ShareAuthenticatedSessionAcrossCores|CommitPostResponseActionsAcrossCores)",
        "-count=1",
        "-v"
    ) | Out-Null
    Write-Host "[ok] CoreExec cross-Core authenticated session + typed post-response action gate passed"
}

function Invoke-CoreExecNoImplicitRetryGate {
    Invoke-External "go" @(
        "test",
        "./internal/coreexec",
        "-run",
        "TestGRPCRemoteDoesNotImplicitlyRetryDispatchAdmittedUnavailable",
        "-count=1",
        "-v"
    ) | Out-Null
    Write-Host "[ok] CoreExec no-implicit-retry gate passed"
}

function Invoke-CoreExecFailureClassificationGate {
    Invoke-External "go" @(
        "test",
        "./internal/coreexec",
        "-run",
        "TestGRPC(RemoteClassifiesStatusErrors|RemoteDoesNotImplicitlyRetryDispatchAdmittedUnavailable|MetricsUnaryServerInterceptorClassifiesResponseErrors)",
        "-count=1",
        "-v"
    ) | Out-Null
    Write-Host "[ok] CoreExec failure classification gate passed"
}

function Invoke-EgressOutboxCrashRecoveryGate {
    $args = @(
        "run",
        ".\scripts\egress-outbox-crash-smoke-probe",
        "-lease-timeout",
        $EgressOutboxCrashRecoveryLeaseTimeout,
        "-stale-age",
        $EgressOutboxCrashRecoveryStaleAge,
        "-timeout",
        "${StartupTimeoutSeconds}s"
    )
    if ($PostgresDSN) {
        $args += @("-postgres-dsn", $PostgresDSN)
    }
    $result = Invoke-External "go" $args
    $output = $result.Output.Trim()
    if ($output.Length -gt 0) {
        Write-Host "[ok] Egress outbox crash recovery gate: $output"
    } else {
        Write-Host "[ok] Egress outbox crash recovery gate passed"
    }
}

function Invoke-SelectedPreflightGates {
    if ($RunRedisFabricGate) {
        Write-Step "Redis fabric gate"
        Invoke-RedisFabricGate $RedisAddr
    }
    if ($RunCoreExecReadinessFlapGate) {
        Write-Step "CoreExec readiness flap gate"
        Invoke-CoreExecReadinessFlapGate
    }
    if ($RunCoreExecDNSResolverGate) {
        Write-Step "CoreExec DNS resolver gate"
        Invoke-CoreExecDNSResolverGate
    }
    if ($RunCoreExecCrossCoreAuthGate) {
        Write-Step "CoreExec cross-Core auth gate"
        Invoke-CoreExecCrossCoreAuthGate
    }
    if ($RunCoreExecNoImplicitRetryGate) {
        Write-Step "CoreExec no implicit retry gate"
        Invoke-CoreExecNoImplicitRetryGate
    }
    if ($RunCoreExecFailureClassificationGate) {
        Write-Step "CoreExec failure classification gate"
        Invoke-CoreExecFailureClassificationGate
    }
    if ($RunEgressOutboxCrashRecoveryGate) {
        Write-Step "Egress outbox crash recovery gate"
        Invoke-EgressOutboxCrashRecoveryGate
    }
}

function Invoke-CoreExecSmokeProbe {
    param(
        [string]$Name,
        [int]$Count = 8,
        [int]$ExpectInstances = 0
    )
    $args = @(
        "run",
        ".\scripts\coreexec-smoke-probe",
        "-targets",
        $CoreExecGRPCTargets,
        "-resolver",
        $CoreExecGRPCResolver,
        "-token",
        $CoreExecToken,
        "-count",
        [string]$Count,
        "-duration",
        $CoreExecProcessProbeDuration,
        "-interval",
        $CoreExecProcessProbeInterval,
        "-timeout",
        "${StartupTimeoutSeconds}s"
    )
    if ($ExpectInstances -gt 0) {
        $args += @("-expect-instances", [string]$ExpectInstances)
    }
    if ($CoreExecGRPCTLSCAFile) {
        $args += @("-tls-ca-file", $CoreExecGRPCTLSCAFile)
    }
    if ($CoreExecGRPCTLSServerName) {
        $args += @("-tls-server-name", $CoreExecGRPCTLSServerName)
    }
    if ($CoreExecGRPCTLSClientCertFile) {
        $args += @("-tls-client-cert-file", $CoreExecGRPCTLSClientCertFile)
    }
    if ($CoreExecGRPCTLSClientKeyFile) {
        $args += @("-tls-client-key-file", $CoreExecGRPCTLSClientKeyFile)
    }
    $result = Invoke-External "go" $args
    $output = $result.Output.Trim()
    if ($output.Length -gt 0) {
        Write-Host "[ok] CoreExec probe ${Name}: $output"
    } else {
        Write-Host "[ok] CoreExec probe $Name passed"
    }
}

function Invoke-EgressDeliverySmokeProbe {
    param(
        [string]$Name,
        [int]$Count = 8,
        [int]$ExpectInstances = 0
    )
    $args = @(
        "run",
        ".\scripts\egress-delivery-smoke-probe",
        "-targets",
        $EgressDeliveryGRPCTargets,
        "-resolver",
        $EgressDeliveryGRPCResolver,
        "-token",
        $EgressDeliveryToken,
        "-count",
        [string]$Count,
        "-duration",
        $EgressDeliveryProcessProbeDuration,
        "-interval",
        $EgressDeliveryProcessProbeInterval,
        "-timeout",
        "${StartupTimeoutSeconds}s"
    )
    if ($ExpectInstances -gt 0) {
        $args += @("-expect-instances", [string]$ExpectInstances)
    }
    $result = Invoke-External "go" $args
    $output = $result.Output.Trim()
    if ($output.Length -gt 0) {
        Write-Host "[ok] Egress Delivery probe ${Name}: $output"
    } else {
        Write-Host "[ok] Egress Delivery probe $Name passed"
    }
}

function Invoke-EgressOutboxMultiProcessGate {
    $args = @(
        "run",
        ".\scripts\egress-outbox-multi-process-probe",
        "-egress-delivery-targets",
        $EgressDeliveryGRPCTargets,
        "-egress-delivery-resolver",
        $EgressDeliveryGRPCResolver,
        "-egress-delivery-token",
        $EgressDeliveryToken,
        "-users",
        [string]$EgressOutboxMultiProcessUsers,
        "-events-per-user",
        [string]$EgressOutboxMultiProcessEventsPerUser,
        "-expect-source-instances",
        [string]$EgressOutboxMultiProcessExpectInstances,
        "-admission-delay",
        $EgressOutboxMultiProcessAdmissionDelay,
        "-client-ack-delay",
        $EgressOutboxMultiProcessClientAckDelay,
        "-timeout",
        $EgressOutboxMultiProcessTimeout
    )
    if ($PostgresDSN) {
        $args += @("-postgres-dsn", $PostgresDSN)
    }
    if ($RedisAddr) {
        $args += @("-redis-addr", $RedisAddr)
    }
    $result = Invoke-External "go" $args
    $output = $result.Output.Trim()
    if ($output.Length -gt 0) {
        Write-Host "[ok] Egress outbox multi-process gate: $output"
    } else {
        Write-Host "[ok] Egress outbox multi-process gate passed"
    }
}

function Start-EgressOutboxProcessCrashProbe {
    param([string]$Stamp, [string]$SignalFile)
    $stdout = Join-Path $LogDir "egress-outbox-process-crash-$Stamp.out.log"
    $stderr = Join-Path $LogDir "egress-outbox-process-crash-$Stamp.err.log"
    Add-SmokeLogPath $stderr
    Remove-Item -LiteralPath $SignalFile -ErrorAction SilentlyContinue
    $args = @(
        "run",
        ".\scripts\egress-outbox-multi-process-probe",
        "-crash-recovery",
        "-crash-signal-file",
        $SignalFile,
        "-egress-delivery-targets",
        $EgressDeliveryGRPCTargets,
        "-egress-delivery-resolver",
        $EgressDeliveryGRPCResolver,
        "-egress-delivery-token",
        $EgressDeliveryToken,
        "-users",
        "1",
        "-events-per-user",
        "1",
        "-expect-source-instances",
        "2",
        "-admission-delay",
        $EgressOutboxProcessCrashAdmissionDelay,
        "-client-ack-delay",
        $EgressOutboxProcessCrashClientAckDelay,
        "-timeout",
        $EgressOutboxProcessCrashTimeout
    )
    if ($PostgresDSN) {
        $args += @("-postgres-dsn", $PostgresDSN)
    }
    if ($RedisAddr) {
        $args += @("-redis-addr", $RedisAddr)
    }
    $proc = Start-Process -FilePath "go" `
        -ArgumentList $args `
        -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -PassThru `
        -WindowStyle Hidden
    $StartedProcesses.Add($proc) | Out-Null
    Write-Host "[ok] started Egress outbox process crash probe PID $($proc.Id)"
    Write-Host "[ok] Egress outbox process crash probe stdout: $stdout"
    Write-Host "[ok] Egress outbox process crash probe stderr: $stderr"
    [pscustomobject]@{
        Process = $proc
        Stdout = $stdout
        Stderr = $stderr
        SignalFile = $SignalFile
    }
}

function Wait-EgressOutboxCrashSignal {
    param([pscustomobject]$Probe)
    $deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $Probe.Process.Refresh()
        if ($Probe.Process.HasExited) {
            $stdoutTail = Get-LogTail $Probe.Stdout $Tail
            $stderrTail = Get-LogTail $Probe.Stderr $Tail
            throw "Egress outbox process crash probe exited before signal with code $($Probe.Process.ExitCode):`nstdout:`n$stdoutTail`nstderr:`n$stderrTail"
        }
        if (Test-Path -LiteralPath $Probe.SignalFile -PathType Leaf) {
            $raw = (Get-Content -LiteralPath $Probe.SignalFile -Raw).Trim()
            if ($raw.Length -gt 0) {
                $signal = $raw | ConvertFrom-Json
                if ([string]::IsNullOrWhiteSpace([string]$signal.source_instance_id)) {
                    throw "Egress outbox process crash signal missing source_instance_id: $raw"
                }
                Write-Host "[ok] Egress outbox process crash signal: source=$($signal.source_instance_id) outbox=$($signal.outbox_id) pts=$($signal.pts) attempt=$($signal.attempt)"
                return $signal
            }
        }
        Start-Sleep -Milliseconds 100
    }
    $stdoutTail = Get-LogTail $Probe.Stdout $Tail
    $stderrTail = Get-LogTail $Probe.Stderr $Tail
    throw "Egress outbox process crash probe did not publish dispatching signal within ${StartupTimeoutSeconds}s:`nstdout:`n$stdoutTail`nstderr:`n$stderrTail"
}

function Wait-EgressOutboxCrashProbeExit {
    param([pscustomobject]$Probe)
    $deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $Probe.Process.Refresh()
        if ($Probe.Process.HasExited) {
            $stdout = Get-LogTail $Probe.Stdout $Tail
            $stderr = Get-LogTail $Probe.Stderr $Tail
            if ($Probe.Process.ExitCode -ne 0) {
                throw "Egress outbox process crash probe failed with code $($Probe.Process.ExitCode):`nstdout:`n$stdout`nstderr:`n$stderr"
            }
            return $stdout.Trim()
        }
        Start-Sleep -Milliseconds 100
    }
    $stdoutTail = Get-LogTail $Probe.Stdout $Tail
    $stderrTail = Get-LogTail $Probe.Stderr $Tail
    throw "Egress outbox process crash probe did not finish within ${StartupTimeoutSeconds}s after source Egress stop:`nstdout:`n$stdoutTail`nstderr:`n$stderrTail"
}

function Invoke-EgressOutboxProcessCrashGate {
    param(
        [System.Diagnostics.Process[]]$EgressProcesses,
        [string[]]$EgressInstanceIDs,
        [string[]]$EgressAddresses,
        [hashtable]$CommonEnv,
        [string]$Stamp
    )
    if ($EgressProcesses.Count -lt 2 -or $EgressInstanceIDs.Count -lt 2 -or $EgressAddresses.Count -lt 2) {
        throw "Egress outbox process crash gate requires at least two running Egress processes"
    }
    $coordDir = Join-Path $LogDir "egress-outbox-process-crash-$Stamp"
    New-Item -ItemType Directory -Force -Path $coordDir | Out-Null
    $signalFile = Join-Path $coordDir "awaiting-ack.json"
    $probe = Start-EgressOutboxProcessCrashProbe $Stamp $signalFile
    $signal = Wait-EgressOutboxCrashSignal $probe
    $source = [string]$signal.source_instance_id
    $index = -1
    for ($i = 0; $i -lt $EgressInstanceIDs.Count; $i++) {
        if ($EgressInstanceIDs[$i] -eq $source) {
            $index = $i
            break
        }
    }
    if ($index -lt 0) {
        throw "Egress outbox process crash gate saw source instance '$source' not in started Egress instances: $($EgressInstanceIDs -join ',')"
    }
    Stop-CoreSmokeRole $EgressProcesses[$index] "egress$index process crash gate"
    Wait-PortFree (Get-ListenPort $EgressAddresses[$index]) "egress$index process crash gate" (Get-Date).AddSeconds($StartupTimeoutSeconds)
    $output = Wait-EgressOutboxCrashProbeExit $probe
    if ($output.Length -gt 0) {
        Write-Host "[ok] Egress outbox process crash gate: $output"
    } else {
        Write-Host "[ok] Egress outbox process crash gate passed"
    }
    $restarted = Start-EgressSmokeRole $index $EgressAddresses[$index] $CommonEnv $Stamp "crash-restart"
    Invoke-EgressDeliverySmokeProbe "after outbox crash recovery restart" 8 $EgressDeliveryGRPCAddrs.Count
    [pscustomobject]@{
        Index = $index
        SourceInstanceID = $source
        Restarted = $restarted
    }
}

function Get-MTProtoProbeRSAKeyPath {
    $path = $RSAKeyPath
    if (-not $path) {
        $path = Join-Path $WorkDir "data\server_rsa.pem"
    }
    if (-not [System.IO.Path]::IsPathRooted($path)) {
        $path = Join-Path $WorkDir $path
    }
    $fullPath = [System.IO.Path]::GetFullPath($path)
    if (-not (Test-Path -LiteralPath $fullPath -PathType Leaf)) {
        throw "MTProto Edge probe RSA key file does not exist: $fullPath"
    }
    return $fullPath
}

function Invoke-MTProtoEdgeSmokeProbe {
    param(
        [string]$Name,
        [string[]]$Addresses,
        [int]$Count = 2
    )
    $rsaPath = Get-MTProtoProbeRSAKeyPath
    foreach ($addr in $Addresses) {
        $args = @(
            "run",
            ".\scripts\mtproto-edge-smoke-probe",
            "-server",
            $addr,
            "-dc",
            "2",
            "-rsa-key",
            $rsaPath,
            "-count",
            [string]$Count,
            "-timeout",
            "${StartupTimeoutSeconds}s"
        )
        if ($MTProtoEdgeProbeObfuscated) {
            $args += "-obfuscated"
        }
        $result = Invoke-External "go" $args
        $output = $result.Output.Trim()
        if ($output.Length -gt 0) {
            Write-Host "[ok] MTProto Edge probe ${Name} ${addr}: $output"
        } else {
            Write-Host "[ok] MTProto Edge probe $Name $addr passed"
        }
    }
}

function Invoke-MTProtoEdgeAuthSmokeProbe {
    param(
        [string]$Name,
        [string[]]$Addresses
    )
    if ($null -eq $Addresses -or $Addresses.Count -eq 0) {
        throw "MTProto Edge auth probe requires at least one Edge address"
    }
    $aliceAddress = $Addresses[0]
    $bobAddress = $Addresses[0]
    if ($Addresses.Count -gt 1) {
        $bobAddress = $Addresses[1]
    }
    $rsaPath = Get-MTProtoProbeRSAKeyPath
    $args = @(
        "run",
        ".\scripts\mtproto-edge-auth-smoke-probe",
        "-alice-server",
        $aliceAddress,
        "-bob-server",
        $bobAddress,
        "-dc",
        "2",
        "-rsa-key",
        $rsaPath,
        "-code",
        $MTProtoEdgeAuthCode,
        "-phone-prefix",
        $MTProtoEdgeAuthPhonePrefix,
        "-runs",
        $MTProtoEdgeAuthProbeRuns,
        "-timeout",
        "$($StartupTimeoutSeconds * $MTProtoEdgeAuthProbeRuns)s"
    )
    if ($MTProtoEdgeProbeObfuscated) {
        $args += "-obfuscated"
    }
    $result = Invoke-External "go" $args
    $output = $result.Output.Trim()
    if ($output.Length -gt 0) {
        Write-Host "[ok] MTProto Edge auth probe ${Name}: $output"
    } else {
        Write-Host "[ok] MTProto Edge auth probe $Name passed"
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

function Get-ListenHost {
    param([string]$Address)
    if ($Address -match '^\[(.+)\]:(\d+)$') {
        return $Matches[1].ToLowerInvariant()
    }
    if ($Address -match '^(.+):(\d+)$') {
        return $Matches[1].ToLowerInvariant()
    }
    throw "Cannot parse listen host from '$Address'"
}

function Test-ListenHostWildcard {
    param([string]$HostName)
    $hostText = $HostName.Trim().ToLowerInvariant()
    return $hostText -eq "" -or $hostText -eq "*" -or $hostText -eq "0.0.0.0" -or $hostText -eq "::"
}

function Test-ListenEndpointConflict {
    param([string]$Left, [string]$Right)
    $leftPort = Get-ListenPort $Left
    $rightPort = Get-ListenPort $Right
    if ($leftPort -ne $rightPort) {
        return $false
    }
    $leftHost = Get-ListenHost $Left
    $rightHost = Get-ListenHost $Right
    if ($leftHost -eq $rightHost) {
        return $true
    }
    return (Test-ListenHostWildcard $leftHost) -or (Test-ListenHostWildcard $rightHost)
}

function Assert-PortFree {
    param([int]$Port, [string]$Name)
    $listeners = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)
    if ($listeners.Count -eq 0) {
        return
    }
    $details = @()
    foreach ($ownerPid in @($listeners | Select-Object -ExpandProperty OwningProcess -Unique)) {
        $proc = Get-Process -Id $ownerPid -ErrorAction SilentlyContinue
        if ($proc) {
            $path = ""
            try {
                $path = $proc.Path
            } catch {
                $path = ""
            }
            $details += "PID $($proc.Id) $($proc.ProcessName) $path"
        } else {
            $details += "PID $ownerPid"
        }
    }
    throw "$Name port $Port is already listening: $($details -join '; ')"
}

function Assert-ListenAddressFree {
    param([string]$Address, [string]$Name)
    $port = Get-ListenPort $Address
    $hostName = Get-ListenHost $Address
    $listeners = @(Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
        Where-Object {
            $localAddress = ([string]$_.LocalAddress).ToLowerInvariant()
            (Test-ListenHostWildcard $hostName) -or
                (Test-ListenHostWildcard $localAddress) -or
                $localAddress -eq $hostName
        })
    if ($listeners.Count -eq 0) {
        return
    }
    $details = @()
    foreach ($ownerPid in @($listeners | Select-Object -ExpandProperty OwningProcess -Unique)) {
        $proc = Get-Process -Id $ownerPid -ErrorAction SilentlyContinue
        if ($proc) {
            $path = ""
            try {
                $path = $proc.Path
            } catch {
                $path = ""
            }
            $details += "PID $($proc.Id) $($proc.ProcessName) $path"
        } else {
            $details += "PID $ownerPid"
        }
    }
    throw "$Name listen address $Address is already listening: $($details -join '; ')"
}

function Wait-PortFree {
    param([int]$Port, [string]$Name, [datetime]$Deadline)
    while ((Get-Date) -lt $Deadline) {
        $listeners = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)
        if ($listeners.Count -eq 0) {
            return
        }
        Start-Sleep -Milliseconds 250
    }
    Assert-PortFree $Port $Name
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

function Add-CoreExecGRPCServerTLSEnv {
    param([hashtable]$Environment)
    if ($CoreExecGRPCTLSServerCertFile) {
        $Environment["TELESRV_CORE_EXEC_GRPC_TLS_CERT_FILE"] = $CoreExecGRPCTLSServerCertFile
    }
    if ($CoreExecGRPCTLSServerKeyFile) {
        $Environment["TELESRV_CORE_EXEC_GRPC_TLS_KEY_FILE"] = $CoreExecGRPCTLSServerKeyFile
    }
    if ($CoreExecGRPCTLSClientCAFile) {
        $Environment["TELESRV_CORE_EXEC_GRPC_TLS_CLIENT_CA_FILE"] = $CoreExecGRPCTLSClientCAFile
    }
}

function Add-CoreExecGRPCClientTLSEnv {
    param([hashtable]$Environment)
    if ($CoreExecGRPCTLSCAFile) {
        $Environment["TELESRV_CORE_EXEC_GRPC_TLS_CA_FILE"] = $CoreExecGRPCTLSCAFile
    }
    if ($CoreExecGRPCTLSServerName) {
        $Environment["TELESRV_CORE_EXEC_GRPC_TLS_SERVER_NAME"] = $CoreExecGRPCTLSServerName
    }
    if ($CoreExecGRPCTLSClientCertFile) {
        $Environment["TELESRV_CORE_EXEC_GRPC_TLS_CLIENT_CERT_FILE"] = $CoreExecGRPCTLSClientCertFile
    }
    if ($CoreExecGRPCTLSClientKeyFile) {
        $Environment["TELESRV_CORE_EXEC_GRPC_TLS_CLIENT_KEY_FILE"] = $CoreExecGRPCTLSClientKeyFile
    }
}

function Get-LogTail {
    param([string]$Path, [int]$LineCount)
    if (-not (Test-Path -LiteralPath $Path)) {
        return ""
    }
    return (Get-Content -LiteralPath $Path -Tail $LineCount -ErrorAction SilentlyContinue) -join "`n"
}

function Add-SmokeLogPath {
    param([string]$Path)
    if (-not $Path) {
        return
    }
    $fullPath = [System.IO.Path]::GetFullPath($Path)
    if (@($script:SmokeLogPaths) -notcontains $fullPath) {
        $script:SmokeLogPaths += $fullPath
    }
}

function Add-SmokeCleanupPort {
    param([int]$Port)
    if ($Port -le 0) {
        return
    }
    if (@($script:SmokeCleanupPorts) -notcontains $Port) {
        $script:SmokeCleanupPorts += $Port
    }
}

function New-LoopbackListenAddress {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Parse("127.0.0.1"), 0)
    try {
        $listener.Start()
        $port = ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
    } finally {
        $listener.Stop()
    }
    Add-SmokeCleanupPort $port
    return "127.0.0.1:$port"
}

function Assert-CoreExecPendingMetricsBody {
    param(
        [string]$Body,
        [string]$Transport,
        [string]$Name
    )
    $required = @(
        "telesrv_coreexec_pending_admissions",
        "telesrv_coreexec_pending_admission_capacity",
        "telesrv_coreexec_pending_admission_oldest_age_seconds",
        "telesrv_coreexec_pending_admission_ttl_seconds"
    )
    $lines = @($Body -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -like "telesrv_coreexec_pending_admission*" -or $_ -like "telesrv_coreexec_pending_admissions*" })
    foreach ($metric in $required) {
        $matches = @($lines | Where-Object { $_ -like "$metric*" -and $_ -like "*transport=`"$Transport`"*"})
        if ($matches.Count -ne 1) {
            $preview = ($lines | Select-Object -First 20) -join "`n"
            throw "$Name metrics missing $metric for transport=$Transport; got:`n$preview"
        }
    }
    $forbidden = '\b(endpoint|address|target|auth|auth_key|session|request|request_id|method)="'
    $bad = @($lines | Where-Object { $_ -match $forbidden })
    if ($bad.Count -gt 0) {
        throw "$Name CoreExec pending metrics contain forbidden high-cardinality labels:`n$($bad -join "`n")"
    }
}

function Invoke-CoreExecPendingMetricsGate {
    param(
        [string[]]$DebugAddrs,
        [string]$Transport,
        [string]$Name
    )
    if ($null -eq $DebugAddrs -or $DebugAddrs.Count -eq 0) {
        throw "$Name pending metrics gate requires at least one debug address"
    }
    foreach ($addr in $DebugAddrs) {
        $url = "http://$addr/metrics"
        $deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
        $lastError = ""
        $passed = $false
        while ((Get-Date) -lt $deadline) {
            try {
                $res = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2
                if ([int]$res.StatusCode -ge 200 -and [int]$res.StatusCode -lt 300) {
                    Assert-CoreExecPendingMetricsBody -Body $res.Content -Transport $Transport -Name "$Name $addr"
                    Write-Host "[ok] CoreExec pending metrics gate ${Name}: $url transport=$Transport"
                    $passed = $true
                    break
                }
                $lastError = "status $($res.StatusCode)"
            } catch {
                $lastError = $_.Exception.Message
            }
            Start-Sleep -Milliseconds 500
        }
        if (-not $passed) {
            throw "$Name pending metrics gate failed for ${url}: $lastError"
        }
    }
}

function Invoke-LogSafetyGate {
    param([string[]]$Paths)
    $pathsToScan = @($Paths | Where-Object { $_ } | Sort-Object -Unique)
    if ($pathsToScan.Count -eq 0) {
        Write-Host "[ok] log safety gate skipped: no role logs were recorded"
        return
    }
    $pattern = 'NOT_IMPLEMENTED|Unhandled RPC|bad_msg|\bpanic\b|\bfatal\b|RPC internal error|500 INTERNAL|code = Internal|DeadlineExceeded|ENCRYPTED_MESSAGE_INVALID|FLOOD_WAIT'
    $findings = @()
    foreach ($path in $pathsToScan) {
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
            continue
        }
        $findings += Select-String -LiteralPath $path -Pattern $pattern -CaseSensitive:$false | ForEach-Object {
            "{0}:{1}: {2}" -f $_.Path, $_.LineNumber, $_.Line.Trim()
        }
    }
    if ($findings.Count -gt 0) {
        $preview = ($findings | Select-Object -First 20) -join "`n"
        throw "log safety gate found unexpected dangerous terms:`n$preview"
    }
    Write-Host "[ok] log safety gate passed: scanned $($pathsToScan.Count) role logs"
}

function Invoke-PortCleanupGate {
    param([int[]]$Ports)
    $portsToCheck = @($Ports | Where-Object { $_ -gt 0 } | Sort-Object -Unique)
    if ($portsToCheck.Count -eq 0) {
        Write-Host "[ok] port cleanup gate skipped: no role ports were recorded"
        return
    }
    $deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
    foreach ($port in $portsToCheck) {
        Wait-PortFree $port "post-smoke cleanup gate" $deadline
    }
    Write-Host "[ok] port cleanup gate passed: $($portsToCheck -join ',')"
}

function Test-ProcessExited {
    param([System.Diagnostics.Process]$Process)
    $Process.Refresh()
    return $Process.HasExited
}

function Wait-ProcessStopped {
    param(
        [System.Diagnostics.Process]$Process,
        [string]$Name,
        [datetime]$Deadline
    )
    while ((Get-Date) -lt $Deadline) {
        $Process.Refresh()
        if ($Process.HasExited) {
            return
        }
        Start-Sleep -Milliseconds 250
    }
    throw "$Name PID $($Process.Id) did not stop within ${StartupTimeoutSeconds}s"
}

function Wait-LogContains {
    param(
        [System.Diagnostics.Process]$Process,
        [string]$Path,
        [string]$Pattern,
        [string]$Name,
        [datetime]$Deadline
    )
    while ((Get-Date) -lt $Deadline) {
        if (Test-ProcessExited $Process) {
            $tailText = Get-LogTail $Path $Tail
            throw "$Name exited during startup with code $($Process.ExitCode):`n$tailText"
        }
        $tailText = Get-LogTail $Path $Tail
        if ($tailText -match $Pattern) {
            return
        }
        Start-Sleep -Milliseconds 250
    }
    $lastTail = Get-LogTail $Path $Tail
    throw "$Name did not write expected log pattern '$Pattern' within ${StartupTimeoutSeconds}s:`n$lastTail"
}

function Wait-PortForProcess {
    param(
        [System.Diagnostics.Process]$Process,
        [int]$Port,
        [string]$Name,
        [string]$LogPath,
        [datetime]$Deadline
    )
    while ((Get-Date) -lt $Deadline) {
        if (Test-ProcessExited $Process) {
            $tailText = Get-LogTail $LogPath $Tail
            throw "$Name exited during startup with code $($Process.ExitCode):`n$tailText"
        }
        $listeners = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
            Where-Object { $_.OwningProcess -eq $Process.Id })
        if ($listeners.Count -gt 0) {
            return
        }
        Start-Sleep -Milliseconds 250
    }
    $tailText = Get-LogTail $LogPath $Tail
    throw "$Name PID $($Process.Id) did not listen on port $Port within ${StartupTimeoutSeconds}s:`n$tailText"
}

function Wait-ProcessExitBeforeListen {
    param(
        [System.Diagnostics.Process]$Process,
        [int]$Port,
        [string]$Name,
        [string]$LogPath,
        [datetime]$Deadline
    )
    while ((Get-Date) -lt $Deadline) {
        $listeners = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
            Where-Object { $_.OwningProcess -eq $Process.Id })
        if ($listeners.Count -gt 0) {
            $tailText = Get-LogTail $LogPath $Tail
            throw "$Name unexpectedly opened MTProto listener on port $Port before CoreExec became reachable:`n$tailText"
        }
        if (Test-ProcessExited $Process) {
            $tailText = Get-LogTail $LogPath $Tail
            if ($Process.ExitCode -eq 0) {
                throw "$Name exited successfully; expected fail-closed startup when CoreExec is unavailable:`n$tailText"
            }
            return
        }
        Start-Sleep -Milliseconds 250
    }
    $tailText = Get-LogTail $LogPath $Tail
    throw "$Name did not fail closed within ${StartupTimeoutSeconds}s while CoreExec was unavailable:`n$tailText"
}

function Copy-Hashtable {
    param([hashtable]$Source)
    $copy = @{}
    foreach ($item in $Source.GetEnumerator()) {
        $copy[$item.Key] = $item.Value
    }
    return $copy
}

function Start-TelesrvRole {
    param(
        [string]$RoleName,
        [hashtable]$Environment,
        [string]$StdoutPath,
        [string]$StderrPath,
        [string]$FilePath = $ExePath
    )
    $saved = @{}
    foreach ($key in $Environment.Keys) {
        $saved[$key] = [Environment]::GetEnvironmentVariable($key, "Process")
        [Environment]::SetEnvironmentVariable($key, [string]$Environment[$key], "Process")
    }
    try {
        $proc = Start-Process -FilePath $FilePath `
            -WorkingDirectory $WorkDir `
            -RedirectStandardOutput $StdoutPath `
            -RedirectStandardError $StderrPath `
            -PassThru `
            -WindowStyle Hidden
        $StartedProcesses.Add($proc) | Out-Null
        Write-Host "[ok] started $RoleName PID $($proc.Id)"
        Write-Host "[ok] $RoleName stdout: $StdoutPath"
        Write-Host "[ok] $RoleName stderr: $StderrPath"
        return $proc
    } finally {
        foreach ($key in $Environment.Keys) {
            [Environment]::SetEnvironmentVariable($key, $saved[$key], "Process")
        }
    }
}

function Start-CoreExecDNSMultiAAuthority {
    param([string]$Stamp)
    $servicePort = Get-ListenPort $CoreExecGRPCAddrs[0]
    $records = @()
    foreach ($addr in $CoreExecGRPCAddrs) {
        $records += Get-ListenHost $addr
    }
    $targetFile = Join-Path $LogDir "coreexec-dns-multia-$Stamp.target"
    Remove-Item -LiteralPath $targetFile -ErrorAction SilentlyContinue
    $stdout = Join-Path $LogDir "coreexec-dns-multia-$Stamp.out.log"
    $stderr = Join-Path $LogDir "coreexec-dns-multia-$Stamp.err.log"
    $args = @(
        "run",
        ".\scripts\coreexec-dns-authority",
        "-name",
        $CoreExecDNSMultiAName,
        "-a",
        ($records -join ","),
        "-service-port",
        [string]$servicePort,
        "-target-file",
        $targetFile
    )
    $proc = Start-Process -FilePath "go" `
        -ArgumentList $args `
        -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -PassThru `
        -WindowStyle Hidden
    $StartedProcesses.Add($proc) | Out-Null
    $deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $proc.Refresh()
        if ($proc.HasExited) {
            $tailText = Get-LogTail $stderr $Tail
            throw "CoreExec DNS multi-A authority exited during startup with code $($proc.ExitCode):`n$tailText"
        }
        if (Test-Path -LiteralPath $targetFile -PathType Leaf) {
            $target = (Get-Content -LiteralPath $targetFile -Raw).Trim()
            if ($target.Length -gt 0) {
                Write-Host "[ok] CoreExec DNS multi-A authority PID $($proc.Id)"
                Write-Host "[ok] CoreExec DNS multi-A records: $($records -join ',')"
                Write-Host "[ok] CoreExec DNS multi-A target: $target"
                Write-Host "[ok] CoreExec DNS multi-A stdout: $stdout"
                Write-Host "[ok] CoreExec DNS multi-A stderr: $stderr"
                return [pscustomobject]@{
                    Process = $proc
                    Target = $target
                    Records = $records
                    Stdout = $stdout
                    Stderr = $stderr
                }
            }
        }
        Start-Sleep -Milliseconds 250
    }
    $tailText = Get-LogTail $stderr $Tail
    throw "CoreExec DNS multi-A authority did not publish a target within ${StartupTimeoutSeconds}s:`n$tailText"
}

function Invoke-CoreExecUnavailableGate {
    param(
        [hashtable]$CommonEnv,
        [string]$Stamp
    )
    Write-Step "CoreExec unavailable gate"
    $edgePort = Get-ListenPort $EdgeListen
    $badPort = Get-ListenPort $BadCoreExecGRPCAddr
    Assert-PortFree $edgePort "Edge unavailable gate"
    Assert-PortFree $badPort "CoreExec unavailable gate target $BadCoreExecGRPCAddr"
    $stdout = Join-Path $LogDir "edge-unavailable-$Stamp.out.log"
    $stderr = Join-Path $LogDir "edge-unavailable-$Stamp.err.log"
    Add-SmokeLogPath $stderr
    $edgeEnv = Copy-Hashtable $CommonEnv
	$edgeEnv["TELESRV_LISTEN"] = $EdgeListen
    $edgeEnv["TELESRV_CORE_EXEC_GRPC_ADDR"] = ""
    $edgeEnv["TELESRV_CORE_EXEC_GRPC_TARGETS"] = $BadCoreExecGRPCAddr
    $edgeEnv["TELESRV_CORE_EXEC_GRPC_RESOLVER"] = $CoreExecGRPCResolver
    $edgeEnv["TELESRV_EGRESS_DELIVERY_GRPC_ADDR"] = ""
    $edgeEnv["TELESRV_EGRESS_DELIVERY_GRPC_TARGETS"] = $EgressDeliveryGRPCTargets
    $edgeEnv["TELESRV_EGRESS_DELIVERY_GRPC_RESOLVER"] = $EgressDeliveryGRPCResolver
    $edgeEnv["TELESRV_INSTANCE_ID"] = "smoke-edge-unavailable-$PID"
    Add-CoreExecGRPCClientTLSEnv $edgeEnv
    $proc = Start-TelesrvRole "edge-unavailable" $edgeEnv $stdout $stderr
    $deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
    Wait-ProcessExitBeforeListen $proc $edgePort "edge unavailable gate" $stderr $deadline
    Write-Host "[ok] edge failed closed before opening MTProto listener"
    Write-Host "[ok] unavailable target: $BadCoreExecGRPCAddr"
    Write-Host "[ok] unavailable gate stderr: $stderr"
}

function Stop-SmokeProcesses {
    for ($i = $StartedProcesses.Count - 1; $i -ge 0; $i--) {
        $proc = $StartedProcesses[$i]
        if (-not $proc) {
            continue
        }
        try {
            $proc.Refresh()
            if (-not $proc.HasExited) {
                Write-Host "[stop] PID $($proc.Id) $($proc.ProcessName)"
                Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
            }
        } catch {
            # Process already exited.
        }
    }
}

function Start-CoreSmokeRole {
    param(
        [int]$Index,
        [string]$Address,
        [hashtable]$CommonEnv,
        [string]$Stamp,
        [string]$Suffix = ""
    )
    $name = "core$Index"
    if ($Suffix) {
        $name = "$name-$Suffix"
    }
    $coreStdout = Join-Path $LogDir "$name-$Stamp.out.log"
    $coreStderr = Join-Path $LogDir "$name-$Stamp.err.log"
    Add-SmokeLogPath $coreStderr
    $coreEnv = Copy-Hashtable $CommonEnv
    $coreEnv["TELESRV_LISTEN"] = $CoreListen
    $coreEnv["TELESRV_CORE_EXEC_GRPC_ADDR"] = $Address
    $coreEnv["TELESRV_CORE_EXEC_GRPC_TARGETS"] = ""
    $coreEnv["TELESRV_INSTANCE_ID"] = "smoke-core-$PID-$Index"
    if ($Suffix) {
        $coreEnv["TELESRV_INSTANCE_ID"] = "smoke-core-$PID-$Index-$Suffix"
    }
    Add-CoreExecGRPCServerTLSEnv $coreEnv
    $proc = Start-TelesrvRole $name $coreEnv $coreStdout $coreStderr $CoreExePath
    $deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
    Wait-PortForProcess $proc (Get-ListenPort $Address) "coreexec grpc $Index" $coreStderr $deadline
    Wait-LogContains $proc $coreStderr "telesrv core role ready" $name $deadline
    Write-Host "[ok] $name ready: $Address"
    [pscustomobject]@{
        Process = $proc
        Log = $coreStderr
    }
}

function Start-EgressSmokeRole {
    param(
        [int]$Index,
        [string]$Address,
        [hashtable]$CommonEnv,
        [string]$Stamp,
        [string]$Suffix = ""
    )
    $name = "egress$Index"
    if ($Suffix) {
        $name = "$name-$Suffix"
    }
    $egressStdout = Join-Path $LogDir "$name-$Stamp.out.log"
    $egressStderr = Join-Path $LogDir "$name-$Stamp.err.log"
    Add-SmokeLogPath $egressStderr
    $egressEnv = Copy-Hashtable $CommonEnv
    $egressEnv["TELESRV_LISTEN"] = $CoreListen
    $egressEnv["TELESRV_CORE_EXEC_GRPC_ADDR"] = ""
    $egressEnv["TELESRV_CORE_EXEC_GRPC_TARGETS"] = ""
    $egressEnv["TELESRV_EGRESS_DELIVERY_GRPC_ADDR"] = $Address
    $egressEnv["TELESRV_EGRESS_DELIVERY_GRPC_TARGETS"] = ""
    $instanceID = "smoke-egress-$PID-$Index"
    if ($Suffix) {
        $instanceID = "smoke-egress-$PID-$Index-$Suffix"
    }
    $egressEnv["TELESRV_INSTANCE_ID"] = $instanceID
    $proc = Start-TelesrvRole $name $egressEnv $egressStdout $egressStderr $EgressExePath
    $deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
    Wait-PortForProcess $proc (Get-ListenPort $Address) "egress delivery grpc $Index" $egressStderr $deadline
    Wait-LogContains $proc $egressStderr "telesrv egress ready" $name $deadline
    Write-Host "[ok] $name ready: $Address"
    [pscustomobject]@{
        Process = $proc
        Log = $egressStderr
        InstanceID = $instanceID
        Address = $Address
    }
}

function Stop-CoreSmokeRole {
    param(
        [System.Diagnostics.Process]$Process,
        [string]$Name
    )
    if (-not $Process) {
        return
    }
    $Process.Refresh()
    if ($Process.HasExited) {
        return
    }
    Write-Host "[stop] $Name PID $($Process.Id)"
    Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
    $deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
    Wait-ProcessStopped $Process $Name $deadline
}

if ($GenerateCoreExecGRPCTestCerts) {
    if (-not $CoreExecGRPCTLSServerName) {
        $CoreExecGRPCTLSServerName = "core.internal"
    }
    $GeneratedCoreExecGRPCTestCertDir = Join-Path $LogDir "coreexec-mtls-certs"
    $certGenPath = Join-Path $RepoRoot "scripts\coreexec-smoke-certgen.go"
    Invoke-External "go" @(
        "run",
        $certGenPath,
        "-out",
        $GeneratedCoreExecGRPCTestCertDir,
        "-server-name",
        $CoreExecGRPCTLSServerName
    ) | Out-Null
    $CoreExecGRPCTLSServerCertFile = Join-Path $GeneratedCoreExecGRPCTestCertDir "server.pem"
    $CoreExecGRPCTLSServerKeyFile = Join-Path $GeneratedCoreExecGRPCTestCertDir "server-key.pem"
    $CoreExecGRPCTLSClientCAFile = Join-Path $GeneratedCoreExecGRPCTestCertDir "ca.pem"
    $CoreExecGRPCTLSCAFile = Join-Path $GeneratedCoreExecGRPCTestCertDir "ca.pem"
    $CoreExecGRPCTLSClientCertFile = Join-Path $GeneratedCoreExecGRPCTestCertDir "client.pem"
    $CoreExecGRPCTLSClientKeyFile = Join-Path $GeneratedCoreExecGRPCTestCertDir "client-key.pem"
    Write-Host "[ok] generated CoreExec mTLS smoke certs: $GeneratedCoreExecGRPCTestCertDir"
    Write-Host "[ok] generated CoreExec mTLS server name: $CoreExecGRPCTLSServerName"
}

$CoreExecGRPCTLSServerCertFile = Resolve-OptionalFile $CoreExecGRPCTLSServerCertFile "CoreExec gRPC server TLS certificate"
$CoreExecGRPCTLSServerKeyFile = Resolve-OptionalFile $CoreExecGRPCTLSServerKeyFile "CoreExec gRPC server TLS key"
$CoreExecGRPCTLSClientCAFile = Resolve-OptionalFile $CoreExecGRPCTLSClientCAFile "CoreExec gRPC client CA"
$CoreExecGRPCTLSCAFile = Resolve-OptionalFile $CoreExecGRPCTLSCAFile "CoreExec gRPC root CA"
$CoreExecGRPCTLSClientCertFile = Resolve-OptionalFile $CoreExecGRPCTLSClientCertFile "CoreExec gRPC client certificate"
$CoreExecGRPCTLSClientKeyFile = Resolve-OptionalFile $CoreExecGRPCTLSClientKeyFile "CoreExec gRPC client key"
$CoreExecGRPCTLSEnabled = [bool]($CoreExecGRPCTLSServerCertFile)
$CoreExecGRPCMTLSEnabled = [bool]($CoreExecGRPCTLSClientCAFile)

$SelectedCoreExecGRPCAddr = $CoreExecGRPCAddr
$CoreExecPort = Get-ListenPort $SelectedCoreExecGRPCAddr
$EdgePort = Get-ListenPort $EdgeListen
$EgressDeliveryPort = Get-ListenPort $EgressDeliveryGRPCAddr
if ($CoreExecPort -eq $EdgePort) {
    throw "Selected CoreExec address and EdgeListen must use different ports"
}
if ($EgressDeliveryPort -eq $CoreExecPort -or $EgressDeliveryPort -eq $EdgePort) {
    throw "Selected Egress Delivery address must use a port distinct from CoreExec and Edge"
}

New-Item -ItemType Directory -Force -Path $BinDir | Out-Null
New-Item -ItemType Directory -Force -Path $CoreBinDir | Out-Null
New-Item -ItemType Directory -Force -Path $EgressBinDir | Out-Null
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

Push-Location $RepoRoot
try {
    if ($PreflightOnly) {
        Invoke-SelectedPreflightGates
        Write-Host "[ok] PreflightOnly requested; split smoke runtime was not built or started"
        [pscustomobject]@{
            PreflightOnly = $true
            RedisFabricGateRun = [bool]$RunRedisFabricGate
            RedisAddr = $RedisAddr
            CoreExecReadinessFlapGateRun = [bool]$RunCoreExecReadinessFlapGate
            CoreExecDNSResolverGateRun = [bool]$RunCoreExecDNSResolverGate
            CoreExecCrossCoreAuthGateRun = [bool]$RunCoreExecCrossCoreAuthGate
            CoreExecNoImplicitRetryGateRun = [bool]$RunCoreExecNoImplicitRetryGate
            CoreExecFailureClassificationGateRun = [bool]$RunCoreExecFailureClassificationGate
            EgressOutboxCrashRecoveryGateRun = [bool]$RunEgressOutboxCrashRecoveryGate
            EgressOutboxCrashRecoveryLeaseTimeout = $EgressOutboxCrashRecoveryLeaseTimeout
            EgressOutboxCrashRecoveryStaleAge = $EgressOutboxCrashRecoveryStaleAge
        }
        return
    }

    if (-not $SkipBuild) {
        Write-Step "Build telesrv-edge, telesrv-core, and telesrv-egress"
        $commit = Get-GitOutput @("rev-parse", "HEAD")
        $branch = Get-GitOutput @("branch", "--show-current")
        $dirty = Get-GitOutput @("status", "--porcelain", "--untracked-files=no") -Default ""
        $treeState = "clean"
        if ($dirty.Length -gt 0) {
            $treeState = "dirty"
        }
        $buildTime = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        $ldflags = "-X main.gitCommit=$commit -X main.gitBranch=$branch -X main.gitTreeState=$treeState -X main.buildTime=$buildTime"
        Invoke-External "go" @("build", "-ldflags", $ldflags, "-o", $ExePath, ".\cmd\telesrv-edge") | Out-Null
        Invoke-External "go" @("build", "-ldflags", $ldflags, "-o", $CoreExePath, ".\cmd\telesrv-core") | Out-Null
        Invoke-External "go" @("build", "-ldflags", $ldflags, "-o", $EgressExePath, ".\cmd\telesrv-egress") | Out-Null
        Write-Host "[ok] built $ExePath"
        Write-Host "[ok] built $CoreExePath"
        Write-Host "[ok] built $EgressExePath"
        Write-Host "[ok] commit=$commit branch=$branch tree=$treeState build_time=$buildTime"
    } elseif (-not (Test-Path -LiteralPath $ExePath)) {
        throw "Executable not found: $ExePath"
    } elseif (-not (Test-Path -LiteralPath $CoreExePath)) {
        throw "Executable not found: $CoreExePath"
    } elseif (-not (Test-Path -LiteralPath $EgressExePath)) {
        throw "Executable not found: $EgressExePath"
    }

    if ($BuildOnly) {
        Write-Host "[ok] BuildOnly requested; split smoke runtime was not started"
        return
    }

    Invoke-SelectedPreflightGates

    if ($MultiInstance) {
        Write-Step "Preflight multi-instance"
        if ($CoreExecGRPCTLSEnabled) {
            Write-Host "[ok] CoreExec grpc TLS enabled; mTLS=$CoreExecGRPCMTLSEnabled"
        }
        $seenAddresses = @()
        foreach ($addr in $CoreExecGRPCAddrs) {
            foreach ($seen in $seenAddresses) {
                if (Test-ListenEndpointConflict $addr $seen) {
                    throw "Duplicate or conflicting smoke listen endpoint $addr overlaps $seen"
                }
            }
            $seenAddresses += $addr
            Assert-ListenAddressFree $addr "CoreExec grpc $addr"
            Add-SmokeCleanupPort (Get-ListenPort $addr)
            Write-Host "[ok] CoreExec grpc listen address is free ($addr)"
        }
        foreach ($addr in $EgressDeliveryGRPCAddrs) {
            foreach ($seen in $seenAddresses) {
                if (Test-ListenEndpointConflict $addr $seen) {
                    throw "Duplicate or conflicting smoke listen endpoint $addr overlaps $seen"
                }
            }
            $seenAddresses += $addr
            Assert-ListenAddressFree $addr "Egress Delivery grpc $addr"
            Add-SmokeCleanupPort (Get-ListenPort $addr)
            Write-Host "[ok] Egress Delivery grpc listen address is free ($addr)"
        }
        foreach ($addr in $EdgeListens) {
            foreach ($seen in $seenAddresses) {
                if (Test-ListenEndpointConflict $addr $seen) {
                    throw "Duplicate or conflicting smoke listen endpoint $addr overlaps $seen"
                }
            }
            $seenAddresses += $addr
            Assert-ListenAddressFree $addr "Edge $addr"
            Add-SmokeCleanupPort (Get-ListenPort $addr)
            Write-Host "[ok] Edge listen address is free ($addr)"
        }
        if ($InjectBadCoreExecTarget) {
            foreach ($seen in $seenAddresses) {
                if (Test-ListenEndpointConflict $BadCoreExecGRPCAddr $seen) {
                    throw "Bad CoreExec target $BadCoreExecGRPCAddr overlaps with smoke listener $seen"
                }
            }
            Assert-ListenAddressFree $BadCoreExecGRPCAddr "Bad CoreExec grpc target $BadCoreExecGRPCAddr"
            Write-Host "[ok] bad CoreExec grpc target will remain unstarted: $BadCoreExecGRPCAddr"
        }

        $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
        if ($RunCoreExecDNSMultiAProcessGate) {
            Write-Step "CoreExec DNS multi-A authority"
            $dnsAuthority = Start-CoreExecDNSMultiAAuthority $stamp
            $CoreExecGRPCTargets = $dnsAuthority.Target
            $CoreExecGRPCResolver = "dns"
            $CoreExecDNSMultiATarget = $dnsAuthority.Target
            $CoreExecDNSMultiARecords = ($dnsAuthority.Records -join ",")
            $CoreExecDNSMultiAAuthorityStdout = $dnsAuthority.Stdout
            $CoreExecDNSMultiAAuthorityStderr = $dnsAuthority.Stderr
        }
        $commonEnv = @{
            TELESRV_ADVERTISE_IP = $AdvertiseIP
            TELESRV_ADVERTISE_PORT = [string](Get-ListenPort $EdgeListen)
            TELESRV_ADMIN_API_ADDR = "127.0.0.1:0"
            TELESRV_ADMIN_API_TOKEN = "edge-core-smoke-admin"
            TELESRV_BOT_API_ADDR = "127.0.0.1:0"
            TELESRV_DEBUG_ADDR = "127.0.0.1:0"
            TELESRV_GROUPCALL_CONTROL_ADDR = "127.0.0.1:0"
            TELESRV_GROUPCALL_CONTROL_TOKEN = "edge-core-smoke-groupcall"
            TELESRV_PUBLIC_LINK_WEB_ADDR = "127.0.0.1:0"
            TELESRV_TELEGRAM_LOGIN_ENABLE = "false"
            TELESRV_LIVESTREAM_ENABLE = "false"
            TELESRV_SFU_CONTROL_TOKEN = "edge-core-smoke-sfu"
            TELESRV_TURN_ENABLE = "false"
            TELESRV_CORE_EXEC_TOKEN = $CoreExecToken
            TELESRV_EGRESS_DELIVERY_TOKEN = $EgressDeliveryToken
        }
        if ($RunEgressOutboxMultiProcessGate -or $RunEgressOutboxProcessCrashGate) {
            $commonEnv["TELESRV_POSTGRES_MAX_CONNS"] = "8"
            $commonEnv["TELESRV_POSTGRES_MIN_CONNS"] = "1"
            $commonEnv["TELESRV_OUTBOX_WORKERS"] = "1"
            $commonEnv["TELESRV_OUTBOX_BATCH"] = "1"
            $commonEnv["TELESRV_OUTBOUND_PUSH_TIMEOUT"] = "1s"
        }
        if ($RunEgressOutboxProcessCrashGate) {
            $commonEnv["TELESRV_OUTBOX_LEASE_TIMEOUT"] = $EgressOutboxProcessCrashLeaseTimeout
        }
        if ($PostgresDSN) {
            $commonEnv["TELESRV_POSTGRES_DSN"] = $PostgresDSN
        }
        if ($RedisAddr) {
            $commonEnv["TELESRV_REDIS_ADDR"] = $RedisAddr
        }
        if ($RSAKeyPath) {
            $commonEnv["TELESRV_RSA_KEY"] = $RSAKeyPath
        }
        if ($RunCoreExecUnavailableGate) {
            Invoke-CoreExecUnavailableGate $commonEnv $stamp
        }

        Write-Step "Start core roles"
        $coreProcs = @()
        $coreLogs = @()
        for ($i = 0; $i -lt $CoreExecGRPCAddrs.Count; $i++) {
            $addr = $CoreExecGRPCAddrs[$i]
            $startedCore = Start-CoreSmokeRole $i $addr $commonEnv $stamp
            $coreProcs += $startedCore.Process
            $coreLogs += $startedCore.Log
        }

        Write-Step "Start egress roles"
        $egressProcs = @()
        $egressLogs = @()
        $egressInstanceIDs = @()
        for ($i = 0; $i -lt $EgressDeliveryGRPCAddrs.Count; $i++) {
            $addr = $EgressDeliveryGRPCAddrs[$i]
            $startedEgress = Start-EgressSmokeRole $i $addr $commonEnv $stamp
            $egressProcs += $startedEgress.Process
            $egressLogs += $startedEgress.Log
            $egressInstanceIDs += $startedEgress.InstanceID
        }

        Write-Step "Start edge roles"
        $edgeProcs = @()
        $edgeLogs = @()
        $edgeDebugAddrs = @()
        for ($i = 0; $i -lt $EdgeListens.Count; $i++) {
            $addr = $EdgeListens[$i]
            $edgeDebugAddr = New-LoopbackListenAddress
            $edgeStdout = Join-Path $LogDir "edge$i-$stamp.out.log"
            $edgeStderr = Join-Path $LogDir "edge$i-$stamp.err.log"
            Add-SmokeLogPath $edgeStderr
            $edgeEnv = Copy-Hashtable $commonEnv
			$edgeEnv["TELESRV_LISTEN"] = $addr
            $edgeEnv["TELESRV_DEBUG_ADDR"] = $edgeDebugAddr
            $edgeEnv["TELESRV_CORE_EXEC_GRPC_ADDR"] = ""
            $edgeEnv["TELESRV_CORE_EXEC_GRPC_TARGETS"] = $CoreExecGRPCTargets
            $edgeEnv["TELESRV_CORE_EXEC_GRPC_RESOLVER"] = $CoreExecGRPCResolver
            $edgeEnv["TELESRV_EGRESS_DELIVERY_GRPC_ADDR"] = ""
            $edgeEnv["TELESRV_EGRESS_DELIVERY_GRPC_TARGETS"] = $EgressDeliveryGRPCTargets
            $edgeEnv["TELESRV_EGRESS_DELIVERY_GRPC_RESOLVER"] = $EgressDeliveryGRPCResolver
            $edgeEnv["TELESRV_INSTANCE_ID"] = "smoke-edge-$PID-$i"
            Add-CoreExecGRPCClientTLSEnv $edgeEnv
            $proc = Start-TelesrvRole "edge$i" $edgeEnv $edgeStdout $edgeStderr
            $edgeProcs += $proc
            $edgeLogs += $edgeStderr
            $edgeDebugAddrs += $edgeDebugAddr
            $deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
            Wait-PortForProcess $proc (Get-ListenPort $addr) "edge$i" $edgeStderr $deadline
            Wait-LogContains $proc $edgeStderr "telesrv edge ready" "edge$i" $deadline
            Write-Host "[ok] edge$i ready: $addr debug=$edgeDebugAddr"
        }

        if ($RunMTProtoEdgeProbeGate) {
            Write-Step "MTProto Edge probe gate"
            Invoke-MTProtoEdgeSmokeProbe -Name "after startup" -Addresses $EdgeListens -Count $MTProtoEdgeProbeCount
        }
        if ($RunMTProtoEdgeAuthProbeGate) {
            Write-Step "MTProto Edge auth probe gate"
            Invoke-MTProtoEdgeAuthSmokeProbe -Name "after startup" -Addresses $EdgeListens
        }
        if ($RunCoreExecProcessProbeGate) {
            Write-Step "CoreExec process probe gate"
            Invoke-CoreExecSmokeProbe "after startup" 8 $CoreExecProcessProbeExpectInstances
        }
        if ($RunEgressDeliveryProcessProbeGate) {
            Write-Step "Egress Delivery process probe gate"
            Invoke-EgressDeliverySmokeProbe "after startup" 8 $EgressDeliveryProcessProbeExpectInstances
        }
        if ($RunEgressOutboxMultiProcessGate) {
            Write-Step "Egress outbox multi-process gate"
            Invoke-EgressOutboxMultiProcessGate
        }
        $egressOutboxProcessCrashSourceInstanceID = ""
        if ($RunEgressOutboxProcessCrashGate) {
            Write-Step "Egress outbox process crash recovery gate"
            $crashResult = Invoke-EgressOutboxProcessCrashGate -EgressProcesses $egressProcs -EgressInstanceIDs $egressInstanceIDs -EgressAddresses $EgressDeliveryGRPCAddrs -CommonEnv $commonEnv -Stamp $stamp
            $egressProcs[$crashResult.Index] = $crashResult.Restarted.Process
            $egressLogs[$crashResult.Index] = $crashResult.Restarted.Log
            $egressInstanceIDs[$crashResult.Index] = $crashResult.Restarted.InstanceID
            $egressOutboxProcessCrashSourceInstanceID = $crashResult.SourceInstanceID
        }
        if ($RunCoreExecPendingMetricsGate) {
            Write-Step "CoreExec pending metrics gate"
            Invoke-CoreExecPendingMetricsGate -Name "after startup" -DebugAddrs $edgeDebugAddrs -Transport "grpc"
        }

        if ($RunCoreExecRollingRestartGate) {
            if ($CoreExecGRPCAddrs.Count -lt 2) {
                throw "-RunCoreExecRollingRestartGate requires at least two CoreExec gRPC addrs"
            }
            Write-Step "CoreExec rolling restart gate"
            Invoke-CoreExecSmokeProbe "before restart" 8
            if ($RunMTProtoEdgeProbeGate) {
                Invoke-MTProtoEdgeSmokeProbe -Name "before restart" -Addresses $EdgeListens -Count $MTProtoEdgeProbeCount
            }

            Stop-CoreSmokeRole $coreProcs[0] "core0 rolling gate"
            Wait-PortFree (Get-ListenPort $CoreExecGRPCAddrs[0]) "core0 rolling gate" (Get-Date).AddSeconds($StartupTimeoutSeconds)
            Invoke-CoreExecSmokeProbe "after core0 stopped" 8
            if ($RunMTProtoEdgeProbeGate) {
                Invoke-MTProtoEdgeSmokeProbe -Name "after core0 stopped" -Addresses $EdgeListens -Count $MTProtoEdgeProbeCount
            }
            if ($RunMTProtoEdgeAuthProbeGate) {
                Invoke-MTProtoEdgeAuthSmokeProbe -Name "after core0 stopped" -Addresses $EdgeListens
            }

            $restartedCore0 = Start-CoreSmokeRole 0 $CoreExecGRPCAddrs[0] $commonEnv $stamp "restart"
            $coreProcs[0] = $restartedCore0.Process
            $coreLogs[0] = $restartedCore0.Log

            Stop-CoreSmokeRole $coreProcs[1] "core1 rolling gate"
            Wait-PortFree (Get-ListenPort $CoreExecGRPCAddrs[1]) "core1 rolling gate" (Get-Date).AddSeconds($StartupTimeoutSeconds)
            Invoke-CoreExecSmokeProbe "after core1 stopped and core0 restarted" 8
            if ($RunMTProtoEdgeProbeGate) {
                Invoke-MTProtoEdgeSmokeProbe -Name "after core1 stopped and core0 restarted" -Addresses $EdgeListens -Count $MTProtoEdgeProbeCount
            }
            if ($RunMTProtoEdgeAuthProbeGate) {
                Invoke-MTProtoEdgeAuthSmokeProbe -Name "after core1 stopped and core0 restarted" -Addresses $EdgeListens
            }

            $restartedCore1 = Start-CoreSmokeRole 1 $CoreExecGRPCAddrs[1] $commonEnv $stamp "restart"
            $coreProcs[1] = $restartedCore1.Process
            $coreLogs[1] = $restartedCore1.Log
            Write-Host "[ok] CoreExec rolling restart gate passed"
        }

        if ($RunEgressDeliveryRollingRestartGate) {
            if ($EgressDeliveryGRPCAddrs.Count -lt 2) {
                throw "-RunEgressDeliveryRollingRestartGate requires at least two Egress Delivery gRPC addrs"
            }
            Write-Step "Egress Delivery rolling restart gate"
            Invoke-EgressDeliverySmokeProbe "before restart" 8 $EgressDeliveryProcessProbeExpectInstances

            Stop-CoreSmokeRole $egressProcs[0] "egress0 rolling gate"
            Wait-PortFree (Get-ListenPort $EgressDeliveryGRPCAddrs[0]) "egress0 rolling gate" (Get-Date).AddSeconds($StartupTimeoutSeconds)
            Invoke-EgressDeliverySmokeProbe "after egress0 stopped" 8

            $restartedEgress0 = Start-EgressSmokeRole 0 $EgressDeliveryGRPCAddrs[0] $commonEnv $stamp "restart"
            $egressProcs[0] = $restartedEgress0.Process
            $egressLogs[0] = $restartedEgress0.Log
            $egressInstanceIDs[0] = $restartedEgress0.InstanceID

            Stop-CoreSmokeRole $egressProcs[1] "egress1 rolling gate"
            Wait-PortFree (Get-ListenPort $EgressDeliveryGRPCAddrs[1]) "egress1 rolling gate" (Get-Date).AddSeconds($StartupTimeoutSeconds)
            Invoke-EgressDeliverySmokeProbe "after egress1 stopped and egress0 restarted" 8

            $restartedEgress1 = Start-EgressSmokeRole 1 $EgressDeliveryGRPCAddrs[1] $commonEnv $stamp "restart"
            $egressProcs[1] = $restartedEgress1.Process
            $egressLogs[1] = $restartedEgress1.Log
            $egressInstanceIDs[1] = $restartedEgress1.InstanceID
            Write-Host "[ok] Egress Delivery rolling restart gate passed"
        }

        Write-Step "Smoke result"
        Write-Host "[ok] multi-instance split smoke passed over CoreExec grpc and Egress Delivery grpc"
        Write-Host "[ok] CoreExec targets: $CoreExecGRPCTargets"
        Write-Host "[ok] CoreExec resolver: $CoreExecGRPCResolver"
        Write-Host "[ok] Egress Delivery targets: $EgressDeliveryGRPCTargets"
        Write-Host "[ok] Egress Delivery resolver: $EgressDeliveryGRPCResolver"
        Write-Host "[ok] Egress outbox multi-process gate run: $RunEgressOutboxMultiProcessGate"
        Write-Host "[ok] CoreExec unavailable gate run: $RunCoreExecUnavailableGate"
        Write-Host "[ok] CoreExec readiness flap gate run: $RunCoreExecReadinessFlapGate"
        Write-Host "[ok] CoreExec DNS resolver gate run: $RunCoreExecDNSResolverGate"
        Write-Host "[ok] CoreExec DNS multi-A process gate run: $RunCoreExecDNSMultiAProcessGate $CoreExecDNSMultiATarget records: $CoreExecDNSMultiARecords"
        Write-Host "[ok] CoreExec cross-Core auth gate run: $RunCoreExecCrossCoreAuthGate"
        Write-Host "[ok] CoreExec no implicit retry gate run: $RunCoreExecNoImplicitRetryGate"
        Write-Host "[ok] CoreExec failure classification gate run: $RunCoreExecFailureClassificationGate"
        Write-Host "[ok] CoreExec process probe gate run: $RunCoreExecProcessProbeGate expect_instances: $CoreExecProcessProbeExpectInstances duration: $CoreExecProcessProbeDuration interval: $CoreExecProcessProbeInterval"
        Write-Host "[ok] Egress Delivery process probe gate run: $RunEgressDeliveryProcessProbeGate expect_instances: $EgressDeliveryProcessProbeExpectInstances duration: $EgressDeliveryProcessProbeDuration interval: $EgressDeliveryProcessProbeInterval"
        Write-Host "[ok] CoreExec pending metrics gate run: $RunCoreExecPendingMetricsGate debug_addrs: $($edgeDebugAddrs -join ',')"
        Write-Host "[ok] CoreExec rolling restart gate run: $RunCoreExecRollingRestartGate"
        Write-Host "[ok] Egress Delivery rolling restart gate run: $RunEgressDeliveryRollingRestartGate"
        Write-Host "[ok] Egress outbox crash recovery gate run: $RunEgressOutboxCrashRecoveryGate lease_timeout: $EgressOutboxCrashRecoveryLeaseTimeout stale_age: $EgressOutboxCrashRecoveryStaleAge"
        Write-Host "[ok] Egress outbox process crash gate run: $RunEgressOutboxProcessCrashGate source: $egressOutboxProcessCrashSourceInstanceID lease_timeout: $EgressOutboxProcessCrashLeaseTimeout"
        Write-Host "[ok] MTProto Edge probe gate run: $RunMTProtoEdgeProbeGate count: $MTProtoEdgeProbeCount obfuscated: $MTProtoEdgeProbeObfuscated"
        Write-Host "[ok] MTProto Edge auth probe gate run: $RunMTProtoEdgeAuthProbeGate phone_prefix: $MTProtoEdgeAuthPhonePrefix runs=$MTProtoEdgeAuthProbeRuns"
        Write-Host "[ok] CoreExec bad target injected: $InjectBadCoreExecTarget $BadCoreExecGRPCAddr"
        Write-Host "[ok] CoreExec TLS enabled: $CoreExecGRPCTLSEnabled mTLS: $CoreExecGRPCMTLSEnabled"
        Write-Host "[ok] CoreExec generated test certs: $GenerateCoreExecGRPCTestCerts $GeneratedCoreExecGRPCTestCertDir"
        Write-Host "[ok] Redis fabric gate run: $RunRedisFabricGate $RedisAddr"
        Write-Host "[ok] post-smoke log safety gate requested: $RunLogSafetyGate"
        Write-Host "[ok] post-smoke port cleanup gate requested: $RunPortCleanupGate"
        Write-Host "[ok] core logs: $($coreLogs -join '; ')"
        Write-Host "[ok] egress logs: $($egressLogs -join '; ')"
        Write-Host "[ok] edge logs: $($edgeLogs -join '; ')"
        if ($KeepRunning) {
            Write-Host "[ok] KeepRunning requested; leave these processes running:"
            Write-Host "     core PIDs $((@($coreProcs) | ForEach-Object { $_.Id }) -join ',')"
            Write-Host "     egress PIDs $((@($egressProcs) | ForEach-Object { $_.Id }) -join ',')"
            Write-Host "     edge PIDs $((@($edgeProcs) | ForEach-Object { $_.Id }) -join ',')"
        }

        $SmokeSucceeded = $true
        [pscustomobject]@{
            CorePIDs = @($coreProcs) | ForEach-Object { $_.Id }
            EgressPIDs = @($egressProcs) | ForEach-Object { $_.Id }
            EdgePIDs = @($edgeProcs) | ForEach-Object { $_.Id }
            CoreExecTransport = "grpc"
            CoreExecGRPCTargets = $CoreExecGRPCTargets
            CoreExecGRPCResolver = $CoreExecGRPCResolver
            EgressDeliveryGRPCTargets = $EgressDeliveryGRPCTargets
            EgressDeliveryGRPCResolver = $EgressDeliveryGRPCResolver
            CoreExecUnavailableGateRun = [bool]$RunCoreExecUnavailableGate
            CoreExecReadinessFlapGateRun = [bool]$RunCoreExecReadinessFlapGate
            CoreExecDNSResolverGateRun = [bool]$RunCoreExecDNSResolverGate
            CoreExecDNSMultiAProcessGateRun = [bool]$RunCoreExecDNSMultiAProcessGate
            CoreExecDNSMultiATarget = $CoreExecDNSMultiATarget
            CoreExecDNSMultiARecords = $CoreExecDNSMultiARecords
            CoreExecDNSMultiAAuthorityStdout = $CoreExecDNSMultiAAuthorityStdout
            CoreExecDNSMultiAAuthorityStderr = $CoreExecDNSMultiAAuthorityStderr
            CoreExecCrossCoreAuthGateRun = [bool]$RunCoreExecCrossCoreAuthGate
            CoreExecNoImplicitRetryGateRun = [bool]$RunCoreExecNoImplicitRetryGate
            CoreExecFailureClassificationGateRun = [bool]$RunCoreExecFailureClassificationGate
            CoreExecProcessProbeGateRun = [bool]$RunCoreExecProcessProbeGate
            CoreExecProcessProbeExpectInstances = $CoreExecProcessProbeExpectInstances
            CoreExecProcessProbeDuration = $CoreExecProcessProbeDuration
            CoreExecProcessProbeInterval = $CoreExecProcessProbeInterval
            EgressDeliveryProcessProbeGateRun = [bool]$RunEgressDeliveryProcessProbeGate
            EgressDeliveryProcessProbeExpectInstances = $EgressDeliveryProcessProbeExpectInstances
            EgressDeliveryProcessProbeDuration = $EgressDeliveryProcessProbeDuration
            EgressDeliveryProcessProbeInterval = $EgressDeliveryProcessProbeInterval
            CoreExecPendingMetricsGateRun = [bool]$RunCoreExecPendingMetricsGate
            CoreExecRollingRestartGateRun = [bool]$RunCoreExecRollingRestartGate
            EgressDeliveryRollingRestartGateRun = [bool]$RunEgressDeliveryRollingRestartGate
            EgressOutboxCrashRecoveryGateRun = [bool]$RunEgressOutboxCrashRecoveryGate
            EgressOutboxCrashRecoveryLeaseTimeout = $EgressOutboxCrashRecoveryLeaseTimeout
            EgressOutboxCrashRecoveryStaleAge = $EgressOutboxCrashRecoveryStaleAge
            EgressOutboxProcessCrashGateRun = [bool]$RunEgressOutboxProcessCrashGate
            EgressOutboxProcessCrashSourceInstanceID = $egressOutboxProcessCrashSourceInstanceID
            EgressOutboxProcessCrashLeaseTimeout = $EgressOutboxProcessCrashLeaseTimeout
            MTProtoEdgeProbeGateRun = [bool]$RunMTProtoEdgeProbeGate
            MTProtoEdgeProbeCount = $MTProtoEdgeProbeCount
            MTProtoEdgeProbeObfuscated = [bool]$MTProtoEdgeProbeObfuscated
            MTProtoEdgeAuthProbeGateRun = [bool]$RunMTProtoEdgeAuthProbeGate
            MTProtoEdgeAuthPhonePrefix = $MTProtoEdgeAuthPhonePrefix
            MTProtoEdgeAuthProbeRuns = $MTProtoEdgeAuthProbeRuns
            InjectedBadCoreExecTarget = [bool]$InjectBadCoreExecTarget
            BadCoreExecGRPCAddr = $BadCoreExecGRPCAddr
            CoreExecGRPCTLSEnabled = $CoreExecGRPCTLSEnabled
            CoreExecGRPCMTLSEnabled = $CoreExecGRPCMTLSEnabled
            GeneratedCoreExecGRPCTestCerts = [bool]$GenerateCoreExecGRPCTestCerts
            GeneratedCoreExecGRPCTestCertDir = $GeneratedCoreExecGRPCTestCertDir
            CoreExecGRPCTLSServerName = $CoreExecGRPCTLSServerName
            RedisFabricGateRun = [bool]$RunRedisFabricGate
            RedisAddr = $RedisAddr
            LogSafetyGateRun = [bool]$RunLogSafetyGate
            PortCleanupGateRun = [bool]$RunPortCleanupGate
            EdgeListens = $EdgeListens
            EdgeDebugAddrs = $edgeDebugAddrs
            CoreLogs = $coreLogs
            EgressLogs = $egressLogs
            EdgeLogs = $edgeLogs
            KeptRunning = [bool]$KeepRunning
        }
        return
    }

    Write-Step "Preflight"
    if ($CoreExecGRPCTLSEnabled) {
        Write-Host "[ok] CoreExec grpc TLS enabled; mTLS=$CoreExecGRPCMTLSEnabled"
    }
    Assert-PortFree $CoreExecPort "CoreExec grpc"
    Assert-PortFree $EgressDeliveryPort "Egress Delivery grpc"
    Assert-PortFree $EdgePort "Edge"
    Add-SmokeCleanupPort $CoreExecPort
    Add-SmokeCleanupPort $EgressDeliveryPort
    Add-SmokeCleanupPort $EdgePort
    if ($InjectBadCoreExecTarget) {
        $badPort = Get-ListenPort $BadCoreExecGRPCAddr
        if ($badPort -eq $CoreExecPort -or $badPort -eq $EgressDeliveryPort -or $badPort -eq $EdgePort) {
            throw "Bad CoreExec target port $badPort must not overlap CoreExec, Egress Delivery, or Edge listen ports"
        }
        Assert-PortFree $badPort "Bad CoreExec grpc target $BadCoreExecGRPCAddr"
        Write-Host "[ok] bad CoreExec grpc target will remain unstarted: $BadCoreExecGRPCAddr"
    }
    Write-Host "[ok] CoreExec grpc port $CoreExecPort is free"
    Write-Host "[ok] Egress Delivery grpc port $EgressDeliveryPort is free"
    Write-Host "[ok] Edge port $EdgePort is free"

    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $coreStdout = Join-Path $LogDir "core-$stamp.out.log"
    $coreStderr = Join-Path $LogDir "core-$stamp.err.log"
    $edgeStdout = Join-Path $LogDir "edge-$stamp.out.log"
    $edgeStderr = Join-Path $LogDir "edge-$stamp.err.log"
    $edgeDebugAddr = New-LoopbackListenAddress
    Add-SmokeLogPath $coreStderr
    Add-SmokeLogPath $edgeStderr

    $commonEnv = @{
        TELESRV_ADVERTISE_IP = $AdvertiseIP
        TELESRV_ADVERTISE_PORT = [string](Get-ListenPort $EdgeListen)
        TELESRV_ADMIN_API_ADDR = "127.0.0.1:0"
        TELESRV_ADMIN_API_TOKEN = "edge-core-smoke-admin"
        TELESRV_BOT_API_ADDR = "127.0.0.1:0"
        TELESRV_DEBUG_ADDR = "127.0.0.1:0"
        TELESRV_GROUPCALL_CONTROL_ADDR = "127.0.0.1:0"
        TELESRV_GROUPCALL_CONTROL_TOKEN = "edge-core-smoke-groupcall"
        TELESRV_PUBLIC_LINK_WEB_ADDR = "127.0.0.1:0"
        TELESRV_TELEGRAM_LOGIN_ENABLE = "false"
        TELESRV_LIVESTREAM_ENABLE = "false"
        TELESRV_SFU_CONTROL_TOKEN = "edge-core-smoke-sfu"
        TELESRV_TURN_ENABLE = "false"
        TELESRV_CORE_EXEC_TOKEN = $CoreExecToken
        TELESRV_EGRESS_DELIVERY_TOKEN = $EgressDeliveryToken
    }
    if ($PostgresDSN) {
        $commonEnv["TELESRV_POSTGRES_DSN"] = $PostgresDSN
    }
    if ($RedisAddr) {
        $commonEnv["TELESRV_REDIS_ADDR"] = $RedisAddr
    }
    if ($RSAKeyPath) {
        $commonEnv["TELESRV_RSA_KEY"] = $RSAKeyPath
    }
    if ($RunCoreExecUnavailableGate) {
        Invoke-CoreExecUnavailableGate $commonEnv $stamp
    }

    Write-Step "Start core role"
    $coreEnv = Copy-Hashtable $commonEnv
    $coreEnv["TELESRV_LISTEN"] = $CoreListen
    $coreEnv["TELESRV_CORE_EXEC_GRPC_ADDR"] = $CoreExecGRPCAddr
    $coreEnv["TELESRV_CORE_EXEC_GRPC_TARGETS"] = ""
    Add-CoreExecGRPCServerTLSEnv $coreEnv
    $coreEnv["TELESRV_INSTANCE_ID"] = "smoke-core-$PID"
    $coreProc = Start-TelesrvRole "core" $coreEnv $coreStdout $coreStderr $CoreExePath
    $deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
    Wait-PortForProcess $coreProc $CoreExecPort "coreexec grpc" $coreStderr $deadline
    Wait-LogContains $coreProc $coreStderr "telesrv core role ready" "core" $deadline
    Write-Host "[ok] coreexec grpc listening: $CoreExecGRPCAddr"

    Write-Step "Start egress role"
    $startedEgress = Start-EgressSmokeRole 0 $EgressDeliveryGRPCAddr $commonEnv $stamp
    $egressProc = $startedEgress.Process
    $egressStderr = $startedEgress.Log
    Write-Host "[ok] egress delivery grpc listening: $EgressDeliveryGRPCAddr"

    Write-Step "Start edge role"
    $edgeEnv = Copy-Hashtable $commonEnv
	$edgeEnv["TELESRV_LISTEN"] = $EdgeListen
    $edgeEnv["TELESRV_DEBUG_ADDR"] = $edgeDebugAddr
    $edgeEnv["TELESRV_CORE_EXEC_GRPC_ADDR"] = ""
    $edgeEnv["TELESRV_CORE_EXEC_GRPC_TARGETS"] = $CoreExecGRPCTargets
    $edgeEnv["TELESRV_CORE_EXEC_GRPC_RESOLVER"] = $CoreExecGRPCResolver
    $edgeEnv["TELESRV_EGRESS_DELIVERY_GRPC_ADDR"] = ""
    $edgeEnv["TELESRV_EGRESS_DELIVERY_GRPC_TARGETS"] = $EgressDeliveryGRPCTargets
    $edgeEnv["TELESRV_EGRESS_DELIVERY_GRPC_RESOLVER"] = $EgressDeliveryGRPCResolver
    Add-CoreExecGRPCClientTLSEnv $edgeEnv
    $edgeEnv["TELESRV_INSTANCE_ID"] = "smoke-edge-$PID"
    $edgeProc = Start-TelesrvRole "edge" $edgeEnv $edgeStdout $edgeStderr
    $deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
    Wait-PortForProcess $edgeProc $EdgePort "edge" $edgeStderr $deadline
    Wait-LogContains $edgeProc $edgeStderr "telesrv edge ready" "edge" $deadline
    Write-Host "[ok] edge is listening on $EdgeListen debug=$edgeDebugAddr"

    if ($RunMTProtoEdgeProbeGate) {
        Write-Step "MTProto Edge probe gate"
        Invoke-MTProtoEdgeSmokeProbe -Name "after startup" -Addresses @($EdgeListen) -Count $MTProtoEdgeProbeCount
    }
    if ($RunMTProtoEdgeAuthProbeGate) {
        Write-Step "MTProto Edge auth probe gate"
        Invoke-MTProtoEdgeAuthSmokeProbe -Name "after startup" -Addresses @($EdgeListen)
    }
    if ($RunCoreExecProcessProbeGate) {
        Write-Step "CoreExec process probe gate"
        Invoke-CoreExecSmokeProbe "after startup" 8 $CoreExecProcessProbeExpectInstances
    }
    if ($RunEgressDeliveryProcessProbeGate) {
        Write-Step "Egress Delivery process probe gate"
        Invoke-EgressDeliverySmokeProbe "after startup" 8 $EgressDeliveryProcessProbeExpectInstances
    }
    if ($RunCoreExecPendingMetricsGate) {
        Write-Step "CoreExec pending metrics gate"
        Invoke-CoreExecPendingMetricsGate -Name "after startup" -DebugAddrs @($edgeDebugAddr) -Transport $CoreExecTransport
    }

    Write-Step "Smoke result"
    Write-Host "[ok] core + egress + edge split smoke passed over CoreExec grpc and Egress Delivery grpc"
    Write-Host "[ok] CoreExec targets: $CoreExecGRPCTargets"
    Write-Host "[ok] CoreExec resolver: $CoreExecGRPCResolver"
    Write-Host "[ok] Egress Delivery targets: $EgressDeliveryGRPCTargets"
    Write-Host "[ok] Egress Delivery resolver: $EgressDeliveryGRPCResolver"
    Write-Host "[ok] CoreExec unavailable gate run: $RunCoreExecUnavailableGate"
    Write-Host "[ok] CoreExec readiness flap gate run: $RunCoreExecReadinessFlapGate"
    Write-Host "[ok] CoreExec DNS resolver gate run: $RunCoreExecDNSResolverGate"
    Write-Host "[ok] CoreExec cross-Core auth gate run: $RunCoreExecCrossCoreAuthGate"
    Write-Host "[ok] CoreExec no implicit retry gate run: $RunCoreExecNoImplicitRetryGate"
    Write-Host "[ok] CoreExec failure classification gate run: $RunCoreExecFailureClassificationGate"
    Write-Host "[ok] CoreExec process probe gate run: $RunCoreExecProcessProbeGate expect_instances: $CoreExecProcessProbeExpectInstances duration: $CoreExecProcessProbeDuration interval: $CoreExecProcessProbeInterval"
    Write-Host "[ok] Egress Delivery process probe gate run: $RunEgressDeliveryProcessProbeGate expect_instances: $EgressDeliveryProcessProbeExpectInstances duration: $EgressDeliveryProcessProbeDuration interval: $EgressDeliveryProcessProbeInterval"
    Write-Host "[ok] CoreExec pending metrics gate run: $RunCoreExecPendingMetricsGate debug_addrs: $edgeDebugAddr"
    Write-Host "[ok] Egress outbox crash recovery gate run: $RunEgressOutboxCrashRecoveryGate lease_timeout: $EgressOutboxCrashRecoveryLeaseTimeout stale_age: $EgressOutboxCrashRecoveryStaleAge"
    Write-Host "[ok] MTProto Edge probe gate run: $RunMTProtoEdgeProbeGate count: $MTProtoEdgeProbeCount obfuscated: $MTProtoEdgeProbeObfuscated"
    Write-Host "[ok] MTProto Edge auth probe gate run: $RunMTProtoEdgeAuthProbeGate phone_prefix: $MTProtoEdgeAuthPhonePrefix runs=$MTProtoEdgeAuthProbeRuns"
    Write-Host "[ok] CoreExec bad target injected: $InjectBadCoreExecTarget $BadCoreExecGRPCAddr"
    Write-Host "[ok] CoreExec TLS enabled: $CoreExecGRPCTLSEnabled mTLS: $CoreExecGRPCMTLSEnabled"
    Write-Host "[ok] CoreExec generated test certs: $GenerateCoreExecGRPCTestCerts $GeneratedCoreExecGRPCTestCertDir"
    Write-Host "[ok] Redis fabric gate run: $RunRedisFabricGate $RedisAddr"
    Write-Host "[ok] post-smoke log safety gate requested: $RunLogSafetyGate"
    Write-Host "[ok] post-smoke port cleanup gate requested: $RunPortCleanupGate"
    Write-Host "[ok] core stderr: $coreStderr"
    Write-Host "[ok] egress stderr: $egressStderr"
    Write-Host "[ok] edge stderr: $edgeStderr"
    if ($KeepRunning) {
        Write-Host "[ok] KeepRunning requested; leave these processes running:"
        Write-Host "     core PID $($coreProc.Id)"
        Write-Host "     egress PID $($egressProc.Id)"
        Write-Host "     edge PID $($edgeProc.Id)"
    }

    $SmokeSucceeded = $true
    [pscustomobject]@{
        CorePID = $coreProc.Id
        EgressPID = $egressProc.Id
        EdgePID = $edgeProc.Id
        CoreExecTransport = "grpc"
        CoreExecGRPCAddr = $CoreExecGRPCAddr
        CoreExecGRPCTargets = $CoreExecGRPCTargets
        CoreExecGRPCResolver = $CoreExecGRPCResolver
        EgressDeliveryGRPCAddr = $EgressDeliveryGRPCAddr
        EgressDeliveryGRPCTargets = $EgressDeliveryGRPCTargets
        EgressDeliveryGRPCResolver = $EgressDeliveryGRPCResolver
        CoreExecUnavailableGateRun = [bool]$RunCoreExecUnavailableGate
        CoreExecReadinessFlapGateRun = [bool]$RunCoreExecReadinessFlapGate
        CoreExecDNSResolverGateRun = [bool]$RunCoreExecDNSResolverGate
        CoreExecCrossCoreAuthGateRun = [bool]$RunCoreExecCrossCoreAuthGate
        CoreExecNoImplicitRetryGateRun = [bool]$RunCoreExecNoImplicitRetryGate
        CoreExecFailureClassificationGateRun = [bool]$RunCoreExecFailureClassificationGate
        CoreExecProcessProbeGateRun = [bool]$RunCoreExecProcessProbeGate
        CoreExecProcessProbeExpectInstances = $CoreExecProcessProbeExpectInstances
        CoreExecProcessProbeDuration = $CoreExecProcessProbeDuration
        CoreExecProcessProbeInterval = $CoreExecProcessProbeInterval
        EgressDeliveryProcessProbeGateRun = [bool]$RunEgressDeliveryProcessProbeGate
        EgressDeliveryProcessProbeExpectInstances = $EgressDeliveryProcessProbeExpectInstances
        EgressDeliveryProcessProbeDuration = $EgressDeliveryProcessProbeDuration
        EgressDeliveryProcessProbeInterval = $EgressDeliveryProcessProbeInterval
        EgressOutboxCrashRecoveryGateRun = [bool]$RunEgressOutboxCrashRecoveryGate
        EgressOutboxCrashRecoveryLeaseTimeout = $EgressOutboxCrashRecoveryLeaseTimeout
        EgressOutboxCrashRecoveryStaleAge = $EgressOutboxCrashRecoveryStaleAge
        CoreExecPendingMetricsGateRun = [bool]$RunCoreExecPendingMetricsGate
        MTProtoEdgeProbeGateRun = [bool]$RunMTProtoEdgeProbeGate
        MTProtoEdgeProbeCount = $MTProtoEdgeProbeCount
        MTProtoEdgeProbeObfuscated = [bool]$MTProtoEdgeProbeObfuscated
        MTProtoEdgeAuthProbeGateRun = [bool]$RunMTProtoEdgeAuthProbeGate
        MTProtoEdgeAuthPhonePrefix = $MTProtoEdgeAuthPhonePrefix
        MTProtoEdgeAuthProbeRuns = $MTProtoEdgeAuthProbeRuns
        InjectedBadCoreExecTarget = [bool]$InjectBadCoreExecTarget
        BadCoreExecGRPCAddr = $BadCoreExecGRPCAddr
        CoreExecGRPCTLSEnabled = $CoreExecGRPCTLSEnabled
        CoreExecGRPCMTLSEnabled = $CoreExecGRPCMTLSEnabled
        GeneratedCoreExecGRPCTestCerts = [bool]$GenerateCoreExecGRPCTestCerts
        GeneratedCoreExecGRPCTestCertDir = $GeneratedCoreExecGRPCTestCertDir
        CoreExecGRPCTLSServerName = $CoreExecGRPCTLSServerName
        RedisFabricGateRun = [bool]$RunRedisFabricGate
        RedisAddr = $RedisAddr
        LogSafetyGateRun = [bool]$RunLogSafetyGate
        PortCleanupGateRun = [bool]$RunPortCleanupGate
        EdgeListen = $EdgeListen
        EdgeDebugAddr = $edgeDebugAddr
        CoreLog = $coreStderr
        EgressLog = $egressStderr
        EdgeLog = $edgeStderr
        KeptRunning = [bool]$KeepRunning
    }
} finally {
    Pop-Location
    if (-not $KeepRunning) {
        Stop-SmokeProcesses
        if ($SmokeSucceeded) {
            if ($RunPortCleanupGate) {
                Invoke-PortCleanupGate $script:SmokeCleanupPorts
            }
            if ($RunLogSafetyGate) {
                Invoke-LogSafetyGate $script:SmokeLogPaths
            }
        }
    }
}
