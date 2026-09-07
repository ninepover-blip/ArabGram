param(
    [string]$AdvertiseIP = $env:TELESRV_ADVERTISE_IP,
    [string]$EdgeListen = '0.0.0.0:2398',
    [string]$CoreExecGRPCAddr = '127.0.0.1:2440',
    [string]$EgressDeliveryGRPCAddr = '127.0.0.1:2510',
    [string]$FileGRPCAddr = '127.0.0.1:2520',
    [string]$GroupCallControlAddr = '127.0.0.1:2420',
    [string]$GroupCallControlURL = 'http://127.0.0.1:2420',
    [string]$SFUControlGRPCAddr = '127.0.0.1:2450',
    [string]$SFUControlGRPCURL = 'grpc://127.0.0.1:2450',
    [int]$SFUUDPPort = 12399,
    [string]$CoreExecToken = $(if ($env:TELESRV_CORE_EXEC_TOKEN) { $env:TELESRV_CORE_EXEC_TOKEN } else { 'edge-core-smoke' }),
    [string]$EgressDeliveryToken = $(if ($env:TELESRV_EGRESS_DELIVERY_TOKEN) { $env:TELESRV_EGRESS_DELIVERY_TOKEN } else { 'edge-core-smoke-egress' }),
    [string]$FileToken = $(if ($env:TELESRV_FILE_TOKEN) { $env:TELESRV_FILE_TOKEN } else { 'edge-core-smoke-file' }),
    [string]$PostgresDSN = $env:TELESRV_POSTGRES_DSN,
    [string]$PostgresContainer = 'telesrv-postgres',
    [string]$PostgresUser = 'telesrv',
    [int]$CorePostgresMaxConns = 32,
    [int]$EgressPostgresMaxConns = 32,
    [int]$FilePostgresMaxConns = 8,
    [int]$EgressWorkers = 4,
    [string]$CoreDebugAddr = '127.0.0.1:6060',
    [string]$EgressDebugAddr = '127.0.0.1:6061',
    [string]$EdgeDebugAddr = '127.0.0.1:6062',
    [string]$RedisAddr = $env:TELESRV_REDIS_ADDR,
    [string]$RedisPassword = $env:TELESRV_REDIS_PASSWORD,
    [string]$RedisDB = $env:TELESRV_REDIS_DB,
    [string]$RSAKeyPath = $env:TELESRV_RSA_KEY,
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'

$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$runDir = Join-Path $repo 'tmp\edge-core-smoke'
$logDir = Join-Path $runDir 'logs'
New-Item -ItemType Directory -Force -Path $runDir, $logDir | Out-Null

if ([string]::IsNullOrWhiteSpace($PostgresDSN)) {
    & (Join-Path $PSScriptRoot 'ensure-local-databases.ps1') `
        -PostgresContainer $PostgresContainer `
        -DbUser $PostgresUser
    $PostgresDSN = 'postgres://telesrv:telesrv@127.0.0.1:5432/telesrv_v2?sslmode=disable'
    Write-Host '[ok] branch=v2 database=telesrv_v2'
} else {
    Write-Host '[ok] branch=v2 database=explicit-override'
}

function Get-ListenPort([string]$addr) {
    $parts = $addr.Split(':')
    if ($parts.Length -lt 2) {
        throw "invalid listen address: $addr"
    }
    return [int]$parts[-1]
}

function Assert-SingleAddress([string]$name, [string]$addr) {
    if ([string]::IsNullOrWhiteSpace($addr)) {
        throw "$name address is required"
    }
    if ($addr.Contains(',')) {
        throw "$name accepts exactly one address for local real-client startup; use smoke-edge-core-split.ps1 -MultiInstance for multi-instance gates"
    }
}

function Join-Targets([string[]]$addrs) {
    return ($addrs | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }) -join ','
}

function Copy-Env([hashtable]$src) {
    $dst = @{}
    foreach ($key in $src.Keys) {
        $dst[$key] = [string]$src[$key]
    }
    return $dst
}

function Quote-YamlScalar([string]$value) {
    if ($null -eq $value) {
        $value = ''
    }
    return "'" + $value.Replace("'", "''") + "'"
}

function Format-YamlList([string[]]$items, [int]$indent) {
    $pad = ' ' * $indent
    return (($items | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | ForEach-Object {
        "$pad- $(Quote-YamlScalar $_)"
    }) -join "`n")
}

function Write-RoleConfig([string]$path, [string]$content) {
    Set-Content -LiteralPath $path -Value ($content.Trim() + "`n") -Encoding UTF8
    return $path
}

function Start-Role([string]$name, [string]$exe, [hashtable]$env, [string]$suffix) {
    $stdout = Join-Path $logDir "$name-$suffix.out.log"
    $stderr = Join-Path $logDir "$name-$suffix.err.log"
    if (Test-Path $stdout) { Remove-Item -Force $stdout }
    if (Test-Path $stderr) { Remove-Item -Force $stderr }
    $savedEnv = @{}
    try {
        foreach ($key in $env.Keys) {
            $savedEnv[$key] = [Environment]::GetEnvironmentVariable($key, 'Process')
            [Environment]::SetEnvironmentVariable($key, [string]$env[$key], 'Process')
        }
        $proc = Start-Process `
            -FilePath $exe `
            -WorkingDirectory $repo `
            -RedirectStandardOutput $stdout `
            -RedirectStandardError $stderr `
            -WindowStyle Hidden `
            -PassThru
    } finally {
        foreach ($key in $env.Keys) {
            [Environment]::SetEnvironmentVariable($key, $savedEnv[$key], 'Process')
        }
    }
    Write-Host ("Started {0} pid={1} stdout={2} stderr={3}" -f $name, $proc.Id, $stdout, $stderr)
    return $proc
}

function Assert-Alive([System.Diagnostics.Process]$proc, [string]$name) {
    Start-Sleep -Milliseconds 300
    if ($proc.HasExited) {
        throw "$name exited early with code $($proc.ExitCode)"
    }
}

function Wait-Port(
    [string]$hostName,
    [int]$port,
    [int]$timeoutSec = 20,
    [System.Diagnostics.Process]$proc = $null,
    [string]$roleName = 'service'
) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        if ($null -ne $proc -and $proc.HasExited) {
            throw "$roleName exited with code $($proc.ExitCode) while waiting for $hostName`:$port"
        }
        try {
            $client = [System.Net.Sockets.TcpClient]::new()
            $async = $client.BeginConnect($hostName, $port, $null, $null)
            if ($async.AsyncWaitHandle.WaitOne(500)) {
                $client.EndConnect($async)
                $client.Close()
                return
            }
            $client.Close()
        } catch {
        }
        Start-Sleep -Milliseconds 300
    }
    throw "timed out waiting for $hostName`:$port"
}

function Resolve-AdvertiseIP([string]$requested, [object[]]$oldProcesses) {
    if (-not [string]::IsNullOrWhiteSpace($requested)) {
        return $requested
    }
    $currentEdge = $oldProcesses | Where-Object { $_.ProcessName -eq 'telesrv-edge' } | ForEach-Object {
        Get-NetTCPConnection -State Listen -OwningProcess $_.Id -ErrorAction SilentlyContinue |
            Where-Object { $_.LocalPort -eq 2398 -and $_.LocalAddress -notlike '127.*' -and $_.LocalAddress -ne '0.0.0.0' -and $_.LocalAddress -ne '::' } |
            Select-Object -First 1
    } | Select-Object -First 1
    if ($currentEdge) {
        return $currentEdge.LocalAddress
    }
    $candidate = Get-NetIPAddress -AddressFamily IPv4 |
        Where-Object {
            $_.IPAddress -notlike '127.*' -and
            $_.IPAddress -notlike '169.254.*' -and
            $_.InterfaceAlias -notmatch 'Clash|vEthernet|Loopback|VMware|Virtual|Docker|WSL|Hyper-V'
        } |
        Sort-Object IPAddress |
        Select-Object -First 1
    if ($candidate) {
        return $candidate.IPAddress
    }
    return '127.0.0.1'
}

Assert-SingleAddress 'Edge listen' $EdgeListen
Assert-SingleAddress 'CoreExec gRPC' $CoreExecGRPCAddr
Assert-SingleAddress 'Egress Delivery gRPC' $EgressDeliveryGRPCAddr
Assert-SingleAddress 'FileData gRPC' $FileGRPCAddr
Assert-SingleAddress 'GroupCall control' $GroupCallControlAddr
Assert-SingleAddress 'SFU control gRPC' $SFUControlGRPCAddr
if ([string]::IsNullOrWhiteSpace($GroupCallControlURL)) {
    throw 'GroupCall control URL is required'
}
if ([string]::IsNullOrWhiteSpace($SFUControlGRPCURL)) {
    throw 'SFU control gRPC URL is required'
}
if ($SFUUDPPort -le 0 -or $SFUUDPPort -gt 65535) {
    throw 'SFU UDP port must be in 1..65535'
}

$builds = @(
    @{ Name = 'file'; Cmd = '.\cmd\telesrv-file'; Exe = Join-Path $runDir 'telesrv-file.exe'; Next = Join-Path $runDir 'telesrv-file.next.exe' },
    @{ Name = 'core'; Cmd = '.\cmd\telesrv-core'; Exe = Join-Path $runDir 'telesrv-core.exe'; Next = Join-Path $runDir 'telesrv-core.next.exe' },
    @{ Name = 'egress'; Cmd = '.\cmd\telesrv-egress'; Exe = Join-Path $runDir 'telesrv-egress.exe'; Next = Join-Path $runDir 'telesrv-egress.next.exe' },
    @{ Name = 'sfu'; Cmd = '.\cmd\telesrv-sfu'; Exe = Join-Path $runDir 'telesrv-sfu.exe'; Next = Join-Path $runDir 'telesrv-sfu.next.exe' },
    @{ Name = 'edge'; Cmd = '.\cmd\telesrv-edge'; Exe = Join-Path $runDir 'telesrv-edge.exe'; Next = Join-Path $runDir 'telesrv-edge.next.exe' }
)

if (-not $SkipBuild) {
    Write-Host 'Building fresh role binaries...'
    Push-Location $repo
    try {
        foreach ($build in $builds) {
            & go build -o $build.Next $build.Cmd
            if ($LASTEXITCODE -ne 0) {
                throw "go build failed for $($build.Name)"
            }
        }
    } finally {
        Pop-Location
    }
}

$repoFull = [System.IO.Path]::GetFullPath($repo)
$processNames = @('telesrv-file', 'telesrv-core', 'telesrv-egress', 'telesrv-sfu', 'telesrv-edge')
$old = Get-Process -ErrorAction SilentlyContinue | Where-Object {
    $processNames -contains $_.ProcessName -and
    $_.Path -and
    ([System.IO.Path]::GetFullPath($_.Path)).StartsWith($repoFull, [System.StringComparison]::OrdinalIgnoreCase)
}
$AdvertiseIP = Resolve-AdvertiseIP $AdvertiseIP $old

if ($old) {
    $summary = ($old | ForEach-Object { "$($_.ProcessName):$($_.Id)" } | Sort-Object) -join ', '
    Write-Host "Stopping old repo-local telesrv processes: $summary"
    $old | Stop-Process -Force
    Start-Sleep -Seconds 2
}

if (-not $SkipBuild) {
    foreach ($build in $builds) {
        Copy-Item -Force $build.Next $build.Exe
    }
}

$coreTargets = Join-Targets @($CoreExecGRPCAddr)
$egressTargets = Join-Targets @($EgressDeliveryGRPCAddr)
$fileTargets = $FileGRPCAddr
$advertisePort = Get-ListenPort $EdgeListen

$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$configDir = Join-Path $runDir 'configs'
New-Item -ItemType Directory -Force -Path $configDir | Out-Null

$redisAddrValue = $(if ([string]::IsNullOrWhiteSpace($RedisAddr)) { '127.0.0.1:6399' } else { $RedisAddr })
$redisPasswordValue = $(if ($null -eq $RedisPassword) { '' } else { $RedisPassword })
$redisDBValue = $(if ([string]::IsNullOrWhiteSpace($RedisDB)) { '0' } else { $RedisDB })
$rsaKeyValue = $(if ([string]::IsNullOrWhiteSpace($RSAKeyPath)) { 'data/server_rsa.pem' } else { $RSAKeyPath })
$coreTargetYaml = Format-YamlList @($CoreExecGRPCAddr) 4
$egressTargetYaml = Format-YamlList @($EgressDeliveryGRPCAddr) 6
$fileTargetYaml = Format-YamlList @($FileGRPCAddr) 4

$commonEnv = @{
    TELESRV_CORE_EXEC_TOKEN = $CoreExecToken
    TELESRV_EGRESS_DELIVERY_TOKEN = $EgressDeliveryToken
    TELESRV_FILE_TOKEN = $FileToken
    TELESRV_GROUPCALL_CONTROL_TOKEN = 'edge-core-smoke-groupcall'
    TELESRV_SFU_CONTROL_TOKEN = 'edge-core-smoke-sfu'
    TELESRV_POSTGRES_DSN = $(if ([string]::IsNullOrWhiteSpace($PostgresDSN)) { '' } else { $PostgresDSN })
}

$fileEnv = Copy-Env $commonEnv
$fileConfig = Write-RoleConfig (Join-Path $configDir "file-$stamp.yaml") @"
version: 1
instance:
  id: $(Quote-YamlScalar "local-file-$PID")
debug:
  addr: '127.0.0.1:0'
public:
  dc: 2
postgres:
  dsn: '`$`{TELESRV_POSTGRES_DSN`}'
  max_conns: $FilePostgresMaxConns
  min_conns: 1
file_data:
  addr: $(Quote-YamlScalar $FileGRPCAddr)
  token: '`$`{TELESRV_FILE_TOKEN`}'
storage:
  blob_backend: localfs
  blob_dir: data/blobs
  blob_staging_dir: data/blob-staging
  low_space_guard_enabled: true
  min_free_bytes: 1073741824
  max_total_bytes: 0
  usage_refresh_interval: 1m
upload:
  part_ttl: 24h
  part_gc_interval: 5m
  part_gc_batch: 1000
media:
  external_media_enable: true
  web_page_preview_enable: true
"@
$fileEnv['TELESRV_CONFIG'] = $fileConfig
$fileProc = Start-Role 'file' (Join-Path $runDir 'telesrv-file.exe') $fileEnv $stamp
Assert-Alive $fileProc 'file'
Wait-Port '127.0.0.1' (Get-ListenPort $FileGRPCAddr) 30 $fileProc 'file'

$coreEnv = Copy-Env $commonEnv
$coreConfig = Write-RoleConfig (Join-Path $configDir "core-$stamp.yaml") @"
version: 1
instance:
  id: $(Quote-YamlScalar "local-core-$PID")
debug:
  addr: $(Quote-YamlScalar $CoreDebugAddr)
public:
  dc: 2
  default_country_code: CN
  advertise:
    ip: $(Quote-YamlScalar $AdvertiseIP)
    port: $advertisePort
  base_url: https://telesrv.net
  app_scheme: telesrv
  web_base_url: https://weba.telesrv.net
postgres:
  dsn: '`$`{TELESRV_POSTGRES_DSN`}'
  max_conns: $CorePostgresMaxConns
  min_conns: 1
redis:
  addr: $(Quote-YamlScalar $redisAddrValue)
  password: $(Quote-YamlScalar $redisPasswordValue)
  db: $redisDBValue
core_exec:
  addr: $(Quote-YamlScalar $CoreExecGRPCAddr)
  token: '`$`{TELESRV_CORE_EXEC_TOKEN`}'
file_data:
  resolver: static
  targets:
$fileTargetYaml
  token: '`$`{TELESRV_FILE_TOKEN`}'
  timeout: 10s
group_call:
  control_addr: $(Quote-YamlScalar $GroupCallControlAddr)
  control_token: '`$`{TELESRV_GROUPCALL_CONTROL_TOKEN`}'
sfu:
  control:
    token: '`$`{TELESRV_SFU_CONTROL_TOKEN`}'
    timeout: 5s
  owner:
    ttl: 2m
    heartbeat_interval: 30s
    health_timeout: 1s
http:
  bot_api_addr: '127.0.0.1:0'
  admin_api_addr: '127.0.0.1:0'
  admin_api_token: edge-core-smoke-admin
  public_link_web_addr: '127.0.0.1:0'
outbox:
  lease_timeout: 30s
  outbound_push_timeout: 2s
turn:
  enabled: false
livestream:
  enabled: false
translation:
  enabled: false
media:
  external_media_enable: true
  web_page_preview_enable: true
storage:
  blob_backend: localfs
  blob_dir: data/blobs
"@
$coreEnv['TELESRV_CONFIG'] = $coreConfig
$coreProc = Start-Role 'core' (Join-Path $runDir 'telesrv-core.exe') $coreEnv $stamp
Assert-Alive $coreProc 'core'
Wait-Port '127.0.0.1' (Get-ListenPort $CoreExecGRPCAddr) 45 $coreProc 'core'
Wait-Port '127.0.0.1' (Get-ListenPort $GroupCallControlAddr) 30 $coreProc 'core'

$egressEnv = Copy-Env $commonEnv
$egressConfig = Write-RoleConfig (Join-Path $configDir "egress-$stamp.yaml") @"
version: 1
instance:
  id: $(Quote-YamlScalar "local-egress-$PID")
debug:
  addr: $(Quote-YamlScalar $EgressDebugAddr)
public:
  dc: 2
  advertise:
    ip: $(Quote-YamlScalar $AdvertiseIP)
    port: $advertisePort
postgres:
  dsn: '`$`{TELESRV_POSTGRES_DSN`}'
  max_conns: $EgressPostgresMaxConns
  min_conns: 4
redis:
  addr: $(Quote-YamlScalar $redisAddrValue)
  password: $(Quote-YamlScalar $redisPasswordValue)
  db: $redisDBValue
egress:
  delivery_server:
    addr: $(Quote-YamlScalar $EgressDeliveryGRPCAddr)
    token: '`$`{TELESRV_EGRESS_DELIVERY_TOKEN`}'
  workers: $EgressWorkers
  batch: 128
  lease_timeout: 30s
  outbound_push_timeout: 2s
  delivery_attempt_timeout: 2s
  delivery_clock_skew_allowance: 1s
"@
$egressEnv['TELESRV_CONFIG'] = $egressConfig
$egressProc = Start-Role 'egress' (Join-Path $runDir 'telesrv-egress.exe') $egressEnv $stamp
Assert-Alive $egressProc 'egress'
Wait-Port '127.0.0.1' (Get-ListenPort $EgressDeliveryGRPCAddr) 30 $egressProc 'egress'

$sfuEnv = Copy-Env $commonEnv
$sfuConfig = Write-RoleConfig (Join-Path $configDir "sfu-$stamp.yaml") @"
version: 1
instance:
  id: $(Quote-YamlScalar "local-sfu-$PID")
debug:
  addr: '127.0.0.1:0'
public:
  dc: 2
  advertise:
    ip: $(Quote-YamlScalar $AdvertiseIP)
redis:
  addr: $(Quote-YamlScalar $redisAddrValue)
  password: $(Quote-YamlScalar $redisPasswordValue)
  db: $redisDBValue
sfu:
  udp_port: $SFUUDPPort
  advertise_ip: $(Quote-YamlScalar $AdvertiseIP)
  instance_ttl: 90s
  instance_heartbeat_interval: 30s
  max_active_calls: 0
  control:
    addr: $(Quote-YamlScalar $SFUControlGRPCAddr)
    url: $(Quote-YamlScalar $SFUControlGRPCURL)
    token: '`$`{TELESRV_SFU_CONTROL_TOKEN`}'
group_call:
  control_url: $(Quote-YamlScalar $GroupCallControlURL)
  control_token: '`$`{TELESRV_GROUPCALL_CONTROL_TOKEN`}'
"@
$sfuEnv['TELESRV_CONFIG'] = $sfuConfig
$sfuProc = Start-Role 'sfu' (Join-Path $runDir 'telesrv-sfu.exe') $sfuEnv $stamp
Assert-Alive $sfuProc 'sfu'
Wait-Port '127.0.0.1' (Get-ListenPort $SFUControlGRPCAddr) 30 $sfuProc 'sfu'

$edgeEnv = Copy-Env $commonEnv
$edgeConfig = Write-RoleConfig (Join-Path $configDir "edge-$stamp.yaml") @"
version: 1
instance:
  id: $(Quote-YamlScalar "local-edge-$PID")
debug:
  addr: $(Quote-YamlScalar $EdgeDebugAddr)
public:
  dc: 2
  default_country_code: CN
  advertise:
    ip: $(Quote-YamlScalar $AdvertiseIP)
    port: $advertisePort
  base_url: https://telesrv.net
  app_scheme: telesrv
  web_base_url: https://weba.telesrv.net
redis:
  addr: $(Quote-YamlScalar $redisAddrValue)
  password: $(Quote-YamlScalar $redisPasswordValue)
  db: $redisDBValue
edge:
  listen: $(Quote-YamlScalar $EdgeListen)
  rsa_key: $(Quote-YamlScalar $rsaKeyValue)
  websocket:
    enabled: true
    allowed_origins:
      - http://localhost:1234
      - http://127.0.0.1:1234
  location:
    ttl: 90s
    heartbeat_interval: 30s
  auth_key_cache:
    max_entries: 262144
    ttl: 30m
  temp_key_cache:
    max_entries: 262144
    ttl: 30m
core_exec:
  resolver: static
  targets:
$coreTargetYaml
  token: '`$`{TELESRV_CORE_EXEC_TOKEN`}'
  timeout: 5s
file_data:
  resolver: static
  targets:
$fileTargetYaml
  token: '`$`{TELESRV_FILE_TOKEN`}'
  timeout: 10s
egress:
  delivery:
    resolver: static
    targets:
$egressTargetYaml
    token: '`$`{TELESRV_EGRESS_DELIVERY_TOKEN`}'
    timeout: 5s
"@
$edgeEnv['TELESRV_CONFIG'] = $edgeConfig
$edgeProc = Start-Role 'edge' (Join-Path $runDir 'telesrv-edge.exe') $edgeEnv $stamp
Assert-Alive $edgeProc 'edge'
$edgePort = Get-ListenPort $EdgeListen
Wait-Port $AdvertiseIP $edgePort 45 $edgeProc 'edge'
Wait-Port '127.0.0.1' $edgePort 45 $edgeProc 'edge'

Start-Sleep -Seconds 2
$ports = @()
$ports += Get-ListenPort $EdgeListen
$ports += Get-ListenPort $CoreExecGRPCAddr
$ports += Get-ListenPort $EgressDeliveryGRPCAddr
$ports += Get-ListenPort $FileGRPCAddr
$ports += Get-ListenPort $GroupCallControlAddr
$ports += Get-ListenPort $SFUControlGRPCAddr
$ports = $ports | Sort-Object -Unique

$listeners = Get-NetTCPConnection -State Listen -LocalPort $ports -ErrorAction SilentlyContinue |
    Sort-Object LocalPort |
    Select-Object LocalAddress, LocalPort, OwningProcess, @{Name = 'ProcessName'; Expression = { (Get-Process -Id $_.OwningProcess -ErrorAction SilentlyContinue).ProcessName } }

Write-Host ''
Write-Host "AdvertiseIP=$AdvertiseIP"
Write-Host "CoreExec=$coreTargets EgressDelivery=$egressTargets FileData=$fileTargets GroupCallControl=$GroupCallControlURL SFUControl=$SFUControlGRPCURL SFUUDP=$SFUUDPPort"
$listeners | Format-Table -AutoSize

$udp = Get-NetUDPEndpoint -LocalPort $SFUUDPPort -ErrorAction SilentlyContinue |
    Select-Object LocalAddress, LocalPort, OwningProcess, @{Name = 'ProcessName'; Expression = { (Get-Process -Id $_.OwningProcess -ErrorAction SilentlyContinue).ProcessName } }
if ($udp) {
    Write-Host ''
    Write-Host 'UDP listeners:'
    $udp | Format-Table -AutoSize
}

Write-Host ''
Write-Host 'Recent stderr warnings/errors:'
Get-ChildItem $logDir -Filter "*$stamp*.err.log" | ForEach-Object {
    $content = Get-Content $_.FullName -ErrorAction SilentlyContinue |
        Select-String -Pattern '\t(WARN|ERROR|PANIC|FATAL)\t|panic|fatal|failed|deadline exceeded|connection refused|unavailable' -CaseSensitive:$false |
        Select-Object -Last 20
    if ($content) {
        Write-Host "--- $($_.Name)"
        $content
    }
}
