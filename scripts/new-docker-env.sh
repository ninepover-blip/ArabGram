#!/bin/sh
set -eu
umask 077

usage() {
  cat <<'EOF'
Usage: ./scripts/new-docker-env.sh --advertise-ip IP [options]

Options:
  --public-base-url URL
  --public-web-base-url URL
  --admin-bind-ip IP
  --sfu-host-network       Use direct host networking for SFU (default).
  --sfu-bridge-network     Use Docker UDP publishing as a compatibility fallback.
  --allow-insecure-development-auth
  --output PATH
  --help
EOF
}

die() {
  printf 'new-docker-env: %s\n' "$*" >&2
  exit 1
}

validate_http_url() {
  name=$1
  value=$2
  case "$value" in
    *[[:space:]]*) die "$name must not contain whitespace" ;;
  esac
  case "$value" in
    http://*|https://*) ;;
    *) die "$name must be an absolute HTTP(S) URL" ;;
  esac
  authority=${value#*://}
  authority=${authority%%/*}
  authority=${authority%%\?*}
  authority=${authority%%\#*}
  [ -n "$authority" ] || die "$name must include a host"
  case "$authority" in
    *@*) die "$name must not contain embedded credentials" ;;
  esac
}

url_host() {
  value=$1
  authority=${value#*://}
  authority=${authority%%/*}
  authority=${authority%%\?*}
  authority=${authority%%\#*}
  case "$authority" in
    \[*\]*)
      host=${authority#\[}
      host=${host%%\]*}
      ;;
    *) host=${authority%%:*} ;;
  esac
  printf '%s\n' "$host"
}

validate_ipv4() {
  awk -F. '
    NF != 4 { exit 1 }
    {
      for (i = 1; i <= 4; i++) {
        if ($i !~ /^[0-9]+$/ || $i < 0 || $i > 255) exit 1
      }
    }
  ' <<EOF
$1
EOF
}

validate_ipv6() {
  case "$1" in
    *[!0-9A-Fa-f:.]*|'') return 1 ;;
  esac
  if command -v perl >/dev/null 2>&1; then
    perl -MSocket=AF_INET6,inet_pton -e 'exit inet_pton(AF_INET6, $ARGV[0]) ? 0 : 1' "$1"
    return
  fi
  if command -v python3 >/dev/null 2>&1; then
    python3 -c 'import ipaddress,sys; ipaddress.IPv6Address(sys.argv[1])' "$1"
    return
  fi
  die "validating an IPv6 address requires perl or python3"
}

random_hex() {
  openssl rand -hex "$1"
}

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH='' cd -- "$script_dir/.." && pwd)
template_path="$repo_root/deploy/docker/.env.example"
output_path="$repo_root/deploy/docker/.env"

advertise_ip=
public_base_url=
public_web_base_url=
admin_bind_ip=127.0.0.1
sfu_host_network=true
allow_insecure=false

while [ "$#" -gt 0 ]; do
  case "$1" in
    --advertise-ip)
      [ "$#" -ge 2 ] || die "$1 requires a value"
      advertise_ip=$2
      shift 2
      ;;
    --public-base-url)
      [ "$#" -ge 2 ] || die "$1 requires a value"
      public_base_url=$2
      shift 2
      ;;
    --public-web-base-url)
      [ "$#" -ge 2 ] || die "$1 requires a value"
      public_web_base_url=$2
      shift 2
      ;;
    --admin-bind-ip)
      [ "$#" -ge 2 ] || die "$1 requires a value"
      admin_bind_ip=$2
      shift 2
      ;;
    --sfu-host-network)
      sfu_host_network=true
      shift
      ;;
    --sfu-bridge-network)
      sfu_host_network=false
      shift
      ;;
    --allow-insecure-development-auth)
      allow_insecure=true
      shift
      ;;
    --output)
      [ "$#" -ge 2 ] || die "$1 requires a value"
      output_path=$2
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *) die "unknown argument: $1" ;;
  esac
done

[ -n "$advertise_ip" ] || die "--advertise-ip is required"
[ -f "$template_path" ] || die "Docker environment template not found: $template_path"
[ ! -e "$output_path" ] || die "$output_path already exists; initialization never overwrites live credentials"
command -v openssl >/dev/null 2>&1 || die "openssl is required"

is_ipv6=false
case "$advertise_ip" in
  *:*)
    validate_ipv6 "$advertise_ip" || die "--advertise-ip is not a valid IPv6 address"
    is_ipv6=true
    ;;
  *) validate_ipv4 "$advertise_ip" || die "--advertise-ip is not a valid IPv4 address" ;;
esac

case "$admin_bind_ip" in
  *:*) validate_ipv6 "$admin_bind_ip" || die "--admin-bind-ip is not a valid IPv6 address" ;;
  *) validate_ipv4 "$admin_bind_ip" || die "--admin-bind-ip is not a valid IPv4 address" ;;
esac

is_loopback=false
if [ "$is_ipv6" = true ]; then
  case "$advertise_ip" in ::1|0:0:0:0:0:0:0:1) is_loopback=true ;; esac
else
  case "$advertise_ip" in 127.*) is_loopback=true ;; esac
fi

if [ -z "$public_base_url" ]; then
  [ "$is_loopback" = true ] || die "--public-base-url is required when --advertise-ip is not loopback"
  if [ "$is_ipv6" = true ]; then
    public_base_url="http://[::1]:2401"
  else
    public_base_url="http://127.0.0.1:2401"
  fi
fi
if [ -z "$public_web_base_url" ]; then
  public_web_base_url=$public_base_url
fi
validate_http_url "--public-base-url" "$public_base_url"
validate_http_url "--public-web-base-url" "$public_web_base_url"

public_bind_ip=0.0.0.0
local_bind_ip=127.0.0.1
if [ "$is_ipv6" = true ]; then
  public_bind_ip=::
  local_bind_ip=::1
fi
if [ "$is_loopback" = true ]; then
  public_bind_ip=$advertise_ip
  local_bind_ip=$advertise_ip
elif [ "${public_base_url%%:*}" = "http" ] && [ "$(url_host "$public_base_url")" = "$advertise_ip" ]; then
  local_bind_ip=$advertise_ip
fi

turn_enable=true
turn_advertise_ip=$advertise_ip
turn_bind_ip=127.0.0.1
if [ "$is_ipv6" = true ]; then
  turn_enable=false
  turn_advertise_ip=127.0.0.1
elif [ "$is_loopback" = false ]; then
  turn_bind_ip=0.0.0.0
fi

rtmp_host=$advertise_ip
[ "$is_ipv6" = true ] && rtmp_host="[$rtmp_host]"
rtmp_url="rtmp://$rtmp_host:2400/live"

build_commit=$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || printf unknown)
build_branch=$(git -C "$repo_root" rev-parse --abbrev-ref HEAD 2>/dev/null || printf unknown)
build_tree_state=unknown
if git -C "$repo_root" status --porcelain >/dev/null 2>&1; then
  if [ -n "$(git -C "$repo_root" status --porcelain)" ]; then
    build_tree_state=dirty
  else
    build_tree_state=clean
  fi
fi

POSTGRES_PASSWORD=$(random_hex 24)
TELESRV_BUILD_COMMIT=$build_commit
TELESRV_BUILD_BRANCH=$build_branch
TELESRV_BUILD_TREE_STATE=$build_tree_state
TELESRV_BUILD_DATE=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
TELESRV_POSTGRES_DSN="postgres://telesrv:$POSTGRES_PASSWORD@postgres:5432/telesrv_v2?sslmode=disable"
TELESRV_REDIS_PASSWORD=$(random_hex 32)
TELESRV_CORE_EXEC_TOKEN=$(random_hex 32)
TELESRV_FILE_TOKEN=$(random_hex 32)
TELESRV_EGRESS_DELIVERY_TOKEN=$(random_hex 32)
TELESRV_GROUPCALL_CONTROL_TOKEN=$(random_hex 32)
TELESRV_SFU_CONTROL_TOKEN=$(random_hex 32)
TELESRV_ADMIN_API_TOKEN=$(random_hex 32)
TELESRV_ADMIN_UI_PASSWORD=$(random_hex 24)
TELESRV_ADMIN_SESSION_KEY=$(random_hex 32)
TELESRV_TURN_SECRET=$(random_hex 32)
TELESRV_OTP_WEBHOOK_SECRET=$(random_hex 32)
TELESRV_DEV_AUTH_CODE=12345
TELESRV_ALLOW_INSECURE_DEVELOPMENT_AUTH=$allow_insecure
[ "$is_loopback" = true ] && TELESRV_ALLOW_INSECURE_DEVELOPMENT_AUTH=true
TELESRV_ADVERTISE_IP=$advertise_ip
TELESRV_PUBLIC_BASE_URL=$public_base_url
TELESRV_PUBLIC_WEB_BASE_URL=$public_web_base_url
TELESRV_TURN_ENABLE=$turn_enable
TELESRV_TURN_ADVERTISE_IP=$turn_advertise_ip
TELESRV_TURN_BIND_IP=$turn_bind_ip
TELESRV_SFU_BIND_IP=$turn_bind_ip
TELESRV_LIVESTREAM_RTMP_URL=$rtmp_url
TELESRV_PUBLIC_BIND_IP=$public_bind_ip
TELESRV_LOCAL_BIND_IP=$local_bind_ip
TELESRV_ADMIN_BIND_IP=$admin_bind_ip
TELESRV_SFU_HOST_NETWORK=$sfu_host_network

replacement_keys='TELESRV_BUILD_COMMIT TELESRV_BUILD_BRANCH TELESRV_BUILD_TREE_STATE TELESRV_BUILD_DATE POSTGRES_PASSWORD TELESRV_POSTGRES_DSN TELESRV_REDIS_PASSWORD TELESRV_CORE_EXEC_TOKEN TELESRV_FILE_TOKEN TELESRV_EGRESS_DELIVERY_TOKEN TELESRV_GROUPCALL_CONTROL_TOKEN TELESRV_SFU_CONTROL_TOKEN TELESRV_ADMIN_API_TOKEN TELESRV_ADMIN_UI_PASSWORD TELESRV_ADMIN_SESSION_KEY TELESRV_TURN_SECRET TELESRV_OTP_WEBHOOK_SECRET TELESRV_DEV_AUTH_CODE TELESRV_ALLOW_INSECURE_DEVELOPMENT_AUTH TELESRV_ADVERTISE_IP TELESRV_PUBLIC_BASE_URL TELESRV_PUBLIC_WEB_BASE_URL TELESRV_TURN_ENABLE TELESRV_TURN_ADVERTISE_IP TELESRV_TURN_BIND_IP TELESRV_SFU_BIND_IP TELESRV_LIVESTREAM_RTMP_URL TELESRV_PUBLIC_BIND_IP TELESRV_LOCAL_BIND_IP TELESRV_ADMIN_BIND_IP TELESRV_SFU_HOST_NETWORK'
export POSTGRES_PASSWORD TELESRV_BUILD_COMMIT TELESRV_BUILD_BRANCH TELESRV_BUILD_TREE_STATE TELESRV_BUILD_DATE
export TELESRV_POSTGRES_DSN TELESRV_REDIS_PASSWORD TELESRV_CORE_EXEC_TOKEN TELESRV_FILE_TOKEN
export TELESRV_EGRESS_DELIVERY_TOKEN TELESRV_GROUPCALL_CONTROL_TOKEN TELESRV_SFU_CONTROL_TOKEN
export TELESRV_ADMIN_API_TOKEN TELESRV_ADMIN_UI_PASSWORD TELESRV_ADMIN_SESSION_KEY TELESRV_TURN_SECRET
export TELESRV_OTP_WEBHOOK_SECRET TELESRV_DEV_AUTH_CODE TELESRV_ALLOW_INSECURE_DEVELOPMENT_AUTH
export TELESRV_ADVERTISE_IP TELESRV_PUBLIC_BASE_URL TELESRV_PUBLIC_WEB_BASE_URL TELESRV_TURN_ENABLE
export TELESRV_TURN_ADVERTISE_IP TELESRV_TURN_BIND_IP TELESRV_SFU_BIND_IP TELESRV_LIVESTREAM_RTMP_URL
export TELESRV_PUBLIC_BIND_IP TELESRV_LOCAL_BIND_IP TELESRV_ADMIN_BIND_IP
export TELESRV_SFU_HOST_NETWORK

output_dir=$(dirname -- "$output_path")
[ -d "$output_dir" ] || die "output directory does not exist: $output_dir"
temporary_path=$(mktemp "$output_path.tmp.XXXXXX")
cleanup() {
  [ ! -e "$temporary_path" ] || unlink "$temporary_path"
}
trap cleanup EXIT HUP INT TERM

if ! awk -v keys="$replacement_keys" '
  BEGIN {
    count = split(keys, list, " ")
    for (i = 1; i <= count; i++) wanted[list[i]] = 1
  }
  {
    separator = index($0, "=")
    key = separator ? substr($0, 1, separator - 1) : ""
    if (key in wanted) {
      print key "=" ENVIRON[key]
      seen[key] = 1
    } else {
      print
    }
  }
  END {
    missing = 0
    for (key in wanted) {
      if (!(key in seen)) {
        print "template is missing " key > "/dev/stderr"
        missing = 1
      }
    }
    exit missing
  }
' "$template_path" >"$temporary_path"; then
  die "failed to populate Docker environment template"
fi

chmod 600 "$temporary_path"
if ! ln "$temporary_path" "$output_path"; then
  die "$output_path was created concurrently; existing credentials were preserved"
fi
unlink "$temporary_path"
trap - EXIT HUP INT TERM
printf 'Created %s with mode 0600.\n' "$output_path"
printf 'Review public URLs and authentication delivery settings before startup.\n'
if [ "$sfu_host_network" = true ]; then
  printf 'Next: docker compose --project-directory deploy/docker --env-file %s -f deploy/docker/compose.yaml config --quiet\n' "$output_path"
else
  printf 'Next: docker compose --project-directory deploy/docker --env-file %s -f deploy/docker/compose.yaml -f deploy/docker/compose.sfu-bridge-network.yaml config --quiet\n' "$output_path"
fi
