#!/bin/sh
set -eu

umask 077

command_name="${1##*/}"

require_secret() {
  name="$1"
  value="$(printenv "$name" 2>/dev/null || true)"
  normalized="$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]')"
  case "$normalized" in
    ""|*changeme*|*change-me*|*replace-me*|*replace_with_a_random_secret*)
      echo "telesrv: required secret $name is missing or still uses a placeholder" >&2
      exit 64
      ;;
  esac
}

require_value() {
  name="$1"
  value="$(printenv "$name" 2>/dev/null || true)"
  normalized="$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]')"
  case "$normalized" in
    ""|*changeme*|*change-me*|*replace-me*)
      echo "telesrv: required setting $name is missing or still uses a placeholder" >&2
      exit 64
      ;;
  esac
}

require_dsn() {
  require_secret TELESRV_POSTGRES_DSN
  case "$(printf '%s' "$TELESRV_POSTGRES_DSN" | tr '[:upper:]' '[:lower:]')" in
    *changeme*|*replace-me*)
      echo "telesrv: TELESRV_POSTGRES_DSN still contains a placeholder" >&2
      exit 64
      ;;
  esac
}

initialize_edge_key() {
  state_dir="${TELESRV_EDGE_STATE_DIR:-/var/lib/telesrv-edge}"
  private_key="$state_dir/server_rsa.pem"
  public_key="$state_dir/server_rsa.pub"
  identity_mode="$(printf '%s' "${TELESRV_RSA_IDENTITY_MODE:-generated}" | tr '[:upper:]' '[:lower:]')"
  embedded_private_key="/usr/share/telesrv/keys/test-server-rsa.pem.b64"
  embedded_public_key="/usr/share/telesrv/keys/test-server-rsa.pub"
  lock_dir="$state_dir/.rsa-init.lock"
  wait_limit="${TELESRV_RSA_INIT_WAIT_SECONDS:-30}"

  case "$identity_mode" in
    generated) ;;
    test)
      if [ ! -r "$embedded_private_key" ] || [ ! -r "$embedded_public_key" ]; then
        echo "telesrv: test RSA identity requested, but this image does not contain the published test key pair; use the edge-test target or set TELESRV_RSA_IDENTITY_MODE=generated" >&2
        exit 66
      fi
      ;;
    *)
      echo "telesrv: TELESRV_RSA_IDENTITY_MODE must be test or generated" >&2
      exit 64
      ;;
  esac

  case "$wait_limit" in
    ""|0|*[!0-9]*)
      echo "telesrv: TELESRV_RSA_INIT_WAIT_SECONDS must be a positive integer" >&2
      exit 64
      ;;
  esac

  mkdir -p "$state_dir"

  recovered_stale_lock=false
  initialized_identity=false
  while [ ! -f "$private_key" ]; do
    if mkdir "$lock_dir" 2>/dev/null; then
      key_tmp="$state_dir/.server_rsa.pem.$$"
      trap 'rm -f "$key_tmp"; rmdir "$lock_dir" 2>/dev/null || true' EXIT HUP INT TERM
      if [ "$identity_mode" = "test" ]; then
        if ! base64 -d "$embedded_private_key" >"$key_tmp"; then
          echo "telesrv: embedded test RSA private-key fixture is not valid Base64" >&2
          exit 65
        fi
      else
        openssl genrsa -traditional -out "$key_tmp" 2048 >/dev/null 2>&1
      fi
      chmod 0600 "$key_tmp"
      mv "$key_tmp" "$private_key"
      initialized_identity=true
      rmdir "$lock_dir"
      trap - EXIT HUP INT TERM
      break
    else
      attempts=0
      while [ ! -f "$private_key" ] && [ "$attempts" -lt "$wait_limit" ]; do
        attempts=$((attempts + 1))
        sleep 1
      done
      if [ ! -f "$private_key" ]; then
        if [ "$recovered_stale_lock" = false ] && rmdir "$lock_dir" 2>/dev/null; then
          recovered_stale_lock=true
          echo "telesrv: recovered stale MTProto RSA initialization lock" >&2
          continue
        fi
        echo "telesrv: timed out waiting for MTProto RSA key initialization; lock is active or not empty" >&2
        exit 70
      fi
    fi
  done

  if ! openssl rsa -in "$private_key" -check -noout >/dev/null 2>&1; then
    echo "telesrv: $private_key is not a valid PKCS#1 RSA private key" >&2
    exit 65
  fi
  if ! grep -q '^-----BEGIN RSA PRIVATE KEY-----$' "$private_key"; then
    echo "telesrv: $private_key must use PKCS#1 (RSA PRIVATE KEY) PEM encoding" >&2
    exit 65
  fi
  chmod 0600 "$private_key"

  public_tmp="$state_dir/.server_rsa.pub.$$"
  openssl rsa -in "$private_key" -RSAPublicKey_out -out "$public_tmp" >/dev/null 2>&1
  chmod 0644 "$public_tmp"
  mv "$public_tmp" "$public_key"

  if [ "$identity_mode" = "test" ]; then
    if cmp -s "$public_key" "$embedded_public_key"; then
      echo "telesrv: WARNING using the published v2 test RSA identity; its private key is public" >&2
    elif [ "$initialized_identity" = true ]; then
      echo "telesrv: embedded test RSA key pair is inconsistent" >&2
      exit 65
    else
      echo "telesrv: existing edge_state RSA identity differs from the embedded test identity and was preserved" >&2
    fi
  fi
}

case "$command_name" in
  telesrv-migrate)
    require_dsn
    ;;
  telesrv-edge)
    require_value TELESRV_ADVERTISE_IP
    require_value TELESRV_PUBLIC_BASE_URL
    require_value TELESRV_PUBLIC_WEB_BASE_URL
    require_secret TELESRV_REDIS_PASSWORD
    require_secret TELESRV_CORE_EXEC_TOKEN
    require_secret TELESRV_FILE_TOKEN
    require_secret TELESRV_EGRESS_DELIVERY_TOKEN
    initialize_edge_key
    ;;
  telesrv-core)
    require_value TELESRV_ADVERTISE_IP
    require_value TELESRV_PUBLIC_BASE_URL
    require_value TELESRV_PUBLIC_WEB_BASE_URL
    require_dsn
    require_secret TELESRV_REDIS_PASSWORD
    require_secret TELESRV_CORE_EXEC_TOKEN
    require_secret TELESRV_FILE_TOKEN
    require_secret TELESRV_GROUPCALL_CONTROL_TOKEN
    require_secret TELESRV_SFU_CONTROL_TOKEN
    require_secret TELESRV_ADMIN_API_TOKEN
    turn_enable="$(printf '%s' "${TELESRV_TURN_ENABLE:-false}" | tr '[:upper:]' '[:lower:]')"
    case "$turn_enable" in
      true)
        require_value TELESRV_TURN_UDP_PORT
        require_value TELESRV_TURN_ADVERTISE_IP
        require_secret TELESRV_TURN_SECRET
        ;;
      false) ;;
      *)
        echo "telesrv: TELESRV_TURN_ENABLE must be true or false" >&2
        exit 64
        ;;
    esac
    livestream_enable="$(printf '%s' "${TELESRV_LIVESTREAM_ENABLE:-false}" | tr '[:upper:]' '[:lower:]')"
    case "$livestream_enable" in
      true)
        require_value TELESRV_LIVESTREAM_RTMP_PORT
        require_value TELESRV_LIVESTREAM_RTMP_URL
        ;;
      false) ;;
      *)
        echo "telesrv: TELESRV_LIVESTREAM_ENABLE must be true or false" >&2
        exit 64
        ;;
    esac
    phone_delivery="$(printf '%s' "${TELESRV_PHONE_CODE_DELIVERY_PROVIDER:-development}" | tr '[:upper:]' '[:lower:]')"
    case "$phone_delivery" in
      development)
        require_secret TELESRV_DEV_AUTH_CODE
        case "$TELESRV_DEV_AUTH_CODE" in
          [0-9][0-9][0-9][0-9][0-9]) ;;
          [0-9][0-9][0-9][0-9][0-9][0-9]) ;;
          *)
            echo "telesrv: TELESRV_DEV_AUTH_CODE must contain five or six decimal digits" >&2
            exit 64
            ;;
        esac
        allow_insecure="$(printf '%s' "${TELESRV_ALLOW_INSECURE_DEVELOPMENT_AUTH:-false}" | tr '[:upper:]' '[:lower:]')"
        if [ "$allow_insecure" != "true" ]; then
          echo "telesrv: development phone-code delivery requires TELESRV_ALLOW_INSECURE_DEVELOPMENT_AUTH=true; use webhook for public deployment" >&2
          exit 64
        fi
        echo "telesrv: WARNING development phone-code delivery uses one shared code; do not expose it as production authentication" >&2
        ;;
      webhook)
        require_value TELESRV_OTP_WEBHOOK_URL
        require_secret TELESRV_OTP_WEBHOOK_SECRET
        ;;
      *)
        echo "telesrv: unsupported TELESRV_PHONE_CODE_DELIVERY_PROVIDER=$phone_delivery" >&2
        exit 64
        ;;
    esac
    ;;
  telesrv-egress)
    require_value TELESRV_ADVERTISE_IP
    require_value TELESRV_PUBLIC_BASE_URL
    require_dsn
    require_secret TELESRV_REDIS_PASSWORD
    require_secret TELESRV_EGRESS_DELIVERY_TOKEN
    ;;
  telesrv-file)
    require_dsn
    require_secret TELESRV_FILE_TOKEN
    ;;
  telesrv-sfu)
    require_value TELESRV_ADVERTISE_IP
    require_value TELESRV_SFU_BIND_IP
    require_secret TELESRV_REDIS_PASSWORD
    require_secret TELESRV_GROUPCALL_CONTROL_TOKEN
    require_secret TELESRV_SFU_CONTROL_TOKEN
    turn_enable="$(printf '%s' "${TELESRV_TURN_ENABLE:-false}" | tr '[:upper:]' '[:lower:]')"
    case "$turn_enable" in
      true)
        require_value TELESRV_TURN_UDP_PORT
        require_value TELESRV_TURN_ADVERTISE_IP
        require_secret TELESRV_TURN_SECRET
        require_value TELESRV_TURN_RELAY_MIN_PORT
        require_value TELESRV_TURN_RELAY_MAX_PORT
        ;;
      false) ;;
      *)
        echo "telesrv: TELESRV_TURN_ENABLE must be true or false" >&2
        exit 64
        ;;
    esac
    ;;
  telesrv-admin)
    require_dsn
    require_secret TELESRV_ADMIN_API_TOKEN
    require_secret TELESRV_ADMIN_SESSION_KEY
    admin_password="$(printenv TELESRV_ADMIN_UI_PASSWORD 2>/dev/null || true)"
    admin_token="$(printenv TELESRV_ADMIN_UI_TOKEN 2>/dev/null || true)"
    if [ -n "$admin_password" ]; then
      require_secret TELESRV_ADMIN_UI_PASSWORD
    elif [ -n "$admin_token" ]; then
      require_secret TELESRV_ADMIN_UI_TOKEN
    else
      echo "telesrv: TELESRV_ADMIN_UI_PASSWORD or TELESRV_ADMIN_UI_TOKEN is required" >&2
      exit 64
    fi
    ;;
esac

exec "$@"
