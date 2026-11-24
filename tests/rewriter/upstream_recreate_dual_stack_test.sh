#!/bin/bash
# The rewriter proxies only to PostgREST's IPv4 address on a dual-stack Docker network, before
# and after the upstream is recreated at new addresses.
#
# Builds Dockerfile.rewriter from this checkout and runs it on a private IPv6-enabled network
# beside an IPv4-only stand-in for PostgREST (openresty from the same image, listening on
# 0.0.0.0:3000 under the name balance-tracker-postgrest). A connect nginx makes to the
# stand-in's IPv6 address is refused and lands in the rewriter's error log with that address.
#
# Needs a docker daemon. Exits 77 (skipped, the reason on the last line) where there is none or
# it cannot create an IPv6 network; 0 on pass, 1 on failure.
set -euo pipefail
cd "$(dirname "$0")/../.."

SKIPPED=77
REQUESTS=20
ALIAS=balance-tracker-postgrest

skip() { echo "SKIPPED: $1"; exit "$SKIPPED"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

command -v docker > /dev/null || skip "no docker client"
docker info > /dev/null 2>&1 || skip "no docker daemon reachable"

name="btracker-rewriter-dualstack-$$"
image="$name:candidate"
rewriter="$name-rewriter"
upstream="$name-upstream"

cleanup() {
    docker rm --force "$rewriter" "$upstream" > /dev/null 2>&1 || true
    docker network rm "$name" > /dev/null 2>&1 || true
    docker image rm --force "$image" > /dev/null 2>&1 || true
}
trap cleanup EXIT

subnet="fd$(printf '%02x' $((RANDOM % 256))):$(printf '%04x' "$RANDOM"):$(printf '%04x' "$RANDOM")::/64"
if ! err=$(docker network create --ipv6 --subnet "$subnet" "$name" 2>&1 > /dev/null); then
    skip "the docker daemon cannot create an IPv6 network: $err"
fi

docker build --quiet --file Dockerfile.rewriter --tag "$image" . > /dev/null

start_upstream() {
    docker run --detach --name "$upstream" --network "$name" --network-alias "$ALIAS" \
        --entrypoint /bin/sh "$image" -c '
            printf "%s\n" "events {}" "http { server { listen 0.0.0.0:3000;" \
                "location / { default_type application/json; return 200 \"\\\"ok\\\"\"; } } }" \
                > /tmp/stub.conf
            exec /usr/local/openresty/bin/openresty -c /tmp/stub.conf -g "daemon off;"' > /dev/null
    local v6
    v6=$(docker inspect "$upstream" | python3 -c \
        "import json,sys; print(json.load(sys.stdin)[0]['NetworkSettings']['Networks']['$name']['GlobalIPv6Address'])")
    [ -n "$v6" ] || skip "the upstream got no IPv6 address on the IPv6 network"
    echo "upstream IPv6 address: $v6"
}

start_upstream
docker run --detach --name "$rewriter" --network "$name" --publish 127.0.0.1::80 "$image" > /dev/null
url="http://$(docker port "$rewriter" 80/tcp | head -1)"

for _ in $(seq 50); do
    curl --silent --fail --max-time 2 --output /dev/null "$url/version" && break
    sleep 0.2
done

requests() {
    local code
    for _ in $(seq "$REQUESTS"); do
        code=$(curl --silent --max-time 10 --output /dev/null --write-out '%{http_code}' "$url/version" || true)
        [ "$code" = 200 ] || fail "$1: a request through the rewriter answered ${code:-nothing}, not 200"
    done
}

no_ipv6_upstream() {
    local hits
    hits=$(docker logs "$rewriter" 2>&1 | grep -E 'upstream: "http://\[' || true)
    [ -z "$hits" ] || fail "$1: the rewriter connected to an IPv6 upstream address:
$hits"
}

requests "before recreate"
no_ipv6_upstream "before recreate"

docker rm --force "$upstream" > /dev/null
start_upstream
# Past the resolver's valid=5s, so every worker looks the new addresses up.
sleep 6

requests "after recreate"
no_ipv6_upstream "after recreate"

echo "PASS: $((2 * REQUESTS)) requests through the rewriter reached the upstream over IPv4 only"
