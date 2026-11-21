#!/bin/sh

# Default value for REWRITE_LOG is off, unless explicitly set to 'on'
if [ "$REWRITE_LOG" = "on" ]; then
    REWRITE_LOG="rewrite_log on;"
else
    REWRITE_LOG="# rewrite_log off;"
fi

# An explicit RESOLVER wins; otherwise the container's first nameserver (bracketed when
# IPv6), else Docker's embedded DNS
if [ -z "$RESOLVER" ]; then
    RESOLVER=$(awk '$1 == "nameserver" { print $2; exit }' /etc/resolv.conf 2>/dev/null)
    case "$RESOLVER" in
        *:*) RESOLVER="[$RESOLVER]" ;;
        "") RESOLVER="127.0.0.11" ;;
    esac
fi

# Use sed to replace only the placeholders in the nginx template file, leaving its nginx variables alone
sed -e "s|\${REWRITE_LOG}|$REWRITE_LOG|g" \
    -e "s|\${RESOLVER}|$RESOLVER|g" \
    /usr/local/openresty/nginx/conf/nginx.conf.template > /usr/local/openresty/nginx/conf/nginx.conf

# Start nginx
/usr/local/openresty/bin/openresty -g 'daemon off;'
