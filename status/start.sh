#!/bin/sh
# Start of the status app (Uptime Kuma template): the repman sync runs next to Kuma in the same
# container, restarted when it stops; Kuma is the main process (its exit stops the container).
#
# SSO (OIDC_ISSUER_URL, OIDC_CLIENT_ID, OIDC_CLIENT_SECRET, SSO_REDIRECT_URL all set): oauth2-proxy
# takes the app port (3001) and sends to the identity provider; Kuma listens on 127.0.0.1:3002
# only, its own login turned off by the sync. Only the emails in $SSO_EMAILS_FILE get in: the
# sync writes there the cluster's users holding cluster-show-routes in replication-manager.
# /api/entry-page stays open for the route monitor. A failed oauth2-proxy download leaves
# nothing on the app port: the page is down, never open.
OAUTH2_PROXY_VERSION=v7.15.5
OAUTH2_PROXY_SHA256=f63f94bf72c5f46ab002a0a275aa8b3cf19b4d828aed08a13978cb9a62c3a1fd

if [ -n "$OIDC_ISSUER_URL" ] && [ -n "$OIDC_CLIENT_ID" ] && [ -n "$OIDC_CLIENT_SECRET" ] && [ -n "$SSO_REDIRECT_URL" ]; then
    export SSO_ENABLED=true
    export SSO_EMAILS_FILE=/tmp/sso/emails
    export UPTIME_KUMA_HOST=127.0.0.1 UPTIME_KUMA_PORT=3002 KUMA_URL=http://127.0.0.1:3002
    mkdir -p /tmp/sso && : > "$SSO_EMAILS_FILE"
    (
        bin=/tmp/sso/oauth2-proxy
        while [ ! -x "$bin" ]; do
            if node /sync/fetch-oauth2-proxy.js "$OAUTH2_PROXY_VERSION" "$OAUTH2_PROXY_SHA256" /tmp/sso; then
                break
            fi
            echo "oauth2-proxy download failed, retrying in 60s"
            sleep 60
        done
        # secrets through the environment, never on the command line
        OAUTH2_PROXY_CLIENT_SECRET="$OIDC_CLIENT_SECRET" \
        OAUTH2_PROXY_COOKIE_SECRET="$(node -e 'process.stdout.write(require("crypto").randomBytes(16).toString("hex"))')"
        export OAUTH2_PROXY_CLIENT_SECRET OAUTH2_PROXY_COOKIE_SECRET
        while true; do
            "$bin" \
                --http-address=0.0.0.0:3001 \
                --upstream=http://127.0.0.1:3002 \
                --provider=keycloak-oidc \
                --oidc-issuer-url="$OIDC_ISSUER_URL" \
                --client-id="$OIDC_CLIENT_ID" \
                --redirect-url="$SSO_REDIRECT_URL" \
                --code-challenge-method=S256 \
                --authenticated-emails-file="$SSO_EMAILS_FILE" \
                --skip-provider-button=true \
                --skip-auth-route='GET=^/api/entry-page$' \
                --reverse-proxy=true \
                --cookie-secure=true \
                --cookie-name=_status_sso
            echo "oauth2-proxy exited, restarting in 10s"
            sleep 10
        done
    ) &
fi

(while true; do node /sync/kuma-sync.js; echo "kuma-sync exited, restarting in 30s"; sleep 30; done) &
cd /app && exec node server/server.js
