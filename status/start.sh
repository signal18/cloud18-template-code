#!/bin/sh
# Start of the status app (Uptime Kuma template): the repman sync runs next to Kuma in the same
# container, restarted when it stops; Kuma is the main process (its exit stops the container).
(while true; do node /sync/kuma-sync.js; echo "kuma-sync exited, restarting in 30s"; sleep 30; done) &
cd /app && exec node server/server.js
