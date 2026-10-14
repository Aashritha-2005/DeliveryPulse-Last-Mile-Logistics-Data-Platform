#!/usr/bin/env bash
# Runs only in the isolated one-shot container, as the image's non-root user.
set -euo pipefail

mongod --auth --bind_ip 127.0.0.1 --port 27017 --dbpath /data/db \
    --wiredTigerCacheSizeGB 0.25 --logpath /tmp/bootstrap-mongod.log &
mongo_pid=$!
trap 'kill -TERM "$mongo_pid" 2>/dev/null || true; wait "$mongo_pid" || true' EXIT
ready=0
for attempt in {1..60}; do
    if mongosh --quiet --host 127.0.0.1 --eval 'quit(db.adminCommand({ping:1}).ok ? 0 : 1)' >/dev/null 2>&1; then
        ready=1
        break
    fi
    kill -0 "$mongo_pid" 2>/dev/null || { echo 'ERROR: MongoDB bootstrap server exited'; exit 1; }
    sleep 1
done
if [ "$ready" -ne 1 ]; then
    echo 'ERROR: MongoDB bootstrap server did not become ready'
    exit 1
fi
mongosh --quiet --host 127.0.0.1 --file /bootstrap/users.js
kill -TERM "$mongo_pid"
wait "$mongo_pid"
trap - EXIT
echo 'MongoDB bootstrap: users, collections and indexes ready (values not shown)'
