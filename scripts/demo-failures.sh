#!/usr/bin/env bash
# End-to-end failure demo (run after `docker compose up -d --build`):
#   udemy goes DOWN -> consumer retries (1s,2s,4s) -> circuit breaker OPENs -> events parked in Redis
#   -> sweeper retries with exponential backoff -> sweepAttempts max out -> DEAD
set -euo pipefail

PLATFORM=${PLATFORM:-udemy}
COUNT=${COUNT:-6}

echo "== resetting mock vendors and Redis"
curl -s -X POST localhost:8090/admin/vendors/reset > /dev/null
curl -s -X DELETE localhost:8082/api/notifications > /dev/null
curl -s -X POST "localhost:8081/api/circuit-breakers/${PLATFORM}/reset" > /dev/null

echo "== ${PLATFORM} -> DOWN (503)"
curl -s -X PUT "localhost:8090/admin/vendors/${PLATFORM}/mode/DOWN"; echo

echo "== sending ${COUNT} events for ${PLATFORM}"
for i in $(seq 1 "$COUNT"); do
  curl -s -X POST localhost:8080/api/v1/payments/notifications \
    -H 'Content-Type: application/json' \
    -d "{\"platformId\":\"${PLATFORM}\",\"transactionId\":\"DEMO-$(date +%s)-${i}\",\"paymentStatus\":\"SUCCESS\"}"
  echo
done

echo
echo "== watching (Ctrl+C to stop). Tip: in another terminal run: docker compose logs -f notification-service sweeper-service"
while true; do
  echo "---------------- $(date +%T)"
  echo -n "circuit breakers: "; curl -s localhost:8081/api/circuit-breakers; echo
  echo -n "redis stats:      "; curl -s localhost:8082/api/notifications/stats; echo
  docker exec redis redis-cli --no-auth-warning ZRANGE notifications:retry-queue 0 -1 WITHSCORES | paste - - | sed 's/^/  retry-queue: /' || true
  for key in $(docker exec redis redis-cli --scan --pattern "notification:${PLATFORM}:*"); do
    docker exec redis redis-cli HMGET "$key" deliveryStatus consumerAttempts sweepAttempts nextRetryAt \
      | paste -sd' ' | sed "s|^|  ${key}: status/consumer/sweeps/next = |"
  done
  sleep 10
done
