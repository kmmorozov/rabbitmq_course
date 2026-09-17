# Run from practice/: source ./env.sh
export API_URL="${API_URL:-http://localhost:15672/api}"
export API_USER="${API_USER:-admin}"
export API_PASS="${API_PASS:-admin-pass}"
export AMQP_URL="${AMQP_URL:-amqp://course:course-pass@localhost:5672/%2Fcourse}"

api() {
  local method="$1" path="$2"
  shift 2
  curl --fail-with-body --silent --show-error \
    --user "$API_USER:$API_PASS" --header 'content-type: application/json' \
    --request "$method" "$API_URL/$path" "$@"
}

exchange() {
  api PUT "exchanges/%2Fcourse/$1" --data "{\"type\":\"$2\",\"durable\":true,\"auto_delete\":false,\"internal\":false,\"arguments\":{}}"
}

queue() {
  local arguments="${2-}"
  if [ -z "$arguments" ]; then arguments='{"x-queue-type":"classic"}'; fi
  api PUT "queues/%2Fcourse/$1" --data "{\"durable\":true,\"auto_delete\":false,\"arguments\":$arguments}"
}

bind() {
  local arguments="${4-}"
  if [ -z "$arguments" ]; then arguments='{}'; fi
  api POST "bindings/%2Fcourse/e/$1/q/$2" --data "{\"routing_key\":\"$3\",\"arguments\":$arguments}"
}

publish() {
  # Python encodes arbitrary payloads safely, including quotes and Unicode.
  local payload
  payload=$(python3 -c 'import json,sys; print(json.dumps({"routing_key":sys.argv[1],"payload":sys.argv[2],"payload_encoding":"string","properties":{"delivery_mode":2}}))' "$2" "$3") || return
  api POST "exchanges/%2Fcourse/$1/publish" --data "$payload" || return
  printf '\n'
}

get() {
  api POST "queues/%2Fcourse/$1/get" --data "{\"count\":${2:-1},\"ackmode\":\"ack_requeue_false\",\"encoding\":\"auto\",\"truncate\":50000}" || return
  printf '\n'
}
