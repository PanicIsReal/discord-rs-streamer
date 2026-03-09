#!/usr/bin/env bash
set -euo pipefail

DAEMON_URL="${DISCORD_RS_STREAMER_DAEMON_URL:-http://127.0.0.1:7331}"
VIDEO_SOCKET="${DISCORD_RS_STREAMER_VIDEO_SOCKET:-/tmp/discord-rs-video.sock}"
AUDIO_SOCKET="${DISCORD_RS_STREAMER_AUDIO_SOCKET:-/tmp/discord-rs-audio.sock}"
SOURCE_NAME="${DISCORD_RS_STREAMER_SOURCE_NAME:-neko-live}"
STREAM_KIND="${DISCORD_STREAM_KIND:-go-live}"
DISPLAY_VALUE="${DISPLAY:-:99.0}"
STREAM_FPS_VALUE="${STREAM_FPS:-15}"
STREAM_OUTPUT_WIDTH_VALUE="${STREAM_OUTPUT_WIDTH:-1280}"
STREAM_OUTPUT_HEIGHT_VALUE="${STREAM_OUTPUT_HEIGHT:-720}"
STREAM_KEYFRAME_INTERVAL_SECONDS_VALUE="${STREAM_KEYFRAME_INTERVAL_SECONDS:-2}"
STREAM_BITRATE_VALUE="${STREAM_BITRATE_KBPS:-2500}"
STREAM_AUDIO_BITRATE_VALUE="${STREAM_AUDIO_BITRATE_KBPS:-128}"
STREAM_X264_PRESET_VALUE="${STREAM_X264_PRESET:-ultrafast}"
STREAM_AUDIO_ENABLED_VALUE="${STREAM_AUDIO_ENABLED:-true}"
VIDEO_LOG_LEVEL="${STREAM_FFMPEG_LOGLEVEL:-warning}"
RUST_LOG_VALUE="${RUST_LOG:-info}"

daemon_pid=""
video_pid=""
audio_pid=""

cleanup() {
  local exit_code=$?
  for pid in "${audio_pid}" "${video_pid}" "${daemon_pid}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    fi
  done
  exit "${exit_code}"
}

wait_for_command() {
  local attempts=$1
  local sleep_seconds=$2
  shift 2

  for _ in $(seq 1 "${attempts}"); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep "${sleep_seconds}"
  done

  return 1
}

trap cleanup EXIT INT TERM

required_envs=(
  DISCORD_TOKEN
  DISCORD_GUILD_ID
  DISCORD_CHANNEL_ID
)

for key in "${required_envs[@]}"; do
  if [[ -z "${!key:-}" ]]; then
    echo "missing required environment variable: ${key}" >&2
    exit 1
  fi
done

export RUST_LOG="${RUST_LOG_VALUE}"

/usr/local/bin/discord-rs-streamer-daemon &
daemon_pid=$!

wait_for_command 60 1 curl -sS "${DAEMON_URL}/v1/health"
curl -sS "${DAEMON_URL}/v1/health" >/dev/null

wait_for_command 60 1 ffmpeg -loglevel error -video_size 640x360 -f x11grab -i "${DISPLAY_VALUE}" -frames:v 1 -f null -

wait_for_command 10 2 \
  /usr/local/bin/discord-rs-streamer-cli \
    --daemon-url "${DAEMON_URL}" \
    session connect \
    --token "${DISCORD_TOKEN}" \
    --guild-id "${DISCORD_GUILD_ID}" \
    --channel-id "${DISCORD_CHANNEL_ID}" \
    --stream-kind "${STREAM_KIND}" \
  || true

/usr/local/bin/discord-rs-streamer-cli --daemon-url "${DAEMON_URL}" media connect >/dev/null

/usr/local/bin/discord-rs-streamer-cli \
  --daemon-url "${DAEMON_URL}" \
  stream start \
  --source unix \
  --source-name "${SOURCE_NAME}" \
  --video-socket "${VIDEO_SOCKET}" \
  --audio-socket "${AUDIO_SOCKET}" \
  --ingest-protocol framed >/dev/null

for _ in $(seq 1 60); do
  media_health="$(curl -sS "${DAEMON_URL}/v1/media/health" || true)"
  if [[ "${media_health}" == *'"connection_state":"Connected"'* ]]; then
    break
  fi
  sleep 1
done

media_health="$(curl -sS "${DAEMON_URL}/v1/media/health" || true)"
if [[ "${media_health}" != *'"connection_state":"Connected"'* ]]; then
  echo "media session did not reach connected state: ${media_health}" >&2
  exit 1
fi

KEYFRAME_INTERVAL_FRAMES=$(( STREAM_FPS_VALUE * STREAM_KEYFRAME_INTERVAL_SECONDS_VALUE ))
if (( KEYFRAME_INTERVAL_FRAMES < 1 )); then
  KEYFRAME_INTERVAL_FRAMES=1
fi

ffmpeg \
  -loglevel "${VIDEO_LOG_LEVEL}" \
  -framerate "${STREAM_FPS_VALUE}" \
  -f x11grab -i "${DISPLAY_VALUE}" \
  -an \
  -vf "scale=${STREAM_OUTPUT_WIDTH_VALUE}:${STREAM_OUTPUT_HEIGHT_VALUE}:flags=lanczos,format=yuv420p" \
  -c:v libx264 \
  -preset "${STREAM_X264_PRESET_VALUE}" \
  -tune zerolatency \
  -pix_fmt yuv420p \
  -profile:v baseline \
  -g "${KEYFRAME_INTERVAL_FRAMES}" \
  -keyint_min "${KEYFRAME_INTERVAL_FRAMES}" \
  -sc_threshold 0 \
  -b:v "${STREAM_BITRATE_VALUE}k" \
  -maxrate "${STREAM_BITRATE_VALUE}k" \
  -bufsize "$(( STREAM_BITRATE_VALUE * 2 ))k" \
  -bsf:v h264_metadata=aud=insert \
  -f h264 pipe:1 \
  | /usr/local/bin/discord-rs-streamer-cli \
      --daemon-url "${DAEMON_URL}" \
      ingest send-h264-annex-b \
      --socket "${VIDEO_SOCKET}" &
video_pid=$!

if [[ "${STREAM_AUDIO_ENABLED_VALUE}" == "true" ]]; then
  ffmpeg \
    -loglevel "${VIDEO_LOG_LEVEL}" \
    -f pulse -i audio_output.monitor \
    -vn \
    -c:a libopus \
    -b:a "${STREAM_AUDIO_BITRATE_VALUE}k" \
    -f ogg pipe:1 \
    | /usr/local/bin/discord-rs-streamer-cli \
        --daemon-url "${DAEMON_URL}" \
        ingest send-ogg-opus \
        --socket "${AUDIO_SOCKET}" \
        --input /dev/stdin &
  audio_pid=$!
fi

wait "${daemon_pid}"
