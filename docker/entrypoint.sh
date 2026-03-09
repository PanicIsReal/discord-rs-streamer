#!/usr/bin/env bash
set -euo pipefail

require_env() {
  local key=$1
  if [[ -z "${!key:-}" ]]; then
    echo "missing required environment variable: ${key}" >&2
    exit 1
  fi
}

DAEMON_URL="${DISCORD_RS_STREAMER_DAEMON_URL:-http://127.0.0.1:7331}"
DAEMON_BIND="${DISCORD_RS_STREAMER_BIND:-0.0.0.0:7331}"
INPUT_PATH="${DISCORD_RS_STREAMER_INPUT:-}"
X11_DISPLAY="${DISCORD_RS_STREAMER_X11_DISPLAY:-${DISPLAY:-:99.0}}"
PULSE_SOURCE="${DISCORD_RS_STREAMER_PULSE_SOURCE:-audio_output.monitor}"
LOOP="${DISCORD_RS_STREAMER_LOOP:-false}"
STREAM_KIND="${DISCORD_STREAM_KIND:-go-live}"
FFMPEG_BIN="${DISCORD_RS_STREAMER_FFMPEG_BIN:-ffmpeg}"
STREAM_FPS="${DISCORD_RS_STREAMER_FPS:-15}"
STREAM_WIDTH="${DISCORD_RS_STREAMER_WIDTH:-1280}"
STREAM_HEIGHT="${DISCORD_RS_STREAMER_HEIGHT:-720}"
STREAM_KEYFRAME_INTERVAL_SECONDS="${DISCORD_RS_STREAMER_KEYFRAME_INTERVAL_SECONDS:-2}"
VIDEO_BITRATE_KBPS="${DISCORD_RS_STREAMER_VIDEO_BITRATE_KBPS:-2500}"
AUDIO_BITRATE_KBPS="${DISCORD_RS_STREAMER_AUDIO_BITRATE_KBPS:-128}"
X264_PRESET="${DISCORD_RS_STREAMER_X264_PRESET:-ultrafast}"
AUDIO_ENABLED="${DISCORD_RS_STREAMER_AUDIO_ENABLED:-true}"

REQUIRED_ENVS=(
  DISCORD_TOKEN
  DISCORD_GUILD_ID
  DISCORD_CHANNEL_ID
)

for key in "${REQUIRED_ENVS[@]}"; do
  require_env "${key}"
done

args=(
  --daemon-url "${DAEMON_URL}"
  play
  --daemon-bind "${DAEMON_BIND}"
  --stream-kind "${STREAM_KIND}"
  --ffmpeg-bin "${FFMPEG_BIN}"
  --fps "${STREAM_FPS}"
  --width "${STREAM_WIDTH}"
  --height "${STREAM_HEIGHT}"
  --video-bitrate-kbps "${VIDEO_BITRATE_KBPS}"
  --audio-bitrate-kbps "${AUDIO_BITRATE_KBPS}"
  --x264-preset "${X264_PRESET}"
  --keyframe-interval-seconds "${STREAM_KEYFRAME_INTERVAL_SECONDS}"
)

if [[ -n "${INPUT_PATH}" ]]; then
  args+=(--input "${INPUT_PATH}")
  if [[ "${LOOP}" == "true" ]]; then
    args+=(--loop)
  fi
else
  args+=(--x11-display "${X11_DISPLAY}")
fi

if [[ "${AUDIO_ENABLED}" == "true" ]]; then
  if [[ -z "${INPUT_PATH}" ]]; then
    args+=(--pulse-source "${PULSE_SOURCE}")
  fi
else
  args+=(--no-audio)
fi

exec /usr/local/bin/discord-rs-streamer "${args[@]}"
