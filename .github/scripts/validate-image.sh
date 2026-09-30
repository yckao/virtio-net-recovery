#!/usr/bin/env bash
# PRs only validate; trusted publication supplies the tags to push afterward.
set -euo pipefail
: "${IMAGE:?}" "${EXPECTED_REVISION:?}"
image_id=$(docker image inspect "$IMAGE" --format '{{.Id}}')
[[ "$image_id" =~ ^sha256:[a-f0-9]{64}$ ]]
revision=$(docker image inspect "$image_id" --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}')
test "$revision" = "$EXPECTED_REVISION"
docker run --rm --network none --read-only --cap-drop ALL --security-opt no-new-privileges "$image_id" --help

if [ -z "${PUBLISH_TAGS:-}" ]; then exit 0; fi
: "${IMAGE_NAME:?}"
tags=()
while IFS= read -r tag; do
  [ -n "$tag" ] || continue
  case "$tag" in "$IMAGE_NAME":*) ;; *) echo "Unexpected publication tag" >&2; exit 1 ;; esac
  [[ ! "$tag" =~ [[:space:]] ]]
  tags+=("$tag")
done <<< "$PUBLISH_TAGS"
test "${#tags[@]}" -gt 0
for tag in "${tags[@]}"; do
  # Tag the immutable ID that passed validation, without rebuilding.
  docker tag "$image_id" "$tag"
  test "$(docker image inspect "$tag" --format '{{.Id}}')" = "$image_id"
  docker push "$tag"
done
repo_digests=$(docker image inspect "$image_id" --format '{{range .RepoDigests}}{{println .}}{{end}}')
digest=$(printf '%s\n' "$repo_digests" | awk -v prefix="$IMAGE_NAME@" 'index($0, prefix) == 1 {print substr($0, length(prefix) + 1)}')
[[ "$digest" =~ ^sha256:[a-f0-9]{64}$ ]]
if [ -n "${GITHUB_OUTPUT:-}" ]; then
  printf 'digest=%s\nimage_id=%s\n' "$digest" "$image_id" >> "$GITHUB_OUTPUT"
fi
