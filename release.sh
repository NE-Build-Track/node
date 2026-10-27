#!/usr/bin/env bash

set -euo pipefail

mode=${1:-}
if [[ $# -ne 1 || ( "$mode" != --authorize && "$mode" != --check-tag && "$mode" != --publish ) ]]; then
  echo 'Usage: ./release.sh --authorize|--check-tag|--publish' >&2
  exit 1
fi

# Called inside every release-writing job so partial reruns check the new actor.
if [[ "$mode" == --authorize ]]; then
  test "$GITHUB_REF" = refs/heads/main || {
    echo 'Release must be run from main.' >&2
    exit 1
  }
  for actor in "$GITHUB_ACTOR" "$GITHUB_TRIGGERING_ACTOR"; do
    permission=$(gh api "repos/$GITHUB_REPOSITORY/collaborators/$actor/permission" --jq .permission)
    if [[ "$permission" != admin ]]; then
      echo "$actor must currently have admin permission to release." >&2
      exit 1
    fi
  done
  exit 0
fi

version=$(git show HEAD:VERSION)
if ! [[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "VERSION must be MAJOR.MINOR.PATCH: $version" >&2
  exit 1
fi
if ! git diff --quiet HEAD -- :/VERSION; then
  echo 'Commit VERSION before releasing.' >&2
  exit 1
fi

tag="v$version"
commit=$(git rev-parse HEAD)

if [[ "$mode" == --publish ]]; then
  test "$tag" = "${RELEASE_TAG:?}"
  test "$commit" = "${RELEASE_COMMIT:?}"
  repo=${GH_REPO:?}
  assets=("livepeer-$tag-linux-amd64.tar.gz" "livepeer-$tag-linux-arm64.tar.gz" SHA256SUMS)
  # Check both bundles before creating any remote state.
  expected=$(
    set -e
    cd dist
    : > SHA256SUMS
    for name in "${assets[@]:0:2}"; do
      checksum=$(sha256sum "$name")
      test "$(< "$name.sha256")" = "$checksum" || {
        echo "Checksum mismatch: $name" >&2
        exit 1
      }
      printf '%s\n' "$checksum" >> SHA256SUMS
    done
    sha256sum "${assets[@]}" | jq -Rn '
      [inputs | capture("^(?<digest>[0-9a-f]{64})  (?<name>.*)$") |
        {name, digest: ("sha256:" + .digest), state: "uploaded"}] | sort_by(.name)'
  )
fi

# ls-remote distinguishes an absent tag (2) from a transport/authentication error.
# Prefer the peeled commit of an annotated tag; also accept lightweight tags.
remote_commit() {
  local refs
  refs=$(git ls-remote --exit-code origin "refs/tags/$tag" "refs/tags/$tag^{}") || return $?
  printf '%s\n' "$refs" | awk '
    /\^\{\}$/ { peeled = $1 }
    { direct = $1 }
    END { print peeled ? peeled : direct }'
}
verify_remote() {
  [[ "$1" == "$commit" ]] || {
    echo "Remote tag $tag points to $1, expected $commit; refusing to move it." >&2
    return 1
  }
}

if existing=$(remote_commit); then
  verify_remote "$existing"
else
  status=$?
  [[ "$status" == 2 ]] || exit "$status"
  [[ "$mode" != --check-tag ]] || exit 0
  if git show-ref --verify --quiet "refs/tags/$tag"; then
    [[ "$(git rev-parse "$tag^{commit}")" == "$commit" ]] || {
      echo "Local tag $tag points to a different commit; refusing to move it." >&2
      exit 1
    }
  else
    git tag -a "$tag" -m "Release $tag"
  fi
  # A push may reach the remote even if its response is lost.
  if ! git push origin "refs/tags/$tag:refs/tags/$tag"; then
    echo 'Push failed; checking whether the intended tag reached the remote.' >&2
  fi
  existing=$(remote_commit)
  verify_remote "$existing"
fi

[[ "$mode" == --publish ]] || exit 0
verify_tag() {
  test "$(gh api "repos/$repo/commits/refs/tags/$tag" --jq .sha)" = "$commit"
}
find_release() {
  # Listing includes drafts; the tag endpoint only returns published releases.
  gh api --paginate --slurp "repos/$repo/releases?per_page=100" |
    jq -c --arg tag "$tag" '[.[][] | select(.tag_name == $tag)] |
      if length > 1 then error("Multiple releases match tag " + $tag) else .[0] end'
}
verify_tag
release=$(find_release)
if [[ "$release" == null ]]; then
  # The releases list can lag behind creation. Keep the response so a new draft
  # does not need to appear in that list before we can upload its assets.
  release=$(gh api --method POST "repos/$repo/releases" \
    -f tag_name="$tag" -f target_commitish="$commit" -f name="$tag" \
    -F draft=true -F prerelease=false -F generate_release_notes=true)
fi
id=$(jq -er .id <<< "$release") || {
  echo "Release $tag has no release ID; refusing to publish." >&2
  exit 1
}
if jq -e .draft <<< "$release" > /dev/null; then
  gh release upload "$tag" --repo "$repo" --clobber "${assets[@]/#/dist/}"
elif ! jq -e '.prerelease == false' <<< "$release" > /dev/null; then
  echo "Published release $tag must have prerelease=false." >&2
  exit 1
fi
gh api --paginate --slurp "repos/$repo/releases/$id/assets?per_page=100" |
  jq -e --argjson expected "$expected" '[.[][] | {name, digest, state}] | sort_by(.name) == $expected' > /dev/null || {
    echo 'Release assets differ from the tested bundles.' >&2
    exit 1
  }
verify_tag
if jq -e .draft <<< "$release" > /dev/null; then
  gh release edit "$tag" --repo "$repo" --draft=false --prerelease=false
fi
