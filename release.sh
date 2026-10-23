#!/usr/bin/env bash

set -e

remote=${1:?Usage: ./release.sh <remote> [--allow-non-main]}
if [[ $# -gt 2 || ( $# -eq 2 && "$2" != --allow-non-main ) ]]; then
  echo 'Usage: ./release.sh <remote> [--allow-non-main]' >&2
  exit 1
fi

branch=$(git branch --show-current)
if [[ "$branch" != main && "${2:-}" != --allow-non-main ]]; then
  echo "Warning: refusing to release from ${branch:-detached HEAD}; switch to main or pass --allow-non-main." >&2
  exit 1
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

git tag -a "v$version" -m "Release v$version"
git push "$remote" "v$version"
