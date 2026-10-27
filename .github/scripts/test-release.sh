#!/usr/bin/env bash
# Exercise real Git tags and mock only GitHub; no network or publishing credentials.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
export GH_REPO=owner/repo GITHUB_REPOSITORY=owner/repo
export GITHUB_REF=refs/heads/main GITHUB_ACTOR=admin GITHUB_TRIGGERING_ACTOR=admin
export STATE="$work/state"
mkdir -p "$work/bin" "$STATE"

fail() { echo "$*" >&2; exit 1; }
reject() {
  if "$@" > "$work/rejected.log" 2>&1; then
    fail "Unexpected success: $*"
  fi
}
git init -q --bare "$work/remote.git"
git init -q -b main "$work/repo"
cd "$work/repo"
git config user.name Test
git config user.email test@example.com
git remote add origin "$work/remote.git"
git commit -q --allow-empty -m initial
echo 1.2.3 > VERSION
git add VERSION
git commit -qm version
RELEASE_COMMIT=$(git rev-parse HEAD)
export RELEASE_COMMIT RELEASE_TAG=v1.2.3
release() { bash "$ROOT/release.sh" "$@"; }

# Only the workflow commands are supported; legacy tag-only calls must fail.
reject release
reject release origin
reject release --allow-non-main
reject release --unknown
reject release --authorize extra
reject release --check-tag extra
reject release --publish extra
test -z "$(git tag --list)"
test -z "$(git --git-dir="$work/remote.git" tag --list)"

# The fake requires pagination flags and returns two pages, including a distractor.
cat > "$work/bin/gh" <<'GH'
#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  api\ */collaborators/*/permission\ --jq\ .permission)
    if [[ "$2" == */writer/permission ]]; then echo write
    elif [[ -f "$STATE/permission-error" ]]; then exit 1
    else echo admin; fi ;;
  api\ */commits/*\ --jq\ .sha) cat "$STATE/commit" ;;
  api\ --paginate\ --slurp\ */releases\?per_page=100)
    [[ ! -f "$STATE/api-error" ]] || exit 1
    if [[ -f "$STATE/stale-list" && -f "$STATE/created" ]]; then
      echo '[[{"id":8,"tag_name":"v0.0.0","draft":false}]]'
      exit 0
    fi
    jq -s '[[{id: 8, tag_name: "v0.0.0", draft: false}], .]' "$STATE/release" ;;
  api\ --paginate\ --slurp\ */assets\?per_page=100)
    jq -s '[[], .]' "$STATE/assets" ;;
  "api --method POST repos/$GH_REPO/releases -f tag_name=$RELEASE_TAG -f target_commitish=$RELEASE_COMMIT -f name=$RELEASE_TAG -F draft=true -F prerelease=false -F generate_release_notes=true")
    echo create >> "$STATE/writes"
    echo '{"id":7,"tag_name":"v1.2.3","draft":true,"prerelease":false}' > "$STATE/release"
    touch "$STATE/created"
    cat "$STATE/release" ;;
  "release upload $RELEASE_TAG --repo $GH_REPO --clobber "*)
    jq -e .draft "$STATE/release" > /dev/null
    echo upload >> "$STATE/writes"
    while [[ "$1" != --clobber ]]; do shift; done
    shift
    : > "$STATE/assets"
    for file; do
      hash=$(sha256sum "$file")
      jq -nc --arg name "${file##*/}" --arg digest "sha256:${hash%% *}" \
        '{name: $name, digest: $digest, state: "uploaded"}' >> "$STATE/assets"
      if [[ -f "$STATE/interrupt" ]]; then rm "$STATE/interrupt"; exit 1; fi
    done
    if [[ -f "$STATE/bad-upload" ]]; then
      jq -c --arg field "$(cat "$STATE/bad-upload")" '.[$field] = null' "$STATE/assets" > "$STATE/changed"
      mv "$STATE/changed" "$STATE/assets"
    fi
    if [[ -f "$STATE/move-tag" ]]; then echo moved > "$STATE/commit"; fi ;;
  "release edit $RELEASE_TAG --repo $GH_REPO --draft=false --prerelease=false")
    echo edit >> "$STATE/writes"
    jq '.draft = false | .prerelease = false' "$STATE/release" > "$STATE/changed"
    mv "$STATE/changed" "$STATE/release" ;;
  *) echo "Unexpected gh arguments: $*" >&2; exit 1 ;;
esac
GH
chmod +x "$work/bin/gh"
export PATH="$work/bin:$PATH"

# Both the original actor and the retrying actor must still be administrators.
bash "$ROOT/release.sh" --authorize
reject env GITHUB_REF=refs/heads/other bash "$ROOT/release.sh" --authorize
grep -Fxq 'Release must be run from main.' "$work/rejected.log"
reject env GITHUB_ACTOR=writer bash "$ROOT/release.sh" --authorize
reject env GITHUB_TRIGGERING_ACTOR=writer bash "$ROOT/release.sh" --authorize
touch "$STATE/permission-error"
reject bash "$ROOT/release.sh" --authorize
rm "$STATE/permission-error"

reset_github() {
  rm -f "$STATE/created"
  : > "$STATE/release"
  : > "$STATE/assets"
  : > "$STATE/writes"
  echo "$RELEASE_COMMIT" > "$STATE/commit"
}
mkdir dist
for arch in amd64 arm64; do
  name="livepeer-$RELEASE_TAG-linux-$arch.tar.gz"
  echo "tested $arch" > "dist/$name"
  (cd dist && sha256sum "$name" > "$name.sha256")
done
reset_github
# The preflight validates the remote tag without creating local or remote state.
release --check-tag
test -z "$(git tag --list)"
test -z "$(git --git-dir="$work/remote.git" tag --list)"
test ! -s "$STATE/writes"

# Mismatched workflow metadata must fail before creating tags or publishing.
reject env RELEASE_TAG=v1.2.4 bash "$ROOT/release.sh" --publish
reject env RELEASE_COMMIT="$(git rev-parse HEAD^)" bash "$ROOT/release.sh" --publish
test -z "$(git tag --list)"
test -z "$(git --git-dir="$work/remote.git" tag --list)"
test ! -s "$STATE/writes"

bundle="dist/livepeer-$RELEASE_TAG-linux-arm64.tar.gz"
cp "$bundle" "$work/original"
echo corrupt > "$bundle"
reject release --publish
rm "$bundle"
reject release --publish
test ! -s "$STATE/writes"
test -z "$(git --git-dir="$work/remote.git" tag --list)"
cp "$work/original" "$bundle"

# A rejected push leaves no remote tag and must stop before GitHub publication.
printf '#!/bin/sh\nexit 1\n' > "$work/remote.git/hooks/pre-receive"
chmod +x "$work/remote.git/hooks/pre-receive"
reject release --publish
grep -Fq 'Push failed; checking whether the intended tag reached the remote.' "$work/rejected.log"
test -z "$(git --git-dir="$work/remote.git" tag --list)"
test ! -s "$STATE/writes"
rm "$work/remote.git/hooks/pre-receive"

# Losing the response after a successful push is safe to recover from.
REAL_GIT=$(command -v git)
export REAL_GIT
cat > "$work/bin/git" <<'GIT'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == push ]]; then
  "$REAL_GIT" "$@"
  exit 1
fi
exec "$REAL_GIT" "$@"
GIT
chmod +x "$work/bin/git"
release --publish > "$work/lost-response.log" 2>&1
rm "$work/bin/git"
grep -Fq 'Push failed; checking whether the intended tag reached the remote.' "$work/lost-response.log"
test "$(git --git-dir="$work/remote.git" rev-parse 'v1.2.3^{commit}')" = "$RELEASE_COMMIT"
test "$(cat "$STATE/writes")" = $'create\nupload\nedit'
release --publish
test "$(cat "$STATE/writes")" = $'create\nupload\nedit' # Published retries write nothing.
cp "$STATE/assets" "$work/published"
cp "$STATE/release" "$work/published-release"

# A tag shared by a draft and a published release cannot be selected safely.
jq -c '., (. + {id: 9, draft: true})' "$work/published-release" > "$STATE/release"
reject release --publish
grep -Fq 'Multiple releases match tag v1.2.3' "$work/rejected.log"
test "$(cat "$STATE/writes")" = $'create\nupload\nedit'

# Matching assets do not make an already-published prerelease a stable release.
jq '.prerelease = true' "$work/published-release" > "$STATE/release"
reject release --publish
grep -Fq 'Published release v1.2.3 must have prerelease=false.' "$work/rejected.log"
test "$(cat "$STATE/writes")" = $'create\nupload\nedit'
jq -e '.draft == false and .prerelease == true' "$STATE/release" > /dev/null
cp "$work/published-release" "$STATE/release"

# New, matching, conflicting, detached, and lightweight tags use a real remote.
release --check-tag
test "$(git --git-dir="$work/remote.git" rev-parse 'v1.2.3^{commit}')" = "$RELEASE_COMMIT"
git commit -q --allow-empty -m advance
reject release --check-tag
RELEASE_COMMIT=$(git rev-parse HEAD) reject release --publish
grep -Fq 'Remote tag v1.2.3 points to' "$work/rejected.log"
test "$(git --git-dir="$work/remote.git" rev-parse 'v1.2.3^{commit}')" = "$RELEASE_COMMIT"
git checkout -q --detach "$RELEASE_COMMIT"
release --publish
git push -q origin :refs/tags/v1.2.3
release --publish # Resume after creating only the local annotated tag.
git push -q origin :refs/tags/v1.2.3
git tag -d v1.2.3 > /dev/null
git tag v1.2.3
release --publish
git checkout -q main
git push -q origin :refs/tags/v1.2.3
RELEASE_COMMIT=$(git rev-parse HEAD) reject release --publish
grep -Fq 'Local tag v1.2.3 points to a different commit' "$work/rejected.log"
git tag -d v1.2.3 > /dev/null
git checkout -q --detach "$RELEASE_COMMIT"
git remote set-url origin "$work/missing"
reject release --check-tag
reject release --publish
test -z "$(git tag --list)"
git remote set-url origin "$work/remote.git"
echo 1.2.4 > VERSION
reject release --publish
git checkout -- VERSION
for version in 01.2.3 v1.2.3 1.2.3-rc1 1.2.3+build; do
  echo "$version" > VERSION
  git commit -qam invalid
  reject release --check-tag
  reject release --publish
done
git checkout -q --detach "$RELEASE_COMMIT"
release --check-tag
test -z "$(git tag --list)"
test -z "$(git --git-dir="$work/remote.git" tag --list)"
release --publish
test "$(cat "$STATE/writes")" = $'create\nupload\nedit'

for failure in missing extra digest; do
  cp "$work/published" "$STATE/assets"
  case "$failure" in
    missing) sed '1d' "$STATE/assets" > "$STATE/changed" ;;
    extra) cat "$STATE/assets" "$STATE/assets" > "$STATE/changed" ;;
    digest) jq -c '.digest = null' "$STATE/assets" > "$STATE/changed" ;;
  esac
  mv "$STATE/changed" "$STATE/assets"
  reject release --publish
  test "$(cat "$STATE/writes")" = $'create\nupload\nedit'
done

reset_github
# Creation must succeed even while the releases list omits the new draft.
touch "$STATE/stale-list"
release --publish
test "$(cat "$STATE/writes")" = $'create\nupload\nedit'
rm "$STATE/stale-list"
reset_github
touch "$STATE/interrupt"
reject release --publish
jq -e .draft "$STATE/release" > /dev/null
release --publish
test "$(cat "$STATE/writes")" = $'create\nupload\nupload\nedit'

for failure in digest state move-tag; do
  reset_github
  if [[ "$failure" == move-tag ]]; then
    touch "$STATE/move-tag"
  else
    echo "$failure" > "$STATE/bad-upload"
  fi
  reject release --publish
  jq -e .draft "$STATE/release" > /dev/null
  test "$(cat "$STATE/writes")" = $'create\nupload'
  rm -f "$STATE/bad-upload" "$STATE/move-tag"
done
reset_github
echo moved > "$STATE/commit"
reject release --publish
test ! -s "$STATE/writes"
reset_github
touch "$STATE/api-error"
reject release --publish
test ! -s "$STATE/writes"
rm "$STATE/api-error"

# Publication needs only HEAD: a shallow checkout can create and push the tag.
git push -q origin "$RELEASE_COMMIT:refs/heads/main" :refs/tags/v1.2.3
git clone -q --depth=1 --no-tags --branch main "file://$work/remote.git" "$work/shallow"
cp -R dist "$work/shallow/dist"
cd "$work/shallow"
git config user.name Test
git config user.email test@example.com
test "$(git rev-parse --is-shallow-repository)" = true
reset_github
release --publish
test "$(cat "$STATE/writes")" = $'create\nupload\nedit'
test "$(git --git-dir="$work/remote.git" rev-parse 'v1.2.3^{commit}')" = "$RELEASE_COMMIT"
echo 'Release tests passed: authorization, Git tags, bundle checks, draft recovery, and immutable assets.'
