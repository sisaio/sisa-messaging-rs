#!/usr/bin/env bash
# shellcheck disable=SC2016
set -Eeuo pipefail

readonly workflow_file=".github/workflows/pr-scope.yml"
max_pr_paths="$(sed -n 's/^[[:space:]]*readonly max_pr_paths=\([0-9][0-9]*\)$/\1/p' "${workflow_file}")"
readonly max_pr_paths

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

assert_source() {
  local expected="$1"
  grep -Fq -- "${expected}" "${workflow_file}" || fail "missing policy source: ${expected}"
}

assert_path_count() {
  local path_count="$1"
  if ((path_count > max_pr_paths)); then
    return 1
  fi
  return 0
}

[[ "${max_pr_paths}" == 99 ]] || fail "PR path limit is not 99"
assert_path_count 99 || fail "99 paths must be accepted"
if assert_path_count 100; then
  fail "100 paths must be rejected"
fi

assert_source '          readonly max_commit_paths=20'
assert_source '          readonly max_pr_commits_from_api=250'
assert_source '          pr_files="$(api_get "${pr_files_url}?per_page=$((max_pr_paths + 1))&page=1")"'
assert_source '                    (.files | length)'
assert_source '          readonly allowed_branch_pattern='
assert_source '          closing_reference_pattern="(^|[^[:alnum:]_])Closes[[:space:]]+#${branch_issue_number}([^[:alnum:]_]|$)"'
assert_source '              if ! grep -Eq "^[[:space:]]*Refs[[:space:]]+#${branch_issue_number}[[:space:]]*$" <<<"${commit_message}"; then'
assert_source '              if grep -Eiq "(^|[^[:alnum:]_])(Closes|Close|Closed|Fixes|Fix|Fixed|Resolves|Resolve|Resolved)'

printf 'changed-path policy: 99 accepted, 100 rejected; existing guards present\n'
