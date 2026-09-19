#!/usr/bin/env bash
# shellcheck disable=SC2016
set -Eeuo pipefail

readonly workflow_file=".github/workflows/pr-scope.yml"
source .github/scripts/pr-scope-policy.sh

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

assert_source() {
  local expected="$1"
  grep -Fq -- "${expected}" "${workflow_file}" || fail "missing policy source: ${expected}"
}

[[ "${max_pr_paths}" == 99 ]] || fail "PR path limit is not 99"
path_count_within_limit 99 || fail "99 paths must be accepted"
if path_count_within_limit 100; then
  fail "100 paths must be rejected"
fi

commit_path_count_within_limit 20 || fail "20 commit paths must be accepted"
if commit_path_count_within_limit 21; then
  fail "21 commit paths must be rejected"
fi
commit_count_within_limit 250 || fail "250 commits must be accepted"
if commit_count_within_limit 251; then
  fail "251 commits must be rejected"
fi
[[ "ci/35-raise-pr-path-limit" =~ ${allowed_branch_pattern} ]] || fail "valid branch rejected"
[[ "ci/no-issue" =~ ${allowed_branch_pattern} ]] && fail "branch without issue accepted"
has_closing_reference 'Closes #35' 35 || fail "valid closing reference rejected"
has_closing_reference 'Closes #36' 35 && fail "wrong closing reference accepted"
issue_is_open_not_pull_request 35 35 open false || fail "valid issue rejected"
issue_is_open_not_pull_request 35 35 closed false && fail "closed issue accepted"
issue_is_open_not_pull_request 35 35 open true && fail "pull request accepted as issue"
has_refs_reference 'Refs #35' 35 || fail "valid Refs reference rejected"
has_refs_reference 'Closes #35' 35 && fail "closing reference accepted as Refs"
has_closing_keyword 'Closes #35' || fail "closing keyword not detected"
has_closing_keyword 'Refs #35' && fail "Refs reference rejected as closing keyword"
assert_source '          source .github/scripts/pr-scope-policy.sh'
assert_source '          persist-credentials: false'
assert_source '          GH_TOKEN: ${{ github.token }}'

printf 'changed-path policy: 99 accepted, 100 rejected; existing guards present\n'
