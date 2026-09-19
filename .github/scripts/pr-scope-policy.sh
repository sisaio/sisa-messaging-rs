#!/usr/bin/env bash
# shellcheck disable=SC2034

readonly max_commit_paths=20
readonly max_pr_paths=99
readonly max_pr_commits_from_api=250
readonly allowed_branch_pattern='^(feat|fix|docs|refactor|test|perf|ci|build|chore|release|hotfix)/([1-9][0-9]*)-[a-z0-9]+(-[a-z0-9]+)*$'
readonly conventional_header_pattern='^(build|chore|ci|docs|feat|fix|perf|refactor|revert|style|test)(\([^()]+\))?(!)?: [^[:space:]].*$'

path_count_within_limit() {
  (( $1 <= max_pr_paths ))
}

commit_path_count_within_limit() {
  (( $1 <= max_commit_paths ))
}

commit_count_within_limit() {
  (( $1 <= max_pr_commits_from_api ))
}

has_closing_reference() {
  local body="$1"
  local issue_number="$2"
  local closing_reference_pattern="(^|[^[:alnum:]_])Closes[[:space:]]+#${issue_number}([^[:alnum:]_]|$)"
  grep -Eiq "${closing_reference_pattern}" <<<"${body}"
}

has_refs_reference() {
  local message="$1"
  local issue_number="$2"
  grep -Eq "^[[:space:]]*Refs[[:space:]]+#${issue_number}[[:space:]]*$" <<<"${message}"
}

has_closing_keyword() {
  local message="$1"
  grep -Eiq '(^|[^[:alnum:]_])(Closes|Close|Closed|Fixes|Fix|Fixed|Resolves|Resolve|Resolved)[[:space:]]*:?[[:space:]]+([[:alnum:]_.-]+/[[:alnum:]_.-]+)?#[1-9][0-9]*([^[:alnum:]_]|$)' <<<"${message}"
}

issue_is_open_not_pull_request() {
  local issue_number="$1"
  local expected_number="$2"
  local issue_state="$3"
  local has_pull_request="$4"
  [[ "${issue_number}" == "${expected_number}" && "${issue_state}" == open && "${has_pull_request}" == false ]]
}
