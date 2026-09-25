#!/bin/sh
# Generate Codex and Claude Code harness configuration from the harness-neutral
# source under .agents/ (roles/*.md, harnesses/*.toml, skills/*).
#
# usage: .agents/sync.sh <codex|claude|all> [--check]
#
# Without --check the generated files are written into the repository.
# With --check they are regenerated into a temporary directory and compared
# with the repository; differing paths are printed and the exit status is 1.
# Requires only POSIX sh, awk, and the usual file utilities.

set -eu

usage() {
  echo "usage: $0 <codex|claude|all> [--check]" >&2
  exit 2
}

script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
roles_dir="$script_dir/roles"
harness_dir="$script_dir/harnesses"
skills_dir="$script_dir/skills"

target=""
check=0
for arg in "$@"; do
  case "$arg" in
    codex | claude | all)
      [ -z "$target" ] || usage
      target="$arg"
      ;;
    --check) check=1 ;;
    *) usage ;;
  esac
done
[ -n "$target" ] || usage

# --- source readers -----------------------------------------------------------

# role_field <role-file> <key>: value of a frontmatter key.
role_field() {
  awk -v key="$2" '
    NR == 1 && $0 == "---" { infm = 1; next }
    infm && $0 == "---" { exit }
    infm && index($0, key ":") == 1 {
      v = substr($0, length(key) + 2)
      sub(/^[ \t]+/, "", v)
      sub(/[ \t]+$/, "", v)
      print v
      exit
    }' "$1"
}

# role_body <role-file>: instruction text after the frontmatter, without
# leading or trailing blank lines.
role_body() {
  awk '
    NR == 1 && $0 == "---" { infm = 1; next }
    infm && $0 == "---" { infm = 0; body = 1; next }
    infm { next }
    body { lines[++n] = $0 }
    END {
      s = 1
      while (s <= n && lines[s] == "") s++
      e = n
      while (e >= s && lines[e] == "") e--
      for (i = s; i <= e; i++) print lines[i]
    }' "$1"
}

# toml_value <toml-file> <table> <key>: unquoted scalar value of key in [table].
# Basic and literal strings are unquoted; escapes are left as written.
toml_value() {
  awk -v table="$2" -v key="$3" '
    /^\[/ { insec = ($0 == "[" table "]"); next }
    insec && index($0, key " =") == 1 {
      v = $0
      sub(/^[^=]*=[ \t]*/, "", v)
      sub(/[ \t]+$/, "", v)
      q = substr(v, 1, 1)
      if (q == "\"" || q == "\047") v = substr(v, 2, length(v) - 2)
      print v
      exit
    }' "$1"
}

# toml_subtables <toml-file> <prefix>: name of every [<prefix>.<name>] table, in file order.
toml_subtables() {
  awk -v prefix="$2." '
    /^\[[^]]+\][ \t]*$/ {
      t = $0
      sub(/^\[/, "", t)
      sub(/\][ \t]*$/, "", t)
      if (index(t, prefix) == 1) print substr(t, length(prefix) + 1)
    }' "$1"
}

# toml_keys <toml-file> <table>: "key<TAB>value" for every key in [table].
toml_keys() {
  awk -v table="$2" '
    /^\[/ { insec = ($0 == "[" table "]"); next }
    insec && /^[A-Za-z0-9_-]+[ \t]*=/ {
      k = $0; sub(/[ \t]*=.*$/, "", k)
      v = $0; sub(/^[^=]*=[ \t]*/, "", v); sub(/[ \t]+$/, "", v)
      if (substr(v, 1, 1) == "\"") v = substr(v, 2, length(v) - 2)
      print k "\t" v
    }' "$1"
}

# toml_strings <toml-file> <table> <key>: one line per string in an array value.
toml_strings() {
  awk -v table="$2" -v key="$3" '
    /^\[/ { insec = ($0 == "[" table "]"); next }
    insec && index($0, key " =") == 1 { collect = 1; sub(/^[^=]*=[ \t]*/, "") }
    collect {
      line = $0
      while (match(line, /"[^"]*"/)) {
        print substr(line, RSTART + 1, RLENGTH - 2)
        line = substr(line, RSTART + RLENGTH)
      }
      if (index($0, "]")) exit
    }' "$1"
}

# toml_escape <string>: backslashes and double quotes escaped for a TOML basic
# string, which is also a valid JSON string body.
toml_escape() {
  printf '%s' "$1" | awk '{ gsub(/\\/, "\\\\"); gsub(/"/, "\\\""); print }'
}

require() {
  [ -n "$2" ] || { echo "sync.sh: $1 is missing in $3" >&2; exit 1; }
}

# --- generators ---------------------------------------------------------------

gen_codex() {
  out="$1"
  harness="$harness_dir/codex.toml"
  mkdir -p "$out/.codex/agents"
  for role in "$roles_dir"/*.md; do
    name=$(role_field "$role" name)
    description=$(role_field "$role" description)
    tier=$(role_field "$role" tier)
    write=$(role_field "$role" write)
    require name "$name" "$role"
    require description "$description" "$role"
    require tier "$tier" "$role"
    model=$(toml_value "$harness" "tiers.$tier" model)
    effort=$(toml_value "$harness" "tiers.$tier" effort)
    require "tiers.$tier.model" "$model" "$harness"
    require "tiers.$tier.effort" "$effort" "$harness"
    case "$write" in
      true) sandbox="workspace-write" ;;
      false) sandbox="read-only" ;;
      *) echo "sync.sh: write must be true or false in $role" >&2; exit 1 ;;
    esac
    kebab=$(printf '%s' "$name" | tr '_' '-')
    {
      printf 'name = "%s"\n' "$(toml_escape "$name")"
      printf 'description = "%s"\n' "$(toml_escape "$description")"
      printf 'model = "%s"\n' "$model"
      printf 'model_reasoning_effort = "%s"\n' "$effort"
      printf 'sandbox_mode = "%s"\n' "$sandbox"
      printf '\ndeveloper_instructions = """\n'
      role_body "$role" | awk '{ gsub(/\\/, "\\\\"); gsub(/"""/, "\"\"\\\""); print }'
      printf '"""\n'
    } >"$out/.codex/agents/$kebab.toml"
  done
  awk '
    /^\[config\]$/ { insec = 1; next }
    /^\[config\./ { insec = 1; sub(/^\[config\./, "["); print; next }
    /^\[/ { insec = 0; next }
    insec { print }' "$harness" |
    awk 'NR == 1 && $0 == "" { next } { print }' >"$out/.codex/config.toml"
}

gen_claude() {
  out="$1"
  harness="$harness_dir/claude.toml"
  mkdir -p "$out/.claude/agents"
  for role in "$roles_dir"/*.md; do
    name=$(role_field "$role" name)
    description=$(role_field "$role" description)
    tier=$(role_field "$role" tier)
    write=$(role_field "$role" write)
    require name "$name" "$role"
    require description "$description" "$role"
    require tier "$tier" "$role"
    model=$(toml_value "$harness" "tiers.$tier" model)
    require "tiers.$tier.model" "$model" "$harness"
    kebab=$(printf '%s' "$name" | tr '_' '-')
    {
      printf -- '---\n'
      printf 'name: %s\n' "$name"
      printf 'description: %s\n' "$description"
      printf 'model: %s\n' "$model"
      case "$write" in
        true) ;;
        false)
          toml_keys "$harness" read_only | while IFS="$(printf '\t')" read -r k v; do
            printf '%s: %s\n' "$k" "$v"
          done
          ;;
        *) echo "sync.sh: write must be true or false in $role" >&2; exit 1 ;;
      esac
      printf -- '---\n\n'
      role_body "$role"
    } >"$out/.claude/agents/$kebab.md"
  done
  events=$(toml_subtables "$harness" settings.hooks)
  {
    printf '{\n  "permissions": {\n    "allow": [\n'
    toml_strings "$harness" settings.permissions allow |
      awk '{ gsub(/\\/, "\\\\"); gsub(/"/, "\\\""); lines[++n] = $0 }
           END { for (i = 1; i <= n; i++) printf "      \"%s\"%s\n", lines[i], (i < n ? "," : "") }'
    printf '    ]\n  }'
    if [ -n "$events" ]; then
      printf ',\n  "hooks": {\n'
      last=$(printf '%s\n' "$events" | tail -n 1)
      for event in $events; do
        table="settings.hooks.$event"
        matcher=$(toml_value "$harness" "$table" matcher)
        command=$(toml_value "$harness" "$table" command)
        timeout=$(toml_value "$harness" "$table" timeout)
        require "$table.matcher" "$matcher" "$harness"
        require "$table.command" "$command" "$harness"
        # toml_value keeps escapes as written and toml_escape adds its own, so a backslash
        # (a basic-string escape) or a raw tab would not survive into valid, equal JSON.
        case "$command" in
          *\\* | *"$(printf '\t')"*)
            echo "sync.sh: $table.command must not contain a backslash or tab in $harness" >&2
            exit 1
            ;;
        esac
        case "$timeout" in
          *[!0-9]*)
            echo "sync.sh: $table.timeout must be a whole number of seconds in $harness" >&2
            exit 1
            ;;
        esac
        printf '    "%s": [\n      {\n' "$event"
        printf '        "matcher": "%s",\n' "$(toml_escape "$matcher")"
        printf '        "hooks": [\n          {\n            "type": "command",\n'
        printf '            "command": "%s"' "$(toml_escape "$command")"
        [ -z "$timeout" ] || printf ',\n            "timeout": %s' "$timeout"
        separator=","
        [ "$event" != "$last" ] || separator=""
        printf '\n          }\n        ]\n      }\n    ]%s\n' "$separator"
      done
      printf '  }'
    fi
    printf '\n}\n'
  } >"$out/.claude/settings.json"
  {
    printf '<!-- Generated by .agents/sync.sh claude; edit .agents/ and re-run instead of this file. -->\n'
    printf '@AGENTS.md\n'
  } >"$out/CLAUDE.md"
}

# stale_skills: .claude/skills entries with no matching .agents/skills/<name>.
stale_skills() {
  [ -d "$repo_root/.claude/skills" ] || return 0
  for dest in "$repo_root/.claude/skills"/* "$repo_root/.claude/skills"/.[!.]*; do
    [ -e "$dest" ] || [ -L "$dest" ] || continue
    name=$(basename "$dest")
    [ -d "$skills_dir/$name" ] || echo "$name"
  done
}

# Link (or copy) each .agents/skills/<name> into .claude/skills/<name> and
# remove destination entries whose source skill no longer exists.
link_skills() {
  mkdir -p "$repo_root/.claude/skills"
  stale_skills | while read -r name; do rm -rf "$repo_root/.claude/skills/$name"; done
  for skill in "$skills_dir"/*/; do
    [ -d "$skill" ] || continue
    name=$(basename "$skill")
    dest="$repo_root/.claude/skills/$name"
    rel="../../.agents/skills/$name"
    if [ -L "$dest" ] && [ "$(readlink "$dest")" = "$rel" ]; then
      continue
    fi
    rm -rf "$dest"
    if ! ln -s "$rel" "$dest" 2>/dev/null; then
      echo "sync.sh: symlink failed for $dest; copying instead" >&2
      cp -R "$skills_dir/$name" "$dest"
    fi
  done
}

# check_skills: report .claude/skills/<name> entries that are neither the
# expected symlink nor an up-to-date copy, and entries with no source skill.
check_skills() {
  stale_skills | while read -r name; do echo ".claude/skills/$name (stale)"; done
  for skill in "$skills_dir"/*/; do
    [ -d "$skill" ] || continue
    name=$(basename "$skill")
    dest="$repo_root/.claude/skills/$name"
    rel="../../.agents/skills/$name"
    if [ -L "$dest" ]; then
      [ "$(readlink "$dest")" = "$rel" ] || echo ".claude/skills/$name"
    elif [ -d "$dest" ]; then
      diff -r "$skills_dir/$name" "$dest" >/dev/null 2>&1 || echo ".claude/skills/$name"
    else
      echo ".claude/skills/$name"
    fi
  done
}

# stale_outputs <dir> <glob-suffix> <out>: files in the repository output
# directory that the generator did not produce.
stale_outputs() {
  [ -d "$repo_root/$1" ] || return 0
  for f in "$repo_root/$1"/*"$2"; do
    [ -e "$f" ] || continue
    [ -e "$3/$1/$(basename "$f")" ] || echo "$1/$(basename "$f")"
  done
}

# --- main ---------------------------------------------------------------------

want_codex=0
want_claude=0
case "$target" in
  codex) want_codex=1 ;;
  claude) want_claude=1 ;;
  all) want_codex=1; want_claude=1 ;;
esac

if [ "$check" -eq 0 ]; then
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/agents-sync.XXXXXX")
  trap 'rm -rf "$tmp"' EXIT INT TERM
  if [ "$want_codex" -eq 1 ]; then
    gen_codex "$tmp"
    stale_outputs .codex/agents .toml "$tmp" | while read -r f; do rm -f "$repo_root/$f"; done
  fi
  if [ "$want_claude" -eq 1 ]; then
    gen_claude "$tmp"
    stale_outputs .claude/agents .md "$tmp" | while read -r f; do rm -f "$repo_root/$f"; done
  fi
  (cd "$tmp" && find . -type f | sed 's|^\./||') | while read -r f; do
    mkdir -p "$repo_root/$(dirname "$f")"
    cp "$tmp/$f" "$repo_root/$f"
  done
  [ "$want_claude" -eq 1 ] && link_skills
  exit 0
fi

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agents-sync.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
[ "$want_codex" -eq 1 ] && gen_codex "$tmp"
[ "$want_claude" -eq 1 ] && gen_claude "$tmp"

# find_diffs: one line per generated path that differs from, or is missing in,
# the repository, plus stale outputs the generator no longer produces.
find_diffs() {
  (cd "$tmp" && find . -type f | sed 's|^\./||' | sort) | while read -r f; do
    if [ ! -f "$repo_root/$f" ] || ! cmp -s "$tmp/$f" "$repo_root/$f"; then
      echo "$f"
    fi
  done
  if [ "$want_codex" -eq 1 ]; then
    stale_outputs .codex/agents .toml "$tmp" | sed 's/$/ (stale)/'
  fi
  if [ "$want_claude" -eq 1 ]; then
    stale_outputs .claude/agents .md "$tmp" | sed 's/$/ (stale)/'
    check_skills
  fi
}

# check_sizes: G6 caps on the files every thread loads. Bytes, not tokens, so
# the check is deterministic; a new rule must displace text, not extend it.
check_sizes() {
  cap() {
    size=$(wc -c <"$repo_root/$1" | tr -d ' ')
    if [ "$size" -gt "$2" ]; then
      echo "$1 ($size bytes exceeds the $2-byte cap)"
    fi
  }
  cap AGENTS.md 6144
  cap docs/agent-workflow.md 8192
  for role in "$repo_root"/.agents/roles/*.md; do
    cap ".agents/roles/$(basename "$role")" 3072
  done
  return 0
}

diffs=$(find_diffs; check_sizes)
if [ -n "$diffs" ]; then
  printf '%s\n' "$diffs"
  echo "sync.sh: generated files differ from .agents/ or a G6 size cap is exceeded; fix the source, then run .agents/sync.sh $target" >&2
  exit 1
fi
exit 0
