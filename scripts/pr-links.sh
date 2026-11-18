#!/usr/bin/env bash
set -euo pipefail

repo_url=$(git remote get-url origin | sed -E 's#^git@github\.com:#https://github.com/#; s#\.git$##')

declare -A pr_numbers
while read -r pr_head pr_number; do
    pr_numbers[$pr_head]=$pr_number
done < <(git log --merges --format='%P %s' |
    sed -nE 's/^[0-9a-f]+ ([0-9a-f]+) Merge pull request #([0-9]+) .*/\1 \2/p')

marker='^(.*) <!-- commit:([0-9a-f]+) -->$'
while IFS= read -r line; do
    if [[ $line =~ $marker ]]; then
        entry=${BASH_REMATCH[1]}
        commit=${BASH_REMATCH[2]}
        pr_number=${pr_numbers[$commit]:-}
        if [[ -z $pr_number && $(git log -1 --format=%s "$commit") =~ \(#([0-9]+)\)$ ]]; then
            pr_number=${BASH_REMATCH[1]}
        fi
        entry=$(sed -E 's/ \((#[0-9]+|\[#[0-9]+\]\([^)]*\))\)$//' <<<"$entry")
        if [[ -n $pr_number ]]; then
            entry="$entry ([#$pr_number]($repo_url/pull/$pr_number))"
        fi
        printf '%s\n' "$entry"
    else
        printf '%s\n' "$line"
    fi
done
