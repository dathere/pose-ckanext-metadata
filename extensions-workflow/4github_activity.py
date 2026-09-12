#!/usr/bin/env python3
"""Collect the GitHub signals the REST crawl does not: pushes, branches, forks,
and each repository's declared ckan.plugins entry points.

Why GraphQL rather than more REST calls: one query carries 25 repositories, each
with its push time, default-branch commit, 30 branches with their commit dates,
the 5 most recently pushed forks, and three packaging files -- for a cost of 1
point. The whole catalogue runs in about 120 points of a 5,000/hour budget,
which is less than 2refresh.py already spends paginating releases.

    GITHUB_TOKEN=... python 4github_activity.py [url_list.csv] [-o github_activity.csv]

Two outputs:
    github_activity.csv   one row per repository
    entry_points.csv      repository_name,plugin -- one row per declared plugin

Private repositories are skipped and counted. A token that can see an
organisation's private repos would otherwise pull them into a public dataset.
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

BATCH = 25

FIELDS = ['repository_name', 'pushed_at', 'default_branch', 'head_committed_at',
          'branches', 'live_branches', 'forks', 'forks_ahead', 'newest_fork',
          'newest_fork_pushed_at', 'forks_ahead_list', 'stars', 'is_archived',
          'is_fork', 'parent']

QUERY = '''  r{i}: repository(owner:"{owner}", name:"{name}") {{
    nameWithOwner isArchived isPrivate isFork stargazerCount forkCount pushedAt
    parent {{ nameWithOwner }}
    defaultBranchRef {{ name target {{ ... on Commit {{ committedDate }} }} }}
    refs(refPrefix:"refs/heads/", first:30) {{
      totalCount nodes {{ name target {{ ... on Commit {{ committedDate }} }} }}
    }}
    forks(first:5, orderBy:{{field:PUSHED_AT, direction:DESC}}) {{
      totalCount nodes {{ nameWithOwner pushedAt isArchived }}
    }}
    pyproject: object(expression:"HEAD:pyproject.toml") {{ ... on Blob {{ text }} }}
    setuppy:   object(expression:"HEAD:setup.py")       {{ ... on Blob {{ text }} }}
    setupcfg:  object(expression:"HEAD:setup.cfg")      {{ ... on Blob {{ text }} }}
  }}'''

# name = "module:Class"  /  name=module:Class
PAIR = re.compile(r'^\s*([A-Za-z_][\w.-]*)\s*=\s*["\']?([\w.]+:[\w.]+)["\']?\s*,?\s*$')
GITHUB = re.compile(r'github\.com/([\w.-]+)/([\w.-]+)', re.I)


def repo_from_url(url):
    m = GITHUB.search(url or '')
    if not m:
        return None
    owner, name = m.group(1), m.group(2)
    return f"{owner}/{name[:-4] if name.lower().endswith('.git') else name}"


# ---------------------------------------------------------------- entry points
def from_toml(text):
    """[project.entry-points."ckan.plugins"] and the poetry spelling of it."""
    out, inside = [], False
    for line in text.splitlines():
        st = line.strip()
        if st.startswith('['):
            inside = 'ckan.plugins' in st and ('entry-points' in st or 'entry_points' in st
                                               or 'plugins' in st)
            continue
        if inside:
            m = PAIR.match(line)
            if m:
                out.append(m.group(1))
    return out


def from_cfg(text):
    """setup.cfg: [options.entry_points] then `ckan.plugins =` and an indented block."""
    out, in_ep, in_ckan = [], False, False
    for line in text.splitlines():
        st = line.strip()
        if st.startswith('['):
            in_ep, in_ckan = 'entry_points' in st, False
            continue
        if not in_ep:
            continue
        if re.match(r'^\s*ckan\.plugins\s*=', line):
            in_ckan = True
            continue
        if in_ckan:
            if st and not line[:1].isspace():
                in_ckan = False
                continue
            m = PAIR.match(line)
            if m:
                out.append(m.group(1))
    return out


def from_setup(text):
    """setup.py, both the '''[ckan.plugins] ...''' block and the dict form."""
    out = []
    for block in re.findall(r'\[ckan\.plugins\](.*?)(?:\'\'\'|"""|\[\w|\Z)', text, re.S):
        out += [m.group(1) for m in (PAIR.match(l) for l in block.splitlines()) if m]
    for block in re.findall(r'["\']ckan\.plugins["\']\s*:\s*\[(.*?)\]', text, re.S):
        for item in re.findall(r'["\']([^"\']+)["\']', block):
            m = PAIR.match(item)
            if m:
                out.append(m.group(1))
    return out


def entry_points(node):
    names = []
    for key, parse in (('pyproject', from_toml), ('setupcfg', from_cfg), ('setuppy', from_setup)):
        blob = node.get(key) or {}
        if blob.get('text'):
            names += parse(blob['text'])
    return sorted({n for n in names if n})


# ---------------------------------------------------------------------- fetch
def run_batch(names):
    query = ['query {']
    for i, full in enumerate(names):
        owner, _, name = full.partition('/')
        query.append(QUERY.format(i=i, owner=owner.replace('"', ''), name=name.replace('"', '')))
    query.append('  rateLimit { cost remaining } }')
    proc = subprocess.run(['gh', 'api', 'graphql', '-f', 'query=' + '\n'.join(query)],
                          capture_output=True, text=True)
    if proc.returncode and not proc.stdout:
        print(f"  ! batch failed: {proc.stderr[:200]}", file=sys.stderr)
        return {}
    try:
        return json.loads(proc.stdout).get('data') or {}
    except json.JSONDecodeError:
        return {}


def age_days(iso, now):
    if not iso:
        return None
    return max((now - datetime.fromisoformat(iso.replace('Z', '+00:00'))).days, 0)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('url_list', nargs='?', default='url_list.csv', type=Path)
    ap.add_argument('-o', '--out', default='github_activity.csv', type=Path)
    ap.add_argument('--entry-points', default='entry_points.csv', type=Path)
    args = ap.parse_args()

    if not args.url_list.exists():
        print(f"✗ {args.url_list} not found — run 1getURL.py first")
        return 1

    with args.url_list.open(newline='', encoding='utf-8') as fh:
        repos = sorted({r for r in (repo_from_url(row.get('github_url', ''))
                                    for row in csv.DictReader(fh)) if r})
    if not repos:
        print(f"✗ no GitHub repositories in {args.url_list}")
        return 1
    print(f"Collecting activity for {len(repos)} repositories")

    now = datetime.now(timezone.utc)
    rows, eps, cost, private, missing = [], [], 0, 0, 0

    for start in range(0, len(repos), BATCH):
        data = run_batch(repos[start:start + BATCH])
        cost += (data.pop('rateLimit', {}) or {}).get('cost', 0)
        for node in data.values():
            if not node:
                missing += 1
                continue
            if node.get('isPrivate'):
                # Never let an org-scoped token leak a private repo into a
                # public dataset.
                private += 1
                continue
            full = node['nameWithOwner']
            refs = node.get('refs') or {}
            branches = refs.get('nodes') or []
            live = sum(1 for b in branches
                       if (age_days((b.get('target') or {}).get('committedDate'), now)
                           or 10 ** 6) < 365)
            push = node.get('pushedAt')
            push_age = age_days(push, now)
            forks = [f for f in ((node.get('forks') or {}).get('nodes') or [])
                     if f.get('pushedAt') and not f.get('isArchived')]
            forks.sort(key=lambda f: f['pushedAt'], reverse=True)
            ahead = [f for f in forks
                     if push_age is not None
                     and (age_days(f['pushedAt'], now) or 0) + 14 < push_age]
            head = (node.get('defaultBranchRef') or {}).get('target') or {}

            rows.append({
                'repository_name': full,
                'pushed_at': (push or '')[:10],
                'default_branch': (node.get('defaultBranchRef') or {}).get('name', ''),
                'head_committed_at': (head.get('committedDate') or '')[:10],
                'branches': refs.get('totalCount', 0),
                'live_branches': live,
                'forks': (node.get('forks') or {}).get('totalCount', node.get('forkCount', 0)),
                'forks_ahead': len(ahead),
                'newest_fork': ahead[0]['nameWithOwner'] if ahead else '',
                'newest_fork_pushed_at': (ahead[0]['pushedAt'][:10] if ahead else ''),
                # Every fork that is ahead, newest first, as name@date. The
                # query asks for five, so this is bounded at five. newest_fork
                # stays for readers that only want the head of the list.
                'forks_ahead_list': '|'.join(
                    f"{f['nameWithOwner']}@{f['pushedAt'][:10]}" for f in ahead),
                'stars': node.get('stargazerCount', 0),
                'is_archived': str(bool(node.get('isArchived'))).lower(),
                'is_fork': str(bool(node.get('isFork'))).lower(),
                'parent': (node.get('parent') or {}).get('nameWithOwner', ''),
            })
            for plugin in entry_points(node):
                eps.append({'repository_name': full, 'plugin': plugin})
        print(f"  {len(rows)}/{len(repos)}", end='\r', file=sys.stderr)
    print(file=sys.stderr)

    with args.out.open('w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: r['repository_name']))
    with args.entry_points.open('w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=['repository_name', 'plugin'])
        writer.writeheader()
        writer.writerows(sorted(eps, key=lambda r: (r['repository_name'], r['plugin'])))

    declaring = len({e['repository_name'] for e in eps})
    print(f"✓ {args.out}: {len(rows)} repositories, {cost} GraphQL points")
    print(f"✓ {args.entry_points}: {len(eps)} entry points from {declaring} repositories")
    if private:
        print(f"  {private} private repositories skipped")
    if missing:
        print(f"  {missing} repositories could not be read (renamed, deleted or blocked)")
    return 0


if __name__ == '__main__':
    sys.exit(main())
