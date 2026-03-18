#!/usr/bin/env python3
# Copyright 2023-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Transform GitHub-generated release notes into a custom format:
- Release date at top
- ## Bug Fixes and ## Improvements sections
- JIRA/ticket identifiers (e.g. [CLIENT-1234]) moved to the end of each line
- Full Changelog link at bottom

Reads raw markdown from stdin; writes formatted markdown to stdout.
"""

import argparse
import re
import sys


# Match ticket IDs like [CLIENT-4385], [AEROSPIKE-123], etc.
TICKET_RE = re.compile(r"\[([A-Z][A-Z0-9]*-[0-9]+)\]")


def move_ticket_to_end(line: str) -> str:
    """Remove ticket from line and append it at the end."""
    tickets = TICKET_RE.findall(line)
    rest = TICKET_RE.sub("", line).strip()
    # Collapse multiple spaces and trim
    rest = re.sub(r"  +", " ", rest).strip()
    if tickets:
        rest = f"{rest} [{tickets[0]}]"
    return rest


def is_bug_fix(title: str) -> bool:
    """True if the change looks like a bug fix (for Bug Fixes section)."""
    t = title.strip().lower()
    return t.startswith("fix ") or t.startswith("fixes ") or t.startswith("bug") or t.startswith("fix:")


def parse_bullet(line: str) -> str | None:
    """
    Extract the bullet text from a line like '* Fix something by @user in https://...'
    Returns None if this doesn't look like a change bullet.
    """
    line = line.strip()
    if not line.startswith("* ") and not line.startswith("- "):
        return None
    text = line[2:].strip()
    # Strip " by @user in URL" suffix (GitHub format)
    by_match = re.search(r"\s+by\s+@[\w-]+\s+in\s+https?://\S+", text, re.IGNORECASE)
    if by_match:
        text = text[: by_match.start()].strip()
    if not text:
        return None
    return text


def main() -> None:
    parser = argparse.ArgumentParser(description="Format release notes.")
    parser.add_argument("--date", required=True, help="Release date (e.g. 'March 17, 2026')")
    parser.add_argument("--changelog-url", default="", help="Full Changelog URL (optional; may be parsed from input)")
    args = parser.parse_args()

    raw = sys.stdin.read()
    bug_fixes: list[str] = []
    improvements: list[str] = []

    changelog_url = args.changelog_url
    for line in raw.splitlines():
        # Capture Full Changelog URL if present
        if "Full Changelog" in line or "full changelog" in line.lower():
            url_match = re.search(r"https://[^\s\)]+", line)
            if url_match and not changelog_url:
                changelog_url = url_match.group(0)
            continue
        bullet = parse_bullet(line)
        if bullet is None:
            continue
        formatted = move_ticket_to_end(bullet)
        if is_bug_fix(formatted):
            bug_fixes.append(formatted)
        else:
            improvements.append(formatted)

    # Build output
    out = [f"Release Date: {args.date}", ""]
    if bug_fixes:
        out.append("## Bug Fixes")
        for item in bug_fixes:
            out.append(f"- {item}")
        out.append("")
    if improvements:
        out.append("## Improvements")
        for item in improvements:
            out.append(f"- {item}")
        out.append("")
    if changelog_url:
        out.append(f"**Full Changelog**: {changelog_url}")

    sys.stdout.write("\n".join(out))
    if out and not out[-1].endswith("\n"):
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
