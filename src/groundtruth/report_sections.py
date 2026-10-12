"""Marked sections of docs/data_quality_report.md.

`make profile` writes the report and keeps the quality section; `make quality-report` owns
the section between the two markers and replaces only that.
"""

QUALITY_START = "<!-- quality-report:start -->"
QUALITY_END = "<!-- quality-report:end -->"


def extract_section(text: str) -> str | None:
    """The marked quality section (markers included), or None if absent or malformed."""
    start = text.find(QUALITY_START)
    end = text.find(QUALITY_END)
    if start == -1 or end == -1 or end < start:
        return None
    return text[start : end + len(QUALITY_END)]


def replace_section(text: str, section: str) -> str:
    """``text`` with its quality section replaced by ``section`` (appended if absent)."""
    current = extract_section(text)
    if current is None:
        return text.rstrip("\n") + "\n\n" + section + "\n"
    return text.replace(current, section)


def keep_section(new_text: str, old_text: str) -> str:
    """``new_text`` plus the quality section of ``old_text``, if it had one."""
    section = extract_section(old_text)
    if section is None:
        return new_text
    return replace_section(new_text, section)
