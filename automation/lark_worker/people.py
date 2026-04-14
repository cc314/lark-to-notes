from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class PersonReference:
    display_name: str
    wikilink: str


def build_people_index(vault_root: Path) -> dict[str, PersonReference]:
    index: dict[str, PersonReference] = {}
    people_dir = vault_root / "people"
    if not people_dir.exists():
        return index

    for note_path in people_dir.glob("*/index.md"):
        display_name = note_path.parent.name
        wikilink = f"[[people/{display_name}/index|{display_name}]]"
        aliases = {display_name}
        aliases.update(_extract_aliases(note_path))
        reference = PersonReference(display_name=display_name, wikilink=wikilink)
        for alias in aliases:
            normalized = alias.strip().casefold()
            if normalized:
                index[normalized] = reference
    return index


def resolve_person(name: str, people_index: dict[str, PersonReference]) -> str | None:
    reference = people_index.get(name.strip().casefold())
    return reference.wikilink if reference else None


def _extract_aliases(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        return set()

    lines = text.splitlines()
    aliases: set[str] = set()
    inside_frontmatter = False
    inside_aliases = False
    for line in lines:
        if line == "---":
            if inside_frontmatter:
                break
            inside_frontmatter = True
            continue
        if not inside_frontmatter:
            continue
        if line.startswith("aliases:"):
            inside_aliases = True
            continue
        if inside_aliases:
            if line.startswith("  - "):
                aliases.add(line[4:].strip().strip('"'))
                continue
            if line.startswith(" ") or not line:
                continue
            inside_aliases = False
    return aliases
