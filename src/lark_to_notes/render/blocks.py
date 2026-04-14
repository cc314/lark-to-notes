"""Machine-owned block marker support for vault-safe note rendering.

Machine-owned blocks use explicit HTML comment delimiters so editors like
Obsidian and standard Markdown renderers pass them through without altering
the surrounding text.

Block format
------------
::

    <!-- ltn:begin id="<block_id>" -->
    ...rendered content...
    <!-- ltn:end id="<block_id>" -->

Rules
-----
* A block ID is a short, URL-safe, stable identifier (e.g. ``task-abc123``).
* The begin/end pair MUST share the same ID.
* Everything between the markers is machine-owned and will be replaced on
  the next render pass.  User text outside the markers is preserved.
* If a begin marker exists with no matching end marker, or the end marker
  precedes the begin marker, the block is considered malformed and an error
  is raised rather than silently clobbering user content.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Marker templates
# ---------------------------------------------------------------------------

_BEGIN_TEMPLATE = '<!-- ltn:begin id="{block_id}" -->'
_END_TEMPLATE = '<!-- ltn:end id="{block_id}" -->'

_BEGIN_RE = re.compile(r'<!-- ltn:begin id="([^"]+)" -->')


def make_begin_marker(block_id: str) -> str:
    """Return the opening marker string for *block_id*."""
    return _BEGIN_TEMPLATE.format(block_id=block_id)


def make_end_marker(block_id: str) -> str:
    """Return the closing marker string for *block_id*."""
    return _END_TEMPLATE.format(block_id=block_id)


def wrap_block(block_id: str, content: str) -> str:
    """Wrap *content* in machine-owned block markers for *block_id*.

    Args:
        block_id: Stable identifier for this block.
        content:  Rendered Markdown content (may span multiple lines).

    Returns:
        The wrapped block including markers and a trailing newline.
    """
    lines = [
        make_begin_marker(block_id),
        content.rstrip("\n"),
        make_end_marker(block_id),
        "",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Block replacement
# ---------------------------------------------------------------------------


class MalformedBlockError(ValueError):
    """Raised when a machine-owned block is malformed in an existing note."""

    def __init__(self, block_id: str, detail: str) -> None:
        super().__init__(f"malformed block {block_id!r}: {detail}")
        self.block_id = block_id
        self.detail = detail


def replace_block(note_text: str, block_id: str, new_content: str) -> str:
    """Replace an existing machine-owned block's content in *note_text*.

    If no block with *block_id* exists in *note_text*, the new block is
    appended at the end of the note (with a leading blank line if needed).

    Args:
        note_text:   Full text of the note file.
        block_id:    Stable block identifier to find and replace.
        new_content: New rendered Markdown content (without markers).

    Returns:
        Updated note text with the block replaced or appended.

    Raises:
        MalformedBlockError: If a begin marker exists without a matching
                             end marker, or end precedes begin.
    """
    begin_marker = make_begin_marker(block_id)
    end_marker = make_end_marker(block_id)

    begin_pos = note_text.find(begin_marker)
    end_pos = note_text.find(end_marker)

    if begin_pos == -1 and end_pos == -1:
        # Block does not exist — append it
        separator = "\n" if note_text.endswith("\n") else "\n\n"
        return note_text + separator + wrap_block(block_id, new_content)

    if begin_pos == -1:
        raise MalformedBlockError(block_id, "end marker found without begin marker")

    if end_pos == -1:
        raise MalformedBlockError(block_id, "begin marker found without end marker")

    if end_pos < begin_pos:
        raise MalformedBlockError(block_id, "end marker precedes begin marker")

    # Replace the entire block (begin marker through end marker inclusive)
    end_of_block = end_pos + len(end_marker)
    new_block = wrap_block(block_id, new_content).rstrip("\n")
    return note_text[:begin_pos] + new_block + note_text[end_of_block:]


def extract_block(note_text: str, block_id: str) -> str | None:
    """Extract the content of a machine-owned block, or return ``None``.

    Args:
        note_text: Full text of the note file.
        block_id:  Stable block identifier to find.

    Returns:
        The content between the begin/end markers (stripped), or ``None``
        if the block does not exist.
    """
    begin_marker = make_begin_marker(block_id)
    end_marker = make_end_marker(block_id)

    begin_pos = note_text.find(begin_marker)
    end_pos = note_text.find(end_marker)

    if begin_pos == -1 or end_pos == -1 or end_pos < begin_pos:
        return None

    start = begin_pos + len(begin_marker)
    return note_text[start:end_pos].strip()


def list_block_ids(note_text: str) -> list[str]:
    """Return all machine-owned block IDs found in *note_text* (in order).

    Args:
        note_text: Full text of the note file.

    Returns:
        List of block IDs whose begin markers appear in the note, in
        document order.  No deduplication is performed; a duplicate ID
        appears twice and indicates a malformed note.
    """
    return _BEGIN_RE.findall(note_text)
