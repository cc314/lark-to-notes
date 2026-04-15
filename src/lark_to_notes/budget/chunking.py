"""Text chunking, batch coalescing, and content-hash utilities.

These helpers keep large-document processing and bursty-update batching
within configurable bounds without requiring LLM calls for every chunk.
"""

from __future__ import annotations

import hashlib
import logging

logger = logging.getLogger(__name__)


def chunk_text(text: str, max_chars: int) -> list[str]:
    """Split *text* into chunks of at most *max_chars* characters.

    Splitting prefers newline boundaries when possible to avoid cutting
    mid-sentence.  If *max_chars* is 0 or *text* is shorter than
    *max_chars*, a single-element list is returned.

    Args:
        text:      Input text to split.
        max_chars: Maximum characters per chunk.  ``0`` disables chunking.

    Returns:
        A list of non-empty string chunks.
    """
    if not text:
        return [""]
    if max_chars <= 0 or len(text) <= max_chars:
        return [text]

    chunks: list[str] = []
    start = 0
    total = len(text)

    while start < total:
        end = min(start + max_chars, total)
        if end < total:
            # Try to break at the last newline within the window
            newline_pos = text.rfind("\n", start, end)
            if newline_pos > start:
                end = newline_pos + 1  # include the newline in the preceding chunk
        chunk = text[start:end]
        if chunk:
            chunks.append(chunk)
        start = end

    logger.debug(
        "chunk_text_result",
        extra={"input_len": total, "max_chars": max_chars, "chunk_count": len(chunks)},
    )
    return chunks


def coalesce_batch[T](items: list[T], batch_size: int) -> list[list[T]]:
    """Partition *items* into sub-lists of at most *batch_size* elements.

    Args:
        items:      Items to partition.  The list is not mutated.
        batch_size: Maximum items per batch.  ``0`` returns a single batch
                    containing all items.

    Returns:
        A list of batches.  Each batch is a non-empty list.  Returns
        ``[[]]`` when *items* is empty.
    """
    if not items:
        return [[]]
    if batch_size <= 0:
        return [list(items)]

    batches = [items[i : i + batch_size] for i in range(0, len(items), batch_size)]
    logger.debug(
        "coalesce_batch_result",
        extra={
            "item_count": len(items),
            "batch_size": batch_size,
            "batch_count": len(batches),
        },
    )
    return batches


class ContentHasher:
    """Compute stable content hashes for LLM-result caching.

    The hash is keyed by the UTF-8 encoded text after Unicode NFC
    normalisation and whitespace stripping.  This ensures that trivially
    equivalent inputs share a cache entry.
    """

    def hash(self, text: str) -> str:
        """Return the SHA-256 hex digest of the normalised input.

        Args:
            text: Raw input text.

        Returns:
            64-character lowercase hex string.
        """
        import unicodedata

        normalised = unicodedata.normalize("NFC", text).strip()
        return hashlib.sha256(normalised.encode("utf-8")).hexdigest()
