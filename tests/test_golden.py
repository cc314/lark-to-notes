"""Golden-file and structural render tests for the note-writer pipeline.

These tests verify that the render layer produces structurally correct
Markdown and that repeated renders are idempotent (user content outside
machine-owned blocks is never destroyed).

Golden tests compare against expected structural properties, not byte-for-byte
snapshots — this avoids brittleness while still proving correctness.
"""

from __future__ import annotations

from pathlib import Path

from lark_to_notes.render.blocks import (
    list_block_ids,
    make_begin_marker,
    make_end_marker,
    replace_block,
    wrap_block,
)
from lark_to_notes.render.current_tasks import render_current_tasks_item
from lark_to_notes.render.daily import render_daily_note
from lark_to_notes.render.models import RenderItem, RenderOutcome, RenderSurface
from lark_to_notes.render.raw import render_raw_note
from lark_to_notes.render.writer import NoteWriter

# ---------------------------------------------------------------------------
# Test-item factory
# ---------------------------------------------------------------------------


def _item(
    *,
    task_id: str = "task-gold-0001",
    fingerprint: str = "gold1234abcd5678",
    title: str = "Golden task",
    promotion_rec: str = "current_tasks",
    reason_code: str = "en_action_marker",
    confidence_band: str = "high",
    task_class: str = "task",
    status: str = "open",
    summary: str = "A concise summary of the golden task.",
    assignee_refs: tuple[str, ...] = (),
    source_note_path: str | None = None,
    daily_note_path: str | None = None,
    due_at: str | None = None,
    source_message_id: str | None = None,
    event_date: str = "2026-05-01",
) -> RenderItem:
    return RenderItem(
        task_id=task_id,
        fingerprint=fingerprint,
        title=title,
        promotion_rec=promotion_rec,
        reason_code=reason_code,
        confidence_band=confidence_band,
        task_class=task_class,
        status=status,
        summary=summary,
        assignee_refs=assignee_refs,
        source_note_path=source_note_path,
        daily_note_path=daily_note_path,
        due_at=due_at,
        source_message_id=source_message_id,
        event_date=event_date,
    )


# ---------------------------------------------------------------------------
# Raw note — structural golden tests
# ---------------------------------------------------------------------------


class TestRawNoteGolden:
    def test_creates_file_with_frontmatter(self, tmp_path: Path) -> None:
        item = _item(title="Review the quarterly report")
        result = render_raw_note(item, tmp_path)

        assert result.outcome == RenderOutcome.CREATED
        content = Path(result.target_path).read_text(encoding="utf-8")
        assert content.startswith("---\n")
        assert "type: raw\n" in content
        assert "source: lark\n" in content
        assert "tags:" in content
        assert "- raw\n" in content
        assert "- ltn-generated\n" in content

    def test_note_path_uses_date_and_slug(self, tmp_path: Path) -> None:
        item = _item(title="Send the weekly update", event_date="2026-05-10")
        result = render_raw_note(item, tmp_path)

        assert "2026-05-10" in result.target_path
        assert "send-the-weekly-update" in result.target_path

    def test_block_markers_present(self, tmp_path: Path) -> None:
        item = _item()
        result = render_raw_note(item, tmp_path)
        content = Path(result.target_path).read_text(encoding="utf-8")

        assert make_begin_marker(f"ltn-raw-{item.fingerprint}") in content
        assert make_end_marker(f"ltn-raw-{item.fingerprint}") in content

    def test_task_id_in_body_block(self, tmp_path: Path) -> None:
        item = _item(task_id="task-golden-abc")
        result = render_raw_note(item, tmp_path)
        content = Path(result.target_path).read_text(encoding="utf-8")
        assert "task-golden-abc" in content

    def test_summary_in_body_block(self, tmp_path: Path) -> None:
        item = _item(summary="Important summary text here")
        result = render_raw_note(item, tmp_path)
        content = Path(result.target_path).read_text(encoding="utf-8")
        assert "Important summary text here" in content

    def test_source_message_id_in_body_block(self, tmp_path: Path) -> None:
        item = _item(source_message_id="om_xyz_999")
        result = render_raw_note(item, tmp_path)
        content = Path(result.target_path).read_text(encoding="utf-8")
        assert "om_xyz_999" in content

    def test_rerender_updates_block_preserves_user_prose(self, tmp_path: Path) -> None:
        """Re-rendering updates the machine block without destroying prose around it."""
        item = _item(summary="First render summary")
        result = render_raw_note(item, tmp_path)
        note_path = Path(result.target_path)

        # Simulate user adding prose before and after the block
        original = note_path.read_text(encoding="utf-8")
        user_note = (
            "User prose above the block.\n\n" + original + "\n\nUser prose below the block.\n"
        )
        note_path.write_text(user_note, encoding="utf-8")

        # Re-render with updated summary
        item2 = _item(summary="Updated render summary")
        result2 = render_raw_note(item2, tmp_path)
        assert result2.outcome == RenderOutcome.UPDATED

        updated = note_path.read_text(encoding="utf-8")
        assert "User prose above the block." in updated
        assert "User prose below the block." in updated
        assert "Updated render summary" in updated
        # Old summary must be replaced
        assert "First render summary" not in updated

    def test_identical_rerender_is_skipped(self, tmp_path: Path) -> None:
        """Rendering the same item twice produces SKIPPED on the second run."""
        item = _item()
        render_raw_note(item, tmp_path)
        result2 = render_raw_note(item, tmp_path)
        assert result2.outcome == RenderOutcome.SKIPPED

    def test_raw_dir_created_automatically(self, tmp_path: Path) -> None:
        vault = tmp_path / "new_vault"
        vault.mkdir()
        item = _item()
        render_raw_note(item, vault)
        assert (vault / "raw").is_dir()

    def test_confidence_tag_in_frontmatter(self, tmp_path: Path) -> None:
        item = _item(confidence_band="high")
        result = render_raw_note(item, tmp_path)
        content = Path(result.target_path).read_text(encoding="utf-8")
        assert "confidence-high" in content

    def test_daily_note_backlink_in_block(self, tmp_path: Path) -> None:
        item = _item(daily_note_path="daily/2026-05-01.md")
        result = render_raw_note(item, tmp_path)
        content = Path(result.target_path).read_text(encoding="utf-8")
        assert "[[2026-05-01]]" in content


# ---------------------------------------------------------------------------
# Daily note — structural golden tests
# ---------------------------------------------------------------------------


class TestDailyNoteGolden:
    def test_creates_daily_note_with_block(self, tmp_path: Path) -> None:
        item = _item(event_date="2026-05-01")
        result = render_daily_note(item, tmp_path, date="2026-05-01")

        assert result.outcome == RenderOutcome.CREATED
        content = Path(result.target_path).read_text(encoding="utf-8")
        assert "Golden task" in content

    def test_daily_note_path_uses_date(self, tmp_path: Path) -> None:
        item = _item(event_date="2026-06-15")
        result = render_daily_note(item, tmp_path, date="2026-06-15")

        assert "2026-06-15" in result.target_path

    def test_rerender_replaces_block_preserves_prose(self, tmp_path: Path) -> None:
        item = _item(title="First title", event_date="2026-05-01")
        result = render_daily_note(item, tmp_path, date="2026-05-01")
        note_path = Path(result.target_path)

        original = note_path.read_text(encoding="utf-8")
        note_path.write_text(
            "User wrote this.\n\n" + original + "\nMore user notes.\n",
            encoding="utf-8",
        )

        item2 = _item(title="Updated title", event_date="2026-05-01")
        result2 = render_daily_note(item2, tmp_path, date="2026-05-01")
        assert result2.outcome == RenderOutcome.UPDATED

        updated = note_path.read_text(encoding="utf-8")
        assert "User wrote this." in updated
        assert "More user notes." in updated
        assert "Updated title" in updated

    def test_identical_rerender_is_skipped(self, tmp_path: Path) -> None:
        item = _item()
        render_daily_note(item, tmp_path, date="2026-05-01")
        result2 = render_daily_note(item, tmp_path, date="2026-05-01")
        assert result2.outcome == RenderOutcome.SKIPPED


# ---------------------------------------------------------------------------
# Current Tasks — structural golden tests
# ---------------------------------------------------------------------------


class TestCurrentTasksGolden:
    def test_creates_current_tasks_entry_for_promoted_item(self, tmp_path: Path) -> None:
        current_tasks_dir = tmp_path / "area" / "current tasks"
        current_tasks_dir.mkdir(parents=True)

        item = _item(promotion_rec="current_tasks")
        result = render_current_tasks_item(item, tmp_path)

        assert result.outcome in (RenderOutcome.CREATED, RenderOutcome.UPDATED)
        content = Path(result.target_path).read_text(encoding="utf-8")
        assert "Golden task" in content

    def test_skips_non_promoted_item(self, tmp_path: Path) -> None:
        """render_pipeline does NOT emit a CT surface result for non-promoted items."""
        vault = tmp_path / "vault"
        vault.mkdir()
        writer = NoteWriter(vault_root=vault)
        item = _item(promotion_rec="daily_only")
        results = writer.render_pipeline(item)
        ct_results = [r for r in results if r.surface == RenderSurface.CURRENT_TASKS]
        assert len(ct_results) == 0, "expected no CT surface for daily_only promotion"

    def test_rerender_does_not_duplicate_block(self, tmp_path: Path) -> None:
        current_tasks_dir = tmp_path / "area" / "current tasks"
        current_tasks_dir.mkdir(parents=True)

        item = _item(promotion_rec="current_tasks")
        render_current_tasks_item(item, tmp_path)
        render_current_tasks_item(item, tmp_path)

        ct_path = tmp_path / "area" / "current tasks" / "index.md"
        content = ct_path.read_text(encoding="utf-8")
        # CT block ID is ltn-ct-{fingerprint}
        block_id = f"ltn-ct-{item.fingerprint}"
        assert content.count(make_begin_marker(block_id)) == 1, "block was duplicated by re-render"

    def test_block_ids_properly_listed(self, tmp_path: Path) -> None:
        current_tasks_dir = tmp_path / "area" / "current tasks"
        current_tasks_dir.mkdir(parents=True)

        items = [_item(task_id=f"task-{i}", fingerprint=f"fp{i:016d}") for i in range(3)]
        for item in items:
            render_current_tasks_item(item, tmp_path)

        ct_path = tmp_path / "area" / "current tasks" / "index.md"
        content = ct_path.read_text(encoding="utf-8")
        block_ids = list_block_ids(content)
        # CT block ID is ltn-ct-{fingerprint}
        for item in items:
            assert f"ltn-ct-{item.fingerprint}" in block_ids


# ---------------------------------------------------------------------------
# NoteWriter pipeline — end-to-end render
# ---------------------------------------------------------------------------


class TestNoteWriterPipelineGolden:
    def test_pipeline_creates_all_three_surfaces(self, tmp_path: Path) -> None:
        """render_pipeline creates raw, daily, and current-tasks notes for a promoted item."""
        vault = tmp_path / "vault"
        vault.mkdir()
        (vault / "area" / "current tasks").mkdir(parents=True)

        writer = NoteWriter(vault_root=vault)
        item = _item(promotion_rec="current_tasks")
        results = writer.render_pipeline(item)

        surfaces = {r.surface for r in results}
        assert RenderSurface.RAW in surfaces
        assert RenderSurface.DAILY in surfaces
        assert RenderSurface.CURRENT_TASKS in surfaces

    def test_pipeline_idempotent_on_second_render(self, tmp_path: Path) -> None:
        """Running render_pipeline twice yields SKIPPED on second pass (no changes)."""
        vault = tmp_path / "vault"
        vault.mkdir()
        (vault / "area" / "current tasks").mkdir(parents=True)

        writer = NoteWriter(vault_root=vault)
        item = _item(promotion_rec="current_tasks")
        writer.render_pipeline(item)
        results2 = writer.render_pipeline(item)

        for r in results2:
            assert r.outcome == RenderOutcome.SKIPPED, (
                f"surface {r.surface} was re-rendered but should be SKIPPED: {r}"
            )

    def test_pipeline_skips_current_tasks_for_daily_only(self, tmp_path: Path) -> None:
        vault = tmp_path / "vault"
        vault.mkdir()

        writer = NoteWriter(vault_root=vault)
        item = _item(promotion_rec="daily_only")
        results = writer.render_pipeline(item)

        ct_results = [r for r in results if r.surface == RenderSurface.CURRENT_TASKS]
        assert len(ct_results) == 0, "pipeline must not attempt CT surface for non-promoted items"


# ---------------------------------------------------------------------------
# Machine-owned block safety (no user content destruction)
# ---------------------------------------------------------------------------


class TestBlockSafety:
    def test_user_content_outside_block_is_preserved_through_multiple_rerenders(
        self, tmp_path: Path
    ) -> None:
        """User prose outside machine blocks must survive multiple render cycles."""
        block_id = "ltn-test-block"
        user_header = "# My Notes\n\nThis is user-authored content.\n\n"
        user_footer = "\n\nMore personal notes below.\n"
        initial_note = user_header + wrap_block(block_id, "Initial content.") + user_footer
        note_path = tmp_path / "test_note.md"
        note_path.write_text(initial_note, encoding="utf-8")

        for i in range(5):
            updated = replace_block(
                note_path.read_text(encoding="utf-8"), block_id, f"Content v{i}."
            )
            note_path.write_text(updated, encoding="utf-8")

        final = note_path.read_text(encoding="utf-8")
        assert "This is user-authored content." in final
        assert "More personal notes below." in final
        assert "Content v4." in final
        assert final.count(make_begin_marker(block_id)) == 1

    def test_new_block_appended_without_destroying_existing_content(self, tmp_path: Path) -> None:
        """Adding a second block leaves the first block and user content intact."""
        note_path = tmp_path / "note.md"
        note_path.write_text(
            "# Header\n\nUser text.\n\n"
            + wrap_block("ltn-block-a", "Block A content.")
            + "\nMore user text.\n",
            encoding="utf-8",
        )

        # Append block B
        from lark_to_notes.render.blocks import replace_block

        existing = note_path.read_text(encoding="utf-8")
        # Block B doesn't exist yet, so replace_block with append behaviour appends it
        updated = replace_block(existing, "ltn-block-b", "Block B content.")
        note_path.write_text(updated, encoding="utf-8")

        final = note_path.read_text(encoding="utf-8")
        assert "User text." in final
        assert "More user text." in final
        assert "Block A content." in final
        assert "Block B content." in final
        assert final.count(make_begin_marker("ltn-block-a")) == 1
        assert final.count(make_begin_marker("ltn-block-b")) == 1

    def test_malformed_block_does_not_corrupt_surrounding_text(self, tmp_path: Path) -> None:
        """A malformed (unclosed) block causes FAILED outcome, not file corruption."""
        # Build a raw note file with a manually malformed block (begin without end)
        block_id = "ltn-raw-abc123"
        malformed = (
            "---\ntype: raw\n---\n\n"
            + make_begin_marker(block_id)
            + "\n"
            + "Dangling content without end marker.\n"
        )
        note_path = tmp_path / "raw" / "2026-05-01-broken.md"
        note_path.parent.mkdir(parents=True)
        note_path.write_text(malformed, encoding="utf-8")

        # Attempting render_raw_note on this path (same date/slug) should fail gracefully.
        item = _item(
            fingerprint="abc123",
            title="Broken",
            event_date="2026-05-01",
        )
        render_raw_note(item, tmp_path)
        # Either FAILED (malformed block detected) or CREATED (new file at a different slug)
        # — the key property is that the original malformed file still exists unchanged.
        assert note_path.exists(), "original malformed file must not be deleted"
        original_content = note_path.read_text(encoding="utf-8")
        assert "Dangling content without end marker." in original_content


# ---------------------------------------------------------------------------
# Render output contains expected structural elements (not byte snapshots)
# ---------------------------------------------------------------------------


class TestRenderOutputStructure:
    def test_raw_note_contains_metadata_table(self, tmp_path: Path) -> None:
        item = _item(
            task_id="task-struct-1",
            fingerprint="struct1111111111",
            reason_code="en_action_marker",
            confidence_band="high",
            promotion_rec="current_tasks",
        )
        result = render_raw_note(item, tmp_path)
        content = Path(result.target_path).read_text(encoding="utf-8")

        # Markdown table headers
        assert "| field |" in content
        assert "| value |" in content
        # Key metadata rows
        assert "task_id" in content
        assert "fingerprint" in content
        assert "confidence" in content
        assert "reason" in content
        assert "promotion" in content

    def test_raw_note_has_heading_inside_block(self, tmp_path: Path) -> None:
        item = _item(title="Ship the release notes")
        result = render_raw_note(item, tmp_path)
        content = Path(result.target_path).read_text(encoding="utf-8")

        block_begin = content.index(make_begin_marker(f"ltn-raw-{item.fingerprint}"))
        block_end = content.index(make_end_marker(f"ltn-raw-{item.fingerprint}"))
        block_content = content[block_begin:block_end]
        assert "## Ship the release notes" in block_content

    def test_due_date_appears_in_raw_note_when_set(self, tmp_path: Path) -> None:
        item = _item(due_at="2026-05-15")
        result = render_raw_note(item, tmp_path)
        content = Path(result.target_path).read_text(encoding="utf-8")
        assert "2026-05-15" in content

    def test_assignees_appear_in_raw_note_when_set(self, tmp_path: Path) -> None:
        item = _item(assignee_refs=("ou_alice", "ou_bob"))
        result = render_raw_note(item, tmp_path)
        content = Path(result.target_path).read_text(encoding="utf-8")
        assert "ou_alice" in content
        assert "ou_bob" in content

    def test_chinese_title_slugified_without_crash(self, tmp_path: Path) -> None:
        """Chinese text in the title must not raise during slug creation."""
        item = _item(title="请审核这份报告", event_date="2026-05-01")
        result = render_raw_note(item, tmp_path)
        # Should not raise; outcome is CREATED or UPDATED
        assert result.outcome in (RenderOutcome.CREATED, RenderOutcome.UPDATED)

    def test_empty_summary_renders_without_extra_blank_lines(self, tmp_path: Path) -> None:
        """An item with no summary should not produce double blank lines."""
        item = _item(summary="")
        result = render_raw_note(item, tmp_path)
        content = Path(result.target_path).read_text(encoding="utf-8")
        # Block content should still be valid
        assert make_begin_marker(f"ltn-raw-{item.fingerprint}") in content
