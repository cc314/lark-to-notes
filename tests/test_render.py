"""Tests for the lark_to_notes.render package.

Coverage:
 - blocks: markers, wrap_block, replace_block, extract_block, list_block_ids, MalformedBlockError
 - raw: render_raw_note — create/update/skip/fail, frontmatter, body block, user content preserved
 - daily: render_daily_note — create/update/skip/fail, bullet variants, section header injection
 - current_tasks: render_current_tasks_item, render_current_tasks (batch), remove_demoted_blocks
 - writer: NoteWriter pipeline, surface dispatch, path propagation, FAILED path not propagated
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lark_to_notes.render.blocks import (
    MalformedBlockError,
    extract_block,
    list_block_ids,
    make_begin_marker,
    make_end_marker,
    replace_block,
    wrap_block,
)
from lark_to_notes.render.current_tasks import (
    remove_demoted_blocks,
    render_current_tasks,
    render_current_tasks_item,
)
from lark_to_notes.render.daily import render_daily_note
from lark_to_notes.render.models import RenderItem, RenderOutcome, RenderSurface
from lark_to_notes.render.raw import render_raw_note
from lark_to_notes.render.writer import NoteWriter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _item(
    *,
    task_id: str = "task-0001",
    fingerprint: str = "abcdef1234567890",
    title: str = "Test task",
    promotion_rec: str = "current_tasks",
    reason_code: str = "en_action_marker",
    confidence_band: str = "high",
    task_class: str = "task",
    status: str = "open",
    summary: str = "",
    assignee_refs: tuple[str, ...] = (),
    source_note_path: str | None = None,
    daily_note_path: str | None = None,
    due_at: str | None = None,
    source_message_id: str | None = None,
    event_date: str = "2024-01-15",
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
# blocks.py
# ---------------------------------------------------------------------------


class TestBlockMarkers:
    def test_make_begin_marker(self) -> None:
        assert make_begin_marker("my-block") == '<!-- ltn:begin id="my-block" -->'

    def test_make_end_marker(self) -> None:
        assert make_end_marker("my-block") == '<!-- ltn:end id="my-block" -->'

    def test_wrap_block_structure(self) -> None:
        result = wrap_block("bid", "content line")
        assert result.startswith(make_begin_marker("bid"))
        assert make_end_marker("bid") in result
        assert "content line" in result
        # Must end with trailing newline
        assert result.endswith("\n")

    def test_wrap_block_strips_trailing_newlines_from_content(self) -> None:
        result = wrap_block("bid", "content\n\n")
        # Content should be stripped once before wrapping
        assert "content\n" + make_end_marker("bid") in result

    def test_wrap_block_multiline(self) -> None:
        result = wrap_block("bid", "line1\nline2")
        assert "line1\nline2" in result


class TestReplaceBlock:
    def test_append_when_no_existing_block(self) -> None:
        note = "# My Note\n\nsome user content\n"
        updated = replace_block(note, "new-block", "new content")
        assert make_begin_marker("new-block") in updated
        assert "new content" in updated
        assert "some user content" in updated

    def test_replace_existing_block(self) -> None:
        block_id = "my-block"
        original = "prefix\n" + wrap_block(block_id, "old content") + "suffix\n"
        updated = replace_block(original, block_id, "new content")
        assert "new content" in updated
        assert "old content" not in updated
        assert "prefix\n" in updated
        assert "suffix\n" in updated

    def test_idempotent_same_content(self) -> None:
        block_id = "my-block"
        initial = wrap_block(block_id, "stable")
        # replace_block always does the replacement; idempotency is checked at caller level
        updated = replace_block(initial, block_id, "stable")
        # The text should be structurally equivalent (block replaced with same content)
        assert make_begin_marker(block_id) in updated
        assert "stable" in updated

    def test_separator_with_trailing_newline(self) -> None:
        note = "some text\n"
        updated = replace_block(note, "bid", "content")
        # Should add newline separator before the block
        assert updated.startswith("some text\n")

    def test_separator_without_trailing_newline(self) -> None:
        note = "some text"
        updated = replace_block(note, "bid", "content")
        assert make_begin_marker("bid") in updated

    def test_raises_on_end_without_begin(self) -> None:
        note = "text\n" + make_end_marker("bid") + "\nmore\n"
        with pytest.raises(MalformedBlockError) as exc_info:
            replace_block(note, "bid", "content")
        assert exc_info.value.block_id == "bid"
        assert "end marker found without begin marker" in exc_info.value.detail

    def test_raises_on_begin_without_end(self) -> None:
        note = "text\n" + make_begin_marker("bid") + "\ncontent\n"
        with pytest.raises(MalformedBlockError) as exc_info:
            replace_block(note, "bid", "new content")
        assert "begin marker found without end marker" in exc_info.value.detail

    def test_raises_on_end_before_begin(self) -> None:
        note = make_end_marker("bid") + "\n" + make_begin_marker("bid") + "\n"
        with pytest.raises(MalformedBlockError) as exc_info:
            replace_block(note, "bid", "content")
        assert "end marker precedes begin marker" in exc_info.value.detail


class TestExtractBlock:
    def test_extract_existing_block(self) -> None:
        block_id = "xid"
        note = "before\n" + wrap_block(block_id, "inner content") + "after\n"
        result = extract_block(note, block_id)
        assert result == "inner content"

    def test_extract_returns_none_when_missing(self) -> None:
        assert extract_block("no blocks here", "missing") is None

    def test_extract_returns_none_when_only_begin(self) -> None:
        note = make_begin_marker("bid") + "\ncontent without end\n"
        assert extract_block(note, "bid") is None

    def test_extract_returns_none_when_only_end(self) -> None:
        note = make_end_marker("bid") + "\n"
        assert extract_block(note, "bid") is None

    def test_extract_returns_none_when_end_before_begin(self) -> None:
        note = make_end_marker("bid") + "\n" + make_begin_marker("bid") + "\n"
        assert extract_block(note, "bid") is None

    def test_extract_strips_whitespace(self) -> None:
        # The extract function strips the content
        block_id = "bid"
        note = make_begin_marker(block_id) + "\n  trimmed  \n" + make_end_marker(block_id) + "\n"
        result = extract_block(note, block_id)
        assert result == "trimmed"


class TestListBlockIds:
    def test_empty_text(self) -> None:
        assert list_block_ids("") == []

    def test_single_block(self) -> None:
        note = wrap_block("alpha", "content")
        assert list_block_ids(note) == ["alpha"]

    def test_multiple_blocks_in_order(self) -> None:
        note = wrap_block("first", "a") + "\n" + wrap_block("second", "b") + "\n" + wrap_block("third", "c")
        assert list_block_ids(note) == ["first", "second", "third"]

    def test_ignores_non_block_content(self) -> None:
        note = "# Heading\n\nsome markdown\n"
        assert list_block_ids(note) == []


# ---------------------------------------------------------------------------
# raw.py
# ---------------------------------------------------------------------------


class TestRenderRawNote:
    def test_creates_new_file(self, tmp_path: Path) -> None:
        item = _item()
        result = render_raw_note(item, tmp_path)
        assert result.outcome == RenderOutcome.CREATED
        assert result.surface == RenderSurface.RAW
        assert result.error is None
        assert Path(result.target_path).exists()

    def test_created_file_is_in_raw_dir(self, tmp_path: Path) -> None:
        item = _item()
        result = render_raw_note(item, tmp_path)
        assert Path(result.target_path).parent == tmp_path / "raw"

    def test_file_name_uses_date_and_slug(self, tmp_path: Path) -> None:
        item = _item(event_date="2024-03-10", title="Deploy the server")
        result = render_raw_note(item, tmp_path)
        assert "2024-03-10" in Path(result.target_path).name
        assert "deploy" in Path(result.target_path).name

    def test_frontmatter_type_and_source(self, tmp_path: Path) -> None:
        item = _item()
        result = render_raw_note(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "type: raw" in text
        assert "source: lark" in text

    def test_frontmatter_published_date(self, tmp_path: Path) -> None:
        item = _item(event_date="2024-05-20")
        result = render_raw_note(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "published: 2024-05-20" in text

    def test_body_contains_task_id_and_fingerprint(self, tmp_path: Path) -> None:
        item = _item(task_id="tid-42", fingerprint="deadbeef12345678")
        result = render_raw_note(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "tid-42" in text
        assert "deadbeef12345678" in text

    def test_body_contains_title(self, tmp_path: Path) -> None:
        item = _item(title="Fix the login bug")
        result = render_raw_note(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "Fix the login bug" in text

    def test_body_contains_summary_when_present(self, tmp_path: Path) -> None:
        item = _item(summary="This is a detailed summary of the task.")
        result = render_raw_note(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "detailed summary" in text

    def test_block_id_in_result(self, tmp_path: Path) -> None:
        item = _item(fingerprint="abcdef1234567890")
        result = render_raw_note(item, tmp_path)
        assert result.block_id == "ltn-raw-abcdef1234567890"

    def test_skipped_on_rerender_same_content(self, tmp_path: Path) -> None:
        item = _item()
        render_raw_note(item, tmp_path)
        result2 = render_raw_note(item, tmp_path)
        assert result2.outcome == RenderOutcome.SKIPPED

    def test_updated_when_status_changes(self, tmp_path: Path) -> None:
        item = _item()
        render_raw_note(item, tmp_path)
        # Change a field that affects rendered output
        item2 = _item(summary="Updated summary that changes the block content.")
        result2 = render_raw_note(item2, tmp_path)
        assert result2.outcome == RenderOutcome.UPDATED

    def test_user_content_outside_block_preserved(self, tmp_path: Path) -> None:
        item = _item()
        result = render_raw_note(item, tmp_path)
        note_path = Path(result.target_path)
        # Inject user content after the machine block
        original = note_path.read_text()
        user_note = original + "\n## My notes\n\nKeep this please.\n"
        note_path.write_text(user_note)
        # Re-render with same content → SKIPPED; user content must survive
        render_raw_note(item, tmp_path)
        after = note_path.read_text()
        assert "Keep this please." in after

    def test_user_content_preserved_on_update(self, tmp_path: Path) -> None:
        item = _item()
        result = render_raw_note(item, tmp_path)
        note_path = Path(result.target_path)
        original = note_path.read_text()
        note_path.write_text(original + "\n## My notes\n\nImportant annotation.\n")
        item2 = _item(summary="Changed summary to force UPDATED.")
        render_raw_note(item2, tmp_path)
        after = note_path.read_text()
        assert "Important annotation." in after

    def test_daily_note_backlink_in_body(self, tmp_path: Path) -> None:
        item = _item(daily_note_path=str(tmp_path / "daily" / "2024-01-15.md"))
        result = render_raw_note(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "[[2024-01-15]]" in text

    def test_failed_on_io_error(self, tmp_path: Path) -> None:
        import os

        # Pre-create raw/ so mkdir(exist_ok=True) inside render_raw_note succeeds,
        # but make it read-only so the actual file write fails with PermissionError.
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        os.chmod(raw_dir, 0o555)
        if os.access(raw_dir, os.W_OK):
            os.chmod(raw_dir, 0o755)
            pytest.skip("cannot make dir read-only in this environment")
        try:
            item = _item()
            result = render_raw_note(item, tmp_path)
        finally:
            os.chmod(raw_dir, 0o755)
        assert result.outcome == RenderOutcome.FAILED
        assert result.error is not None


# ---------------------------------------------------------------------------
# daily.py
# ---------------------------------------------------------------------------


class TestRenderDailyNote:
    def test_creates_new_daily_note(self, tmp_path: Path) -> None:
        item = _item(event_date="2024-06-01")
        result = render_daily_note(item, tmp_path)
        assert result.outcome == RenderOutcome.CREATED
        assert result.surface == RenderSurface.DAILY
        assert Path(result.target_path).exists()

    def test_file_path_is_date_based(self, tmp_path: Path) -> None:
        item = _item(event_date="2024-06-01")
        result = render_daily_note(item, tmp_path)
        assert Path(result.target_path) == tmp_path / "daily" / "2024-06-01.md"

    def test_failed_when_no_date(self, tmp_path: Path) -> None:
        item = _item(event_date="")
        result = render_daily_note(item, tmp_path)
        assert result.outcome == RenderOutcome.FAILED
        assert result.error is not None

    def test_date_override_parameter(self, tmp_path: Path) -> None:
        item = _item(event_date="2024-06-01")
        result = render_daily_note(item, tmp_path, date="2024-07-04")
        assert Path(result.target_path) == tmp_path / "daily" / "2024-07-04.md"

    def test_bullet_contains_title(self, tmp_path: Path) -> None:
        item = _item(title="Review PR 42")
        result = render_daily_note(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "Review PR 42" in text

    def test_bullet_contains_reason_code(self, tmp_path: Path) -> None:
        item = _item(reason_code="en_action_marker")
        result = render_daily_note(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "#en_action_marker" in text

    def test_bullet_contains_source_note_link(self, tmp_path: Path) -> None:
        item = _item(source_note_path=str(tmp_path / "raw" / "2024-01-15-test-task.md"))
        result = render_daily_note(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "[[2024-01-15-test-task]]" in text

    def test_bullet_open_checkbox(self, tmp_path: Path) -> None:
        item = _item(status="open")
        result = render_daily_note(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "[ ]" in text

    def test_bullet_completed_checkbox(self, tmp_path: Path) -> None:
        item = _item(status="completed")
        result = render_daily_note(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "[x]" in text

    def test_bullet_needs_review_checkbox(self, tmp_path: Path) -> None:
        item = _item(status="needs_review")
        result = render_daily_note(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "[?]" in text

    def test_bullet_assignees(self, tmp_path: Path) -> None:
        item = _item(assignee_refs=("@alice", "@bob"))
        result = render_daily_note(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "@alice" in text
        assert "@bob" in text

    def test_bullet_due_at(self, tmp_path: Path) -> None:
        item = _item(due_at="2024-02-28")
        result = render_daily_note(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "2024-02-28" in text

    def test_section_header_created(self, tmp_path: Path) -> None:
        item = _item()
        result = render_daily_note(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "## Work items (ltn-managed)" in text

    def test_section_header_injected_into_existing_note(self, tmp_path: Path) -> None:
        daily_dir = tmp_path / "daily"
        daily_dir.mkdir()
        note_path = daily_dir / "2024-01-15.md"
        note_path.write_text("---\ntype: daily\n---\n\n# 2024-01-15\n\nExisting content.\n")
        item = _item()
        render_daily_note(item, tmp_path)
        text = note_path.read_text()
        assert "## Work items (ltn-managed)" in text
        assert "Existing content." in text

    def test_skipped_on_rerender_same_content(self, tmp_path: Path) -> None:
        item = _item()
        render_daily_note(item, tmp_path)
        result2 = render_daily_note(item, tmp_path)
        assert result2.outcome == RenderOutcome.SKIPPED

    def test_updated_when_status_changes(self, tmp_path: Path) -> None:
        item = _item(status="open")
        render_daily_note(item, tmp_path)
        item2 = _item(status="completed")
        result2 = render_daily_note(item2, tmp_path)
        assert result2.outcome == RenderOutcome.UPDATED
        text = Path(result2.target_path).read_text()
        assert "[x]" in text

    def test_block_id_in_result(self, tmp_path: Path) -> None:
        item = _item(fingerprint="deadbeef12345678")
        result = render_daily_note(item, tmp_path)
        assert result.block_id == "ltn-daily-deadbeef12345678"

    def test_user_content_outside_block_preserved(self, tmp_path: Path) -> None:
        item = _item()
        result = render_daily_note(item, tmp_path)
        note_path = Path(result.target_path)
        note_path.write_text(note_path.read_text() + "\n## My diary entry\n\nHand-written notes.\n")
        item2 = _item(status="completed")
        render_daily_note(item2, tmp_path)
        assert "Hand-written notes." in note_path.read_text()


# ---------------------------------------------------------------------------
# current_tasks.py
# ---------------------------------------------------------------------------


class TestRenderCurrentTasksItem:
    def test_creates_new_file(self, tmp_path: Path) -> None:
        item = _item()
        result = render_current_tasks_item(item, tmp_path)
        assert result.outcome == RenderOutcome.CREATED
        assert result.surface == RenderSurface.CURRENT_TASKS
        assert Path(result.target_path).exists()

    def test_file_path_is_canonical(self, tmp_path: Path) -> None:
        item = _item()
        result = render_current_tasks_item(item, tmp_path)
        expected = tmp_path / "area" / "current tasks" / "index.md"
        assert Path(result.target_path) == expected

    def test_bullet_open(self, tmp_path: Path) -> None:
        item = _item(status="open")
        result = render_current_tasks_item(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "[ ]" in text

    def test_bullet_terminal_strikethrough(self, tmp_path: Path) -> None:
        item = _item(status="completed", title="Done task")
        result = render_current_tasks_item(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "~~Done task~~" in text

    def test_bullet_dismissed_strikethrough(self, tmp_path: Path) -> None:
        item = _item(status="dismissed", title="Dropped task")
        result = render_current_tasks_item(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "~~Dropped task~~" in text

    def test_bullet_with_source_and_daily_links(self, tmp_path: Path) -> None:
        item = _item(
            source_note_path=str(tmp_path / "raw" / "2024-01-15-test.md"),
            daily_note_path=str(tmp_path / "daily" / "2024-01-15.md"),
        )
        result = render_current_tasks_item(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "[[2024-01-15-test]]" in text
        assert "[[2024-01-15]]" in text

    def test_skipped_on_rerender(self, tmp_path: Path) -> None:
        item = _item()
        render_current_tasks_item(item, tmp_path)
        result2 = render_current_tasks_item(item, tmp_path)
        assert result2.outcome == RenderOutcome.SKIPPED

    def test_updated_when_status_changes(self, tmp_path: Path) -> None:
        item = _item(status="open")
        render_current_tasks_item(item, tmp_path)
        item2 = _item(status="completed")
        result2 = render_current_tasks_item(item2, tmp_path)
        assert result2.outcome == RenderOutcome.UPDATED

    def test_section_header_present(self, tmp_path: Path) -> None:
        item = _item()
        result = render_current_tasks_item(item, tmp_path)
        text = Path(result.target_path).read_text()
        assert "## Open tasks (ltn-managed)" in text

    def test_user_content_outside_block_preserved(self, tmp_path: Path) -> None:
        item = _item()
        result = render_current_tasks_item(item, tmp_path)
        ct_path = Path(result.target_path)
        ct_path.write_text(ct_path.read_text() + "\n## My section\n\nUser annotation.\n")
        item2 = _item(status="snoozed")
        render_current_tasks_item(item2, tmp_path)
        assert "User annotation." in ct_path.read_text()

    def test_block_id_uses_fingerprint(self, tmp_path: Path) -> None:
        item = _item(fingerprint="cafe0000deadbeef")
        result = render_current_tasks_item(item, tmp_path)
        assert result.block_id == "ltn-ct-cafe0000deadbeef"


class TestRenderCurrentTasksBatch:
    def test_items_with_wrong_promotion_rec_skipped(self, tmp_path: Path) -> None:
        items = [
            _item(fingerprint="aaa0000000000001", promotion_rec="daily_only"),
            _item(fingerprint="bbb0000000000002", promotion_rec="review"),
        ]
        results = render_current_tasks(items, tmp_path)
        assert all(r.outcome == RenderOutcome.SKIPPED for r in results)
        # File should NOT have been created
        assert not (tmp_path / "area" / "current tasks" / "index.md").exists()

    def test_eligible_items_are_written(self, tmp_path: Path) -> None:
        items = [
            _item(fingerprint="aaa0000000000001", promotion_rec="current_tasks", title="Task A"),
            _item(fingerprint="bbb0000000000002", promotion_rec="current_tasks", title="Task B"),
        ]
        results = render_current_tasks(items, tmp_path)
        assert results[0].outcome == RenderOutcome.CREATED
        assert results[1].outcome == RenderOutcome.UPDATED  # second write to same file

    def test_mixed_batch(self, tmp_path: Path) -> None:
        items = [
            _item(fingerprint="aaa0000000000001", promotion_rec="current_tasks", title="Eligible"),
            _item(fingerprint="bbb0000000000002", promotion_rec="daily_only", title="Skipped"),
        ]
        results = render_current_tasks(items, tmp_path)
        eligible_result = next(r for r in results if r.entity_id != "task-0001" or r.outcome != RenderOutcome.SKIPPED)
        assert eligible_result.outcome in (RenderOutcome.CREATED, RenderOutcome.UPDATED)


class TestRemoveDemotedBlocks:
    def test_removes_block_not_in_keep_set(self, tmp_path: Path) -> None:
        item1 = _item(fingerprint="keep0000000000001")
        item2 = _item(fingerprint="drop0000000000002")
        render_current_tasks_item(item1, tmp_path)
        render_current_tasks_item(item2, tmp_path)

        removed = remove_demoted_blocks({"keep0000000000001"}, tmp_path)
        assert len(removed) == 1
        assert removed[0] == "ltn-ct-drop0000000000002"

        ct_path = tmp_path / "area" / "current tasks" / "index.md"
        text = ct_path.read_text()
        assert "ltn-ct-drop0000000000002" not in text
        assert "ltn-ct-keep0000000000001" in text

    def test_keeps_all_blocks_when_all_in_keep_set(self, tmp_path: Path) -> None:
        item = _item(fingerprint="keep0000000000001")
        render_current_tasks_item(item, tmp_path)
        removed = remove_demoted_blocks({"keep0000000000001"}, tmp_path)
        assert removed == []

    def test_returns_empty_when_no_ct_file(self, tmp_path: Path) -> None:
        removed = remove_demoted_blocks(set(), tmp_path)
        assert removed == []

    def test_ignores_non_ltn_ct_blocks(self, tmp_path: Path) -> None:
        ct_path = tmp_path / "area" / "current tasks" / "index.md"
        ct_path.parent.mkdir(parents=True, exist_ok=True)
        ct_path.write_text(wrap_block("other-block-123", "user content") + "\n")
        removed = remove_demoted_blocks(set(), tmp_path)
        assert removed == []
        assert "other-block-123" in ct_path.read_text()

    def test_user_content_preserved_after_demotion(self, tmp_path: Path) -> None:
        item = _item(fingerprint="drop0000000000001")
        result = render_current_tasks_item(item, tmp_path)
        ct_path = Path(result.target_path)
        ct_path.write_text(ct_path.read_text() + "\n## Manual notes\n\nUser wrote this.\n")
        remove_demoted_blocks(set(), tmp_path)
        assert "User wrote this." in ct_path.read_text()


# ---------------------------------------------------------------------------
# writer.py
# ---------------------------------------------------------------------------


class TestNoteWriterRenderRaw:
    def test_writes_raw_note(self, tmp_path: Path) -> None:
        writer = NoteWriter(vault_root=tmp_path)
        item = _item()
        result = writer.render_raw(item)
        assert result.outcome == RenderOutcome.CREATED

    def test_skipped_on_second_render(self, tmp_path: Path) -> None:
        writer = NoteWriter(vault_root=tmp_path)
        item = _item()
        writer.render_raw(item)
        result2 = writer.render_raw(item)
        assert result2.outcome == RenderOutcome.SKIPPED


class TestNoteWriterRenderDaily:
    def test_writes_daily_note(self, tmp_path: Path) -> None:
        writer = NoteWriter(vault_root=tmp_path)
        item = _item()
        result = writer.render_daily(item)
        assert result.outcome == RenderOutcome.CREATED

    def test_respects_date_override(self, tmp_path: Path) -> None:
        writer = NoteWriter(vault_root=tmp_path)
        item = _item(event_date="2024-01-01")
        result = writer.render_daily(item, date="2024-12-25")
        assert "2024-12-25" in result.target_path


class TestNoteWriterPipeline:
    def test_full_pipeline_all_three_stages(self, tmp_path: Path) -> None:
        writer = NoteWriter(vault_root=tmp_path)
        item = _item(promotion_rec="current_tasks", event_date="2024-06-10")
        results = writer.render_pipeline(item)
        surfaces = {r.surface for r in results}
        assert RenderSurface.RAW in surfaces
        assert RenderSurface.DAILY in surfaces
        assert RenderSurface.CURRENT_TASKS in surfaces

    def test_pipeline_skips_daily_when_no_date(self, tmp_path: Path) -> None:
        writer = NoteWriter(vault_root=tmp_path)
        item = _item(event_date="")
        results = writer.render_pipeline(item)
        surfaces = {r.surface for r in results}
        assert RenderSurface.DAILY not in surfaces

    def test_pipeline_skips_ct_when_not_promotion_rec(self, tmp_path: Path) -> None:
        writer = NoteWriter(vault_root=tmp_path)
        item = _item(promotion_rec="daily_only")
        results = writer.render_pipeline(item)
        surfaces = {r.surface for r in results}
        assert RenderSurface.CURRENT_TASKS not in surfaces

    def test_source_note_path_propagated_to_daily(self, tmp_path: Path) -> None:
        writer = NoteWriter(vault_root=tmp_path)
        item = _item(promotion_rec="daily_only", event_date="2024-01-15")
        writer.render_pipeline(item)
        daily_path = tmp_path / "daily" / "2024-01-15.md"
        text = daily_path.read_text()
        # The daily note should have a wikilink to the raw note
        assert "[[" in text

    def test_source_note_path_not_propagated_when_raw_fails(
        self, tmp_path: Path
    ) -> None:
        import os

        # Make raw/ read-only so render_raw_note fails with PermissionError,
        # which is caught internally and returns outcome=FAILED.
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        os.chmod(raw_dir, 0o555)
        if os.access(raw_dir, os.W_OK):
            os.chmod(raw_dir, 0o755)
            pytest.skip("cannot make dir read-only in this environment")
        try:
            writer = NoteWriter(vault_root=tmp_path)
            item = _item(promotion_rec="daily_only", event_date="2024-01-15")
            results = writer.render_pipeline(item)
        finally:
            os.chmod(raw_dir, 0o755)

        # Raw stage must have failed.
        assert results[0].outcome == RenderOutcome.FAILED
        # Daily note should still be rendered (pipeline continues after raw failure).
        daily_path = tmp_path / "daily" / "2024-01-15.md"
        if daily_path.exists():
            text = daily_path.read_text()
            # source_note_path was NOT propagated from the failed raw result,
            # so the daily note must not contain a wikilink pointing into raw/.
            assert str(raw_dir) not in text

    def test_pipeline_returns_results_in_order(self, tmp_path: Path) -> None:
        writer = NoteWriter(vault_root=tmp_path)
        item = _item(promotion_rec="current_tasks", event_date="2024-06-10")
        results = writer.render_pipeline(item)
        assert results[0].surface == RenderSurface.RAW
        assert results[1].surface == RenderSurface.DAILY
        assert results[2].surface == RenderSurface.CURRENT_TASKS


class TestNoteWriterSurfaceDispatch:
    def test_render_surface_raw(self, tmp_path: Path) -> None:
        writer = NoteWriter(vault_root=tmp_path)
        result = writer.render_surface(_item(), RenderSurface.RAW)
        assert result.surface == RenderSurface.RAW

    def test_render_surface_daily(self, tmp_path: Path) -> None:
        writer = NoteWriter(vault_root=tmp_path)
        result = writer.render_surface(_item(), RenderSurface.DAILY)
        assert result.surface == RenderSurface.DAILY

    def test_render_surface_current_tasks(self, tmp_path: Path) -> None:
        writer = NoteWriter(vault_root=tmp_path)
        result = writer.render_surface(_item(), RenderSurface.CURRENT_TASKS)
        assert result.surface == RenderSurface.CURRENT_TASKS
