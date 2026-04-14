from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from automation.lark_worker.fixture_corpus import (
    copy_source_access_artifacts,
    has_han,
    is_english_only,
    is_mixed_language,
    is_threaded_record,
    select_fixture_examples,
)


class FixtureCorpusTests(unittest.TestCase):
    def test_language_helpers(self) -> None:
        self.assertTrue(has_han("需要 review"))
        self.assertTrue(is_mixed_language("需要 review"))
        self.assertTrue(is_english_only("please review the draft"))
        self.assertFalse(is_english_only("请 review"))

    def test_threaded_record_detection(self) -> None:
        self.assertTrue(is_threaded_record({"payload": {"thread_id": "omt_1"}}))
        self.assertTrue(is_threaded_record({"payload": {"thread_replies": [{"message_id": "x"}]}}))
        self.assertFalse(is_threaded_record({"payload": {}}))

    def test_select_fixture_examples_picks_expected_scenarios(self) -> None:
        records = [
            {"content": "please review", "payload": {"updated": False, "deleted": False}},
            {"content": "需要 review", "payload": {"updated": False, "deleted": False}},
            {"content": "只中文", "payload": {"updated": True, "deleted": False}},
            {"content": "deleted sample", "payload": {"updated": False, "deleted": True}},
            {"content": "thread sample", "payload": {"thread_id": "omt_1", "updated": False, "deleted": False}},
        ]
        examples = select_fixture_examples(records)
        self.assertIn("english_only", examples)
        self.assertIn("mixed", examples)
        self.assertIn("han", examples)
        self.assertIn("updated", examples)
        self.assertIn("deleted", examples)
        self.assertIn("threaded", examples)

    def test_copy_source_access_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source_access_dir = Path(temp_dir) / "source-access"
            destination = Path(temp_dir) / "fixture-corpus" / "doc-surfaces"
            source_access_dir.mkdir(parents=True)
            (source_access_dir / "doc_fetch-sample.json").write_text("{}", encoding="utf-8")
            (source_access_dir / "source-access-report.json").write_text(
                '{"probes":[{"artifact_relpath":"doc_fetch-sample.json"}]}',
                encoding="utf-8",
            )
            copied = copy_source_access_artifacts(source_access_dir, destination)
            self.assertIn("doc-surfaces/source-access-report.json", copied)
            self.assertIn("doc-surfaces/doc_fetch-sample.json", copied)


if __name__ == "__main__":
    unittest.main()
