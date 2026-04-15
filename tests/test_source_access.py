from __future__ import annotations

import unittest

from lark_to_notes.testing.fixtures import (
    ProbeRecord,
    build_report_manifest,
    extract_doc_token,
    summarize_probe,
)


class SourceAccessTests(unittest.TestCase):
    def test_extract_doc_token_from_url(self) -> None:
        self.assertEqual(
            extract_doc_token(
                "https://gotocompany.sg.larksuite.com/docx/D5IXd2EQLoZ3yvxY1WBlJi71gmf"
            ),
            "D5IXd2EQLoZ3yvxY1WBlJi71gmf",
        )

    def test_extract_doc_token_from_raw_token(self) -> None:
        self.assertEqual(
            extract_doc_token("D5IXd2EQLoZ3yvxY1WBlJi71gmf"), "D5IXd2EQLoZ3yvxY1WBlJi71gmf"
        )

    def test_summarize_probe_marks_chat_payload_with_messages_as_ok(self) -> None:
        summary = summarize_probe(
            "dm_chat_messages",
            {
                "ok": True,
                "identity": "user",
                "data": {
                    "messages": [{"message_id": "om_1"}],
                    "has_more": False,
                },
            },
        )
        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["sample_count"], 1)
        self.assertEqual(summary["identity"], "user")

    def test_summarize_probe_marks_empty_comment_payload_as_empty(self) -> None:
        summary = summarize_probe(
            "doc_comments",
            {
                "code": 0,
                "data": {
                    "items": [],
                    "has_more": False,
                },
            },
        )
        self.assertEqual(summary["status"], "empty")
        self.assertEqual(summary["sample_count"], 0)

    def test_summarize_probe_marks_failed_payload_as_blocked(self) -> None:
        summary = summarize_probe(
            "doc_fetch",
            {
                "ok": False,
                "message": "permission denied",
            },
        )
        self.assertEqual(summary["status"], "blocked")
        self.assertEqual(summary["notes"], "permission denied")

    def test_build_report_manifest_counts_statuses(self) -> None:
        manifest = build_report_manifest(
            [
                ProbeRecord(
                    surface="dm_chat_messages",
                    target_id="dm-a",
                    target_name="DM A",
                    command="cmd-a",
                    status="ok",
                    sample_count=1,
                    top_level_keys=["messages"],
                ),
                ProbeRecord(
                    surface="doc_comments",
                    target_id="doc-a",
                    target_name="Doc A",
                    command="cmd-b",
                    status="empty",
                    sample_count=0,
                    top_level_keys=["items"],
                ),
                ProbeRecord(
                    surface="doc_comment_replies",
                    target_id="doc-a",
                    target_name="Doc A",
                    command="cmd-c",
                    status="not_sampled",
                    sample_count=0,
                    top_level_keys=[],
                ),
            ]
        )
        self.assertEqual(manifest["probe_count"], 3)
        self.assertEqual(manifest["counts_by_status"]["ok"], 1)
        self.assertEqual(manifest["counts_by_status"]["empty"], 1)
        self.assertEqual(manifest["counts_by_status"]["not_sampled"], 1)


if __name__ == "__main__":
    unittest.main()
