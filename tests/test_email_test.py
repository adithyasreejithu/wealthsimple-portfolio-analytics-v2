import sys
import unittest
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import email_test  # noqa: E402


class InteracEmailTest(unittest.TestCase):
    def test_parse_email_content_extracts_debit_and_body_date(self):
        body = "\n".join(
            [
                "You received money.",
                "Amount: $1,234.56",
                "Date: June 18, 2026",
            ]
        )

        df = email_test.parse_email_content(body, date(2026, 6, 19))

        self.assertEqual(df.iloc[0]["account"], "TFSA")
        self.assertEqual(df.iloc[0]["transaction"], "Deposit")
        self.assertEqual(df.iloc[0]["ticker_id"], 0)
        self.assertEqual(df.iloc[0]["ticker"], "EMAIL")
        self.assertEqual(df.iloc[0]["debit"], "1234.56")
        self.assertEqual(df.iloc[0]["date"], date(2026, 6, 18))

    def test_parse_email_content_uses_received_date_when_body_date_missing(self):
        df = email_test.parse_email_content(
            "Your deposit for CAD 50.00 is complete.",
            date(2026, 6, 19),
        )

        self.assertEqual(df.iloc[0]["debit"], "50.00")
        self.assertEqual(df.iloc[0]["date"], date(2026, 6, 19))

    def test_parse_email_content_extracts_debit_from_subject(self):
        df = email_test.parse_email_content(
            "",
            date(2026, 5, 20),
            "Interac e-Transfer: Name sent you $300.00 In response to your request.",
        )

        self.assertEqual(df.iloc[0]["debit"], "300.00")
        self.assertEqual(df.iloc[0]["date"], date(2026, 5, 20))

    @patch.object(email_test, "MailBox")
    def test_find_interac_emails_fetches_sender_without_date_or_database(
        self,
        mailbox_mock,
    ):
        message = SimpleNamespace(
            subject=(
                "Interac e-Transfer: ADITHYA SREEJITHU PANICKER sent you "
                "$75.00 In response to your request."
            ),
            from_=email_test.INTERAC_SENDER,
            date=datetime(2026, 6, 18, 10, 0),
            text="Date: 2026-06-18",
        )
        mailbox_session = MagicMock()
        mailbox_session.fetch.return_value = [message]
        mailbox_mock.return_value.login.return_value.__enter__.return_value = (
            mailbox_session
        )

        with patch("builtins.print"):
            df = email_test.find_interac_emails()

        fetch_criteria = mailbox_session.fetch.call_args.args[0]
        self.assertNotIn("SINCE", str(fetch_criteria).upper())
        self.assertIn(email_test.INTERAC_SENDER, str(fetch_criteria))
        self.assertIn(email_test.INTERAC_SUBJECT, str(fetch_criteria))
        self.assertEqual(df.iloc[0]["ticker_id"], 0)
        self.assertEqual(df.iloc[0]["debit"], "75.00")

    @patch.object(email_test, "MailBox")
    def test_find_interac_emails_filters_sender_in_python(self, mailbox_mock):
        messages = [
            SimpleNamespace(
                subject="Other",
                from_="someone@example.com",
                date=datetime(2026, 6, 18, 9, 0),
                text="Amount: $1.00",
            ),
            SimpleNamespace(
                subject=(
                    "Interac e-Transfer: ADITHYA SREEJITHU PANICKER sent you "
                    "$75.00 In response to your request."
                ),
                from_=f"Payments <{email_test.INTERAC_SENDER}>",
                date=datetime(2026, 6, 18, 10, 0),
                text="",
            ),
        ]
        mailbox_session = MagicMock()
        mailbox_session.fetch.return_value = messages
        mailbox_mock.return_value.login.return_value.__enter__.return_value = (
            mailbox_session
        )

        with patch("builtins.print"):
            df = email_test.find_interac_emails()

        self.assertEqual(df.shape[0], 1)
        self.assertEqual(df.iloc[0]["debit"], "75.00")

    @patch.object(email_test, "MailBox")
    def test_find_interac_emails_requires_expected_subject_shape(self, mailbox_mock):
        messages = [
            SimpleNamespace(
                subject="Interac e-Transfer: Someone else sent you $75.00",
                from_=email_test.INTERAC_SENDER,
                date=datetime(2026, 6, 18, 9, 0),
                text="Amount: $75.00",
            ),
            SimpleNamespace(
                subject=(
                    "Interac e-Transfer: ADITHYA SREEJITHU PANICKER sent you "
                    "$1,300.50 In response to your request"
                ),
                from_=email_test.INTERAC_SENDER,
                date=datetime(2026, 6, 18, 10, 0),
                text="",
            ),
        ]
        mailbox_session = MagicMock()
        mailbox_session.fetch.return_value = messages
        mailbox_mock.return_value.login.return_value.__enter__.return_value = (
            mailbox_session
        )

        with patch("builtins.print"):
            df = email_test.find_interac_emails()

        self.assertEqual(df.shape[0], 1)
        self.assertEqual(df.iloc[0]["debit"], "1300.50")

    @patch.object(email_test, "MailBox")
    def test_find_interac_emails_only_requires_subject_through_name(self, mailbox_mock):
        message = SimpleNamespace(
            subject=(
                "Interac e-Transfer: ADITHYA SREEJITHU PANICKER "
                "sent you $300.00 with unexpected wording"
            ),
            from_=email_test.INTERAC_SENDER,
            date=datetime(2026, 6, 18, 10, 0),
            text="",
        )
        mailbox_session = MagicMock()
        mailbox_session.fetch.return_value = [message]
        mailbox_mock.return_value.login.return_value.__enter__.return_value = (
            mailbox_session
        )

        with patch("builtins.print"):
            df = email_test.find_interac_emails()

        self.assertEqual(df.shape[0], 1)
        self.assertEqual(df.iloc[0]["debit"], "300.00")

    @patch.object(email_test, "MailBox")
    def test_find_interac_emails_returns_empty_frame_when_no_emails(
        self,
        mailbox_mock,
    ):
        mailbox_session = MagicMock()
        mailbox_session.fetch.return_value = []
        mailbox_mock.return_value.login.return_value.__enter__.return_value = (
            mailbox_session
        )

        df = email_test.find_interac_emails()

        self.assertTrue(df.empty)


if __name__ == "__main__":
    unittest.main()
