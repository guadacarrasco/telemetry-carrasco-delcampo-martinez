import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambdas", "start_simulation"))

import handler as sim


class TestTrigger:
    def test_missing_session_key(self):
        event = {"body": json.dumps({"playback_seconds": 60})}
        resp = sim._trigger(event, MagicMock())
        assert resp["statusCode"] == 400

    def test_missing_playback_seconds(self):
        event = {"body": json.dumps({"session_key": 9662})}
        resp = sim._trigger(event, MagicMock())
        assert resp["statusCode"] == 400

    def test_invalid_json_body(self):
        event = {"body": "{not valid json"}
        resp = sim._trigger(event, MagicMock())
        assert resp["statusCode"] == 400

    def test_zero_playback_seconds(self):
        event = {"body": json.dumps({"session_key": 9662, "playback_seconds": 0})}
        resp = sim._trigger(event, MagicMock())
        assert resp["statusCode"] == 400

    def test_negative_playback_seconds(self):
        event = {"body": json.dumps({"session_key": 9662, "playback_seconds": -5})}
        resp = sim._trigger(event, MagicMock())
        assert resp["statusCode"] == 400

    def test_non_integer_values(self):
        event = {"body": json.dumps({"session_key": "abc", "playback_seconds": 300})}
        resp = sim._trigger(event, MagicMock())
        assert resp["statusCode"] == 400

    def test_returns_202_and_invokes_lambda_async(self):
        event = {"body": json.dumps({"session_key": 9662, "playback_seconds": 300})}
        context = MagicMock()
        context.function_name = "StartSimulationFunction"

        with patch.object(sim, "_lambda_client") as mock_client_factory:
            mock_lambda = MagicMock()
            mock_client_factory.return_value = mock_lambda

            resp = sim._trigger(event, context)

        assert resp["statusCode"] == 202
        body = json.loads(resp["body"])
        assert body["session_key"] == 9662
        assert body["playback_seconds"] == 300
        mock_lambda.invoke.assert_called_once()
        call_kwargs = mock_lambda.invoke.call_args[1]
        assert call_kwargs["InvocationType"] == "Event"
        payload = json.loads(call_kwargs["Payload"].decode())
        assert payload["_async"] is True
        assert payload["session_key"] == 9662

    def test_empty_body_treated_as_missing_fields(self):
        resp = sim._trigger({}, MagicMock())
        assert resp["statusCode"] == 400


class TestParseDate:
    def test_valid_iso_string(self):
        dt = sim._parse_date("2024-05-26T13:05:00+00:00")
        assert dt is not None
        assert dt.year == 2024
        assert dt.month == 5

    def test_z_suffix_normalized(self):
        dt = sim._parse_date("2024-05-26T13:05:00Z")
        assert dt is not None

    def test_empty_string_returns_none(self):
        assert sim._parse_date("") is None

    def test_invalid_string_returns_none(self):
        assert sim._parse_date("not-a-date") is None

    def test_none_like_empty_string(self):
        assert sim._parse_date("   ") is None


class TestProcess:
    def _make_lap(self, number, date_start, driver_number=1):
        return {
            "lap_number": number,
            "date_start": date_start,
            "lap_duration": 90.0,
            "position": 1,
            "sector_1": 30.0,
            "sector_2": 30.0,
            "sector_3": 30.0,
            "is_pit_out": False,
            "_driver_number": driver_number,
        }

    def test_session_not_found(self):
        with patch.object(sim, "SessionRepository") as MockSR, \
             patch.object(sim, "DriverStatsRepository"), \
             patch.object(sim, "LapsRepository"), \
             patch.object(sim, "_sqs_client"), \
             patch.dict(os.environ, {"SIMULATION_QUEUE_URL": "http://test"}):

            MockSR.return_value.get.side_effect = LookupError("not found")
            result = sim._process(9999, 60)

        assert result["ok"] is False
        assert "9999" in result["error"]

    def test_no_drivers_returns_error(self):
        with patch.object(sim, "SessionRepository") as MockSR, \
             patch.object(sim, "DriverStatsRepository") as MockDR, \
             patch.object(sim, "LapsRepository"), \
             patch.object(sim, "_sqs_client"), \
             patch.dict(os.environ, {"SIMULATION_QUEUE_URL": "http://test"}):

            MockSR.return_value.get.return_value = {"session_key": 9662}
            MockDR.return_value.list_for_session.return_value = []
            result = sim._process(9662, 60)

        assert result["ok"] is False

    def test_no_laps_returns_error(self):
        with patch.object(sim, "SessionRepository") as MockSR, \
             patch.object(sim, "DriverStatsRepository") as MockDR, \
             patch.object(sim, "LapsRepository") as MockLR, \
             patch.object(sim, "_sqs_client"), \
             patch.dict(os.environ, {"SIMULATION_QUEUE_URL": "http://test"}):

            MockSR.return_value.get.return_value = {"session_key": 9662}
            MockDR.return_value.list_for_session.return_value = [
                {"driver_number": 1, "full_name": "Max Verstappen", "acronym": "VER", "team_name": "RBR"}
            ]
            MockLR.return_value.list_for_driver.return_value = []
            result = sim._process(9662, 60)

        assert result["ok"] is False

    def test_publishes_events_to_sqs(self):
        laps = [
            self._make_lap(1, "2024-05-26T13:00:00Z"),
            self._make_lap(2, "2024-05-26T13:01:30Z"),
            self._make_lap(3, "2024-05-26T13:03:00Z"),
        ]

        with patch.object(sim, "SessionRepository") as MockSR, \
             patch.object(sim, "DriverStatsRepository") as MockDR, \
             patch.object(sim, "LapsRepository") as MockLR, \
             patch.object(sim, "_sqs_client") as mock_sqs_factory, \
             patch.dict(os.environ, {"SIMULATION_QUEUE_URL": "http://sqs/queue"}):

            MockSR.return_value.get.return_value = {"session_key": 9662}
            MockDR.return_value.list_for_session.return_value = [
                {"driver_number": 1, "full_name": "Max Verstappen", "acronym": "VER", "team_name": "RBR"}
            ]
            MockLR.return_value.list_for_driver.return_value = laps

            mock_sqs = MagicMock()
            mock_sqs_factory.return_value = mock_sqs

            result = sim._process(9662, 300)

        assert result["ok"] is True
        assert result["events_queued"] == 3
        assert mock_sqs.send_message.call_count == 3

    def test_sqs_delay_capped_at_900(self):
        laps = [
            self._make_lap(1, "2024-05-26T13:00:00Z"),
            self._make_lap(2, "2024-05-26T15:00:00Z"),  # 2h offset → massive delay
        ]

        with patch.object(sim, "SessionRepository") as MockSR, \
             patch.object(sim, "DriverStatsRepository") as MockDR, \
             patch.object(sim, "LapsRepository") as MockLR, \
             patch.object(sim, "_sqs_client") as mock_sqs_factory, \
             patch.dict(os.environ, {"SIMULATION_QUEUE_URL": "http://sqs/queue"}):

            MockSR.return_value.get.return_value = {"session_key": 9662}
            MockDR.return_value.list_for_session.return_value = [
                {"driver_number": 1, "full_name": "Max", "acronym": "VER", "team_name": "RBR"}
            ]
            MockLR.return_value.list_for_driver.return_value = laps

            mock_sqs = MagicMock()
            mock_sqs_factory.return_value = mock_sqs

            sim._process(9662, 10)

        calls = mock_sqs.send_message.call_args_list
        for call in calls:
            delay = call[1]["DelaySeconds"]
            assert delay <= sim.SQS_MAX_DELAY_SECONDS
