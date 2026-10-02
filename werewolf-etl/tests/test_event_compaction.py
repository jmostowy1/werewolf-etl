"""From the bundle root: python -m unittest discover -s tests -p test_event_compaction.py

Loads the actual pure parser functions without requiring PySpark/Databricks.
"""

import ast
import copy
import hashlib
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "src" / "werewolf_pipeline.py").read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)
CONSTANTS = {"EVENT_PAYLOAD_TEXT_FIELDS", "ACTION_TELEMETRY_EVENTS"}
NAMESPACE = {
    "json": json, "hashlib": hashlib,
    "SCHEMAS": dict.fromkeys(("episodes", "players", "actions", "events")),
}
NODES = [
    node for node in TREE.body
    if (isinstance(node, ast.FunctionDef) and not node.decorator_list)
    or (isinstance(node, ast.Assign) and any(
        isinstance(target, ast.Name) and target.id in CONSTANTS
        for target in node.targets))
]
exec(compile(ast.Module(body=NODES, type_ignores=[]),
             str(ROOT / "src" / "werewolf_pipeline.py"), "exec"), NAMESPACE)


class EventCompactionTests(unittest.TestCase):
    def test_nested_payload_and_input_immutability(self):
        original = {
            "actor_id": "Alex", "target_id": "Jordan", "message": "hello",
            "reason": "game ended", "raw_prompt": "large", "error": "traceback",
            "nested": [{"reasoning": "large", "error": None, "future_field": 7}],
            "action_json_schema": {"large": True}, "valid_targets": ["Alex"],
        }
        before = copy.deepcopy(original)
        result = NAMESPACE["compact_event_payload"](original)
        self.assertEqual(original, before)
        self.assertEqual(result, {
            "actor_id": "Alex", "target_id": "Jordan", "message": "hello",
            "reason": "game ended", "has_error": True,
            "nested": [{"has_error": False, "future_field": 7}],
            "valid_targets": ["Alex"],
        })
        self.assertIsNone(NAMESPACE["compact_event_payload"](None))
        self.assertEqual(NAMESPACE["compact_event_payload"]([]), [])

    def test_descriptions(self):
        retain = NAMESPACE["retained_event_description"]
        self.assertIsNone(retain("dict", {
            "event_name": "no_op_action", "description": "traceback", "data": {},
        }))
        self.assertEqual(retain("NoneType", {
            "event_name": "vote_request", "description": "tally and options", "data": None,
        }), "tally and options")
        self.assertEqual(retain("ChatDataEntry", {
            "event_name": "discussion", "description": "public chat",
            "data": {"message": "hello"},
        }), "public chat")
        self.assertIsNone(retain("FutureTelemetry", {
            "event_name": "future_action", "description": "large prompt",
            "data": {"raw_prompt": "large"},
        }))

    def test_sample_preserves_events_and_action_matches(self):
        sample = ROOT / "tests" / "fixtures" / "74788868.json"
        content = sample.read_bytes()
        root = json.loads(content)
        output = NAMESPACE["normalize_episode"](content, str(sample))
        self.assertEqual(len(output["events"]), 290)
        self.assertEqual(len(output["actions"]), 53)
        self.assertEqual(len(output["players"]), 8)
        removed_descriptions = 0
        for event in output["events"]:
            original = json.loads(root["info"]["MODERATOR_OBSERVATION"]
                                  [event["batch_index"]][event["entry_index"]]["json_str"])
            self.assertNotIn("event_json", event)
            for field in ("event_name", "source", "day", "phase", "detailed_phase",
                          "public", "visible_to", "visible_in_ui"):
                self.assertEqual(event[field], original.get(field))
            self.assertEqual(event["created_at_raw"], original.get("created_at"))
            if event["description"] is None and original.get("description"):
                removed_descriptions += 1
            payload = json.loads(event["data_json"])
            if isinstance(original.get("data"), dict):
                for field in ("actor_id", "target_id", "message", "role", "team",
                              "mentioned_player_ids", "vote_order_of_player_ids",
                              "chat_order_of_player_ids", "valid_candidates",
                              "valid_targets", "elected_target_player_id",
                              "elected_player_id", "eliminated_player_id", "saved_player_id"):
                    self.assertEqual(payload.get(field), original["data"].get(field))
            self.assert_payload_is_compact(payload)
        self.assertEqual(removed_descriptions, 48)
        mapping = {
            "ChatAction": ("discussion", "dict"),
            "VoteAction": ("vote_action", "dict"),
            "EliminateProposalAction": ("eliminate_proposal_action", "dict"),
            "HealAction": ("heal_action", "DoctorHealActionDataEntry"),
            "InspectAction": ("inspect_action", "SeerInspectActionDataEntry"),
            "NoOpAction": ("no_op_action", "dict"),
        }
        for action in output["actions"]:
            matches = []
            for event in output["events"]:
                if (event["batch_index"] != action["step_index"]
                    or (event["event_name"], event["data_type"]) != mapping[action["action_type"]]):
                    continue
                data = json.loads(event["data_json"])
                if isinstance(data, dict) and all(
                    data.get(key) == action.get(key)
                    for key in ("actor_id", "target_id", "message")
                ):
                    matches.append(event)
            self.assertEqual(len(matches), 1, (action["step_index"], action["seat_index"]))
        # Check the declared Spark schema as well as the returned dictionaries.
        schemas_node = next(node for node in TREE.body if isinstance(node, ast.Assign)
                            and any(isinstance(t, ast.Name) and t.id == "SCHEMAS"
                                    for t in node.targets))
        self.assertNotIn("event_json", ast.get_source_segment(SOURCE, schemas_node))

    def assert_payload_is_compact(self, value):
        if isinstance(value, dict):
            self.assertFalse(set(value) & (NAMESPACE["EVENT_PAYLOAD_TEXT_FIELDS"] | {"error"}))
            for item in value.values():
                self.assert_payload_is_compact(item)
        elif isinstance(value, list):
            for item in value:
                self.assert_payload_is_compact(item)


if __name__ == "__main__":
    unittest.main()
