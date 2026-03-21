import json

from src.api.handlers.base.cli_sse_helpers import _format_converted_events_to_sse


def test_format_converted_events_to_sse_uses_event_lines_for_openai_cli() -> None:
    events = [
        {
            "type": "response.output_text.delta",
            "item_id": "msg_123",
            "output_index": 0,
            "content_index": 0,
            "delta": "Hi",
            "logprobs": [],
            "sequence_number": 1,
        }
    ]

    lines = _format_converted_events_to_sse(events, "openai:cli")

    assert lines == [
        "event: response.output_text.delta\n"
        f"data: {json.dumps(events[0], ensure_ascii=False)}\n"
    ]


def test_format_converted_events_to_sse_keeps_data_only_for_openai_chat() -> None:
    events = [{"id": "chatcmpl-123", "object": "chat.completion.chunk", "choices": []}]

    lines = _format_converted_events_to_sse(events, "openai:chat")

    assert lines == [f"data: {json.dumps(events[0], ensure_ascii=False)}\n"]
