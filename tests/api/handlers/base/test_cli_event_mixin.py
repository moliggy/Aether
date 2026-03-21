from typing import Any

from src.api.handlers.base.cli_event_mixin import CliEventMixin
from src.api.handlers.base.stream_context import StreamContext


class _DummyCliEventHandler(CliEventMixin):
    request_id = "req-test"

    def _process_event_data(
        self,
        ctx: StreamContext,
        event_type: str,
        data: dict[str, Any],
    ) -> None:
        del ctx, event_type, data


def test_convert_sse_line_drops_done_for_openai_cli() -> None:
    handler = _DummyCliEventHandler()
    ctx = StreamContext(model="gpt-test", api_format="openai:cli")
    ctx.client_api_format = "openai:cli"

    lines, converted = handler._convert_sse_line(ctx, "data: [DONE]", [])

    assert lines == []
    assert converted == []


def test_convert_sse_line_keeps_done_for_openai_chat() -> None:
    handler = _DummyCliEventHandler()
    ctx = StreamContext(model="gpt-test", api_format="openai:chat")
    ctx.client_api_format = "openai:chat"

    lines, converted = handler._convert_sse_line(ctx, "data: [DONE]", [])

    assert lines == ["data: [DONE]"]
    assert converted == []
