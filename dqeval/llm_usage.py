"""
llm_usage.py
Counts LLM calls and tokens, and records every request and response, without
changing what the calling code does (spec §6.4, §7.3).

    tracker = UsageTracker()
    with tracking(tracker):
        ...  # any code that calls openai.OpenAI(...).chat.completions.create(...)

While the context is open, openai.OpenAI is replaced by a thin wrapper around the
real client. That is how arm A3-v1 is measured without editing its frozen code.
"""

from contextlib import contextmanager
from dataclasses import dataclass, field

import openai


@dataclass
class UsageTracker:
    calls:         int = 0
    input_tokens:  int = 0
    output_tokens: int = 0
    models:        set = field(default_factory=set)
    exchanges:     list = field(default_factory=list)   # request messages and response text, in order

    def record(self, request: dict, response) -> None:
        self.calls += 1
        usage = getattr(response, "usage", None)
        if usage is not None:
            self.input_tokens  += getattr(usage, "prompt_tokens", 0) or 0
            self.output_tokens += getattr(usage, "completion_tokens", 0) or 0
        self.models.add(getattr(response, "model", None) or request.get("model"))
        self.exchanges.append({
            "request":  {k: v for k, v in request.items() if k in ("model", "messages", "response_format", "tools")},
            "response": response.choices[0].message.content if getattr(response, "choices", None) else None,
        })


class _Completions:
    def __init__(self, inner, tracker: UsageTracker):
        self._inner, self._tracker = inner, tracker

    def create(self, **kwargs):
        response = self._inner.create(**kwargs)
        self._tracker.record(kwargs, response)
        return response


class _Chat:
    def __init__(self, inner, tracker: UsageTracker):
        self.completions = _Completions(inner.completions, tracker)


class _TrackedClient:
    def __init__(self, real_cls, tracker: UsageTracker, *args, **kwargs):
        self._client = real_cls(*args, **kwargs)
        self.chat    = _Chat(self._client.chat, tracker)


@contextmanager
def tracking(tracker: UsageTracker):
    real_cls = openai.OpenAI
    openai.OpenAI = lambda *args, **kwargs: _TrackedClient(real_cls, tracker, *args, **kwargs)
    try:
        yield tracker
    finally:
        openai.OpenAI = real_cls
