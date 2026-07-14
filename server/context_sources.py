"""Provider-neutral evidence sources and deterministic context selection."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from typing import (
    Any,
    Awaitable,
    Callable,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

import httpx

from . import kb
from .chart_serializer import render_chart
from .config import llm_config, querystore_config
from .querystore_client import QueryStoreClient

_CHART_MARKER = "Patient records (most recent first):"
_CHART_LINE = re.compile(r"^\[(\d+)]\s*(.*)$")
_DATE_PREFIX = re.compile(r"^\((\d{4}-\d{2}-\d{2})\)\s*")
_QUERY_TOKEN = re.compile(r"[A-Za-z0-9]+(?:[-_.:/][A-Za-z0-9]+)*")
_QUOTED = re.compile(r'["“]([^"”]+)["”]')
_CITATION_TOKEN = re.compile(r"(?<!\w)\[\d+\](?!\w)")


class ContextSourceError(RuntimeError):
    def __init__(self, code: str, message: str, *, source: str) -> None:
        self.code = code
        self.source = source
        super().__init__(message)


class InsufficientContextError(ContextSourceError):
    def __init__(self, message: str, *, mandatory_ids: Sequence[str]) -> None:
        self.mandatory_ids = tuple(mandatory_ids)
        super().__init__("insufficient_context", message, source="selector")


@dataclass(frozen=True)
class ContextRequest:
    messages: Sequence[Mapping[str, Any]]
    patient: Optional[str] = None
    source: Optional[str] = None
    sources: Tuple[str, ...] = ()
    supplemental_sources: Tuple[str, ...] = ()
    question: str = ""


@dataclass(frozen=True)
class EvidenceRecord:
    stable_id: str
    source: str
    source_priority: int
    resource_type: str
    resource_uuid: Optional[str]
    date: Optional[str]
    text: str
    mandatory: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict, compare=False)
    raw: Mapping[str, Any] = field(default_factory=dict, compare=False)

    def mapping(self, index: int) -> dict[str, Any]:
        title = str(self.metadata.get("title") or "").strip()
        if not title:
            title = self.text.splitlines()[0][:120]
        mapping = {
            "index": index,
            "sourceId": self.stable_id,
            "source": self.source,
            "resourceType": self.resource_type,
            "resourceUuid": self.resource_uuid,
            "date": self.date,
            "text": self.text,
            "title": title,
        }
        provenance = {
            key: self.metadata.get(key)
            for key in ("authority", "url", "version", "license")
            if self.metadata.get(key)
        }
        if provenance:
            mapping["provenance"] = provenance
        return mapping


def _render_records(
    records: Sequence[EvidenceRecord], indices: Optional[Sequence[int]] = None
) -> str:
    if not records:
        return ""
    record_indices = indices or range(1, len(records) + 1)
    def rendered_text(record: EvidenceRecord) -> str:
        if record.resource_type == "KnowledgeReference":
            return f"KnowledgeReference (source: {record.source}): {record.text}"
        return record.text

    return "\n".join(
        f"[{index}] {rendered_text(record)}"
        for index, record in zip(record_indices, records)
    ) + "\n"


@dataclass(frozen=True)
class EvidenceLedger:
    records: Tuple[EvidenceRecord, ...]
    original_text: str = field(default="", compare=False)
    preamble: str = field(default="", compare=False)

    @property
    def source_names(self) -> Tuple[str, ...]:
        return tuple(dict.fromkeys(record.source for record in self.records))

    def render(self) -> str:
        return (
            self.original_text
            if self.original_text
            else self.preamble + _render_records(self.records)
        )

    def mappings(self) -> list[dict[str, Any]]:
        return [record.mapping(index) for index, record in enumerate(self.records, 1)]

    def raw_records(self) -> list[dict[str, Any]]:
        return [dict(record.raw) for record in self.records if record.raw]


class ContextSource(Protocol):
    name: str
    priority: int
    supports_patient: bool

    async def fetch(self, request: ContextRequest) -> EvidenceLedger:
        ...


class TokenCounter(Protocol):
    async def count(self, model: str, text: str) -> int:
        ...


class ChatTokenCounter(TokenCounter, Protocol):
    async def count_chat(self, model: str, payload: Mapping[str, Any]) -> int:
        ...


@dataclass(frozen=True)
class ContextBudget:
    context_window: int
    reserved_output_tokens: int

    @property
    def input_limit(self) -> int:
        return self.context_window - self.reserved_output_tokens


@dataclass(frozen=True)
class ExcludedRecord:
    stable_id: str
    reason: str


@dataclass(frozen=True)
class IncludedRecord:
    stable_id: str
    reason: str


@dataclass(frozen=True)
class ContextView:
    records: Tuple[EvidenceRecord, ...]
    record_indices: Tuple[int, ...]
    mode: str
    included: Tuple[IncludedRecord, ...]
    excluded: Tuple[ExcludedRecord, ...]
    input_tokens: int
    input_limit: int
    original_text: str = field(default="", compare=False)
    preamble: str = field(default="", compare=False)

    @property
    def included_ids(self) -> Tuple[str, ...]:
        return tuple(item.stable_id for item in self.included)

    def render(self) -> str:
        if self.mode == "full" and self.original_text:
            return self.original_text
        return self.preamble + _render_records(self.records, self.record_indices)

    def mappings(self) -> list[dict[str, Any]]:
        return [
            record.mapping(index)
            for index, record in zip(self.record_indices, self.records)
        ]


class ContextSelector(Protocol):
    async def __call__(
        self,
        ledger: EvidenceLedger,
        *,
        question: str,
        model: str,
        budget: ContextBudget,
        counter: TokenCounter,
        fixed_text: str = "",
        input_measure: Optional[Callable[[str], Awaitable[int]]] = None,
    ) -> ContextView:
        ...


@dataclass(frozen=True)
class HistoryView:
    messages: Tuple[Mapping[str, Any], ...]
    dropped_turns: Tuple[str, ...]
    stripped_citation_tokens: int
    fixed_input_tokens: int


class InlineChartSource:
    name = "inline"
    priority = 10
    supports_patient = False

    async def fetch(self, request: ContextRequest) -> EvidenceLedger:
        chart = _inline_chart(request.messages)
        if not chart:
            raise ContextSourceError(
                "context_source_unavailable",
                "No inline chart was supplied in the request messages.",
                source=self.name,
            )
        records: list[EvidenceRecord] = []
        preamble_lines: list[str] = []
        carried_date: Optional[str] = None
        saw_record = False
        for line in chart.splitlines():
            match = _CHART_LINE.match(line.strip())
            if not match:
                if not saw_record:
                    preamble_lines.append(line)
                continue
            saw_record = True
            original_index, text = match.groups()
            date_match = _DATE_PREFIX.match(text)
            if date_match:
                carried_date = date_match.group(1)
            records.append(
                EvidenceRecord(
                    stable_id=f"inline:{original_index}",
                    source=self.name,
                    source_priority=self.priority,
                    resource_type="ChartRecord",
                    resource_uuid=None,
                    date=carried_date,
                    text=text,
                )
            )
        if not records:
            records.append(
                EvidenceRecord(
                    stable_id="inline:1",
                    source=self.name,
                    source_priority=self.priority,
                    resource_type="ChartRecord",
                    resource_uuid=None,
                    date=None,
                    text=chart.strip(),
                )
            )
            preamble_lines = []
        normalized = chart if chart.endswith("\n") else chart + "\n"
        preamble = "\n".join(preamble_lines)
        if preamble:
            preamble += "\n"
        return EvidenceLedger(
            tuple(records), original_text=normalized, preamble=preamble
        )


class StaticKnowledgeSource:
    """Optional adapter over the hub's provenance-bearing clinical KB."""

    name = "knowledge-base"
    priority = 20
    supports_patient = False

    async def fetch(self, request: ContextRequest) -> EvidenceLedger:
        query = request.question.strip()
        rows = kb.search(query, k=3)
        prior_answer = next(
            (
                str(message.get("content") or "").strip()
                for message in reversed(request.messages)
                if message.get("role") == "assistant"
                and str(message.get("content") or "").strip()
            ),
            "",
        )
        prior_answer = _CITATION_TOKEN.sub("", prior_answer).strip()
        if prior_answer:
            expanded_query = (
                f"{query}\nPrior answer context: {prior_answer[:1200]}".strip()
            )
            expanded_rows = kb.search(expanded_query, k=3)
            # Protect current-topic recall while reserving one slot for anaphoric follow-ups.
            # Stable-id deduplication and fixed lane order keep the merge deterministic.
            merged: list[dict[str, Any]] = []
            seen_ids: set[str] = set()

            def add(candidates: Sequence[Mapping[str, Any]], limit: int) -> None:
                for candidate in candidates:
                    source_id = str(candidate.get("id") or "")
                    if not source_id or source_id in seen_ids:
                        continue
                    seen_ids.add(source_id)
                    merged.append(dict(candidate))
                    if len(merged) >= limit:
                        return

            add(rows[:2], 2)
            add(expanded_rows, min(3, len(merged) + 1))
            add(rows[2:], 3)
            rows = merged[:3]
        records = []
        for position, row in enumerate(rows, 1):
            source_id = str(row.get("id") or position)
            title = str(row.get("title") or "Clinical reference")
            body = str(row.get("text") or "")
            provenance = str(row.get("source") or "").strip()
            text = f"{title}: {body}"
            if provenance:
                text += f" (source: {provenance})"
            records.append(
                EvidenceRecord(
                    stable_id=f"knowledge-base:{source_id}",
                    source=self.name,
                    source_priority=self.priority,
                    resource_type="KnowledgeReference",
                    resource_uuid=source_id,
                    date=None,
                    text=text,
                    metadata={
                        "authority": row.get("source"),
                        "url": row.get("url"),
                        "version": row.get("version"),
                        "license": row.get("license"),
                    },
                    raw=row,
                )
            )
        return EvidenceLedger(tuple(records))


class QueryStoreSource:
    name = "querystore"
    priority = 50
    supports_patient = True

    def __init__(self, client: QueryStoreClient) -> None:
        self.client = client

    async def fetch(self, request: ContextRequest) -> EvidenceLedger:
        if not request.patient:
            raise ContextSourceError(
                "context_source_unavailable",
                "Querystore requires a patient identifier.",
                source=self.name,
            )
        try:
            raw_records = await self.client.get_patient_chart(request.patient)
        except Exception as exc:
            raise ContextSourceError(
                "context_source_failed",
                f"Querystore could not retrieve patient {request.patient!r}: {exc}",
                source=self.name,
            ) from exc
        chart, mappings = render_chart(raw_records)
        records: list[EvidenceRecord] = []
        raw_by_key = {
            (record.get("resourceType"), record.get("resourceUuid")): record
            for record in raw_records
            if record and record.get("resourceType") and record.get("resourceUuid")
        }
        for mapping in mappings:
            raw = raw_by_key.get(
                (mapping.get("resourceType"), mapping.get("resourceUuid")), {}
            )
            stable_id = f"querystore:{mapping.get('resourceType')}:{mapping.get('resourceUuid')}"
            metadata = raw.get("metadata") or {}
            records.append(
                EvidenceRecord(
                    stable_id=stable_id,
                    source=self.name,
                    source_priority=self.priority,
                    resource_type=str(mapping.get("resourceType") or "Record"),
                    resource_uuid=mapping.get("resourceUuid"),
                    date=mapping.get("date"),
                    text=str(mapping.get("text") or ""),
                    mandatory=bool(metadata.get("mandatory_context")),
                    metadata=metadata,
                    raw=raw,
                )
            )
        return EvidenceLedger(tuple(records), original_text=chart)


class SourceRegistry:
    def __init__(self, sources: Iterable[ContextSource]) -> None:
        self._sources = {source.name: source for source in sources}
        if "inline" not in self._sources:
            self._sources["inline"] = InlineChartSource()

    @classmethod
    def default(cls) -> "SourceRegistry":
        sources: list[ContextSource] = [InlineChartSource(), StaticKnowledgeSource()]
        if querystore_config.enabled:
            sources.append(
                QueryStoreSource(
                    QueryStoreClient(
                        querystore_config.base_url,
                        querystore_config.username,
                        querystore_config.password,
                    )
                )
            )
        return cls(sources)

    async def build_ledger(self, request: ContextRequest) -> EvidenceLedger:
        sources = self._resolve(request)
        ledgers = [await source.fetch(request) for source in sources]
        records = tuple(record for ledger in ledgers for record in ledger.records)
        stable_ids = [record.stable_id for record in records]
        duplicate_ids = sorted(
            source_id for source_id, count in Counter(stable_ids).items() if count > 1
        )
        if duplicate_ids:
            raise ContextSourceError(
                "context_source_failed",
                f"Context sources emitted duplicate stable ids: {duplicate_ids}",
                source="registry",
            )
        original_text = ledgers[0].original_text if len(ledgers) == 1 else ""
        preamble = "".join(ledger.preamble for ledger in ledgers if ledger.preamble)
        return EvidenceLedger(records, original_text=original_text, preamble=preamble)

    def _resolve(self, request: ContextRequest) -> Tuple[ContextSource, ...]:
        requested = request.sources or ((request.source,) if request.source else ())
        if requested:
            primary_names = tuple(dict.fromkeys(requested))
        elif request.patient:
            patient_sources = sorted(
                (
                    source
                    for source in self._sources.values()
                    if source.supports_patient
                ),
                key=lambda source: (-source.priority, source.name),
            )
            if patient_sources:
                primary_names = (patient_sources[0].name,)
            elif _inline_chart(request.messages):
                primary_names = ("inline",)
            else:
                raise ContextSourceError(
                    "context_source_unavailable",
                    "A patient identifier was supplied, but no patient context source is configured.",
                    source="auto",
                )
        else:
            primary_names = ("inline",)

        resolved = []
        for source_name in dict.fromkeys(
            (*primary_names, *request.supplemental_sources)
        ):
            source = self._sources.get(source_name)
            if source is None:
                raise ContextSourceError(
                    "context_source_unavailable",
                    f"Context source {source_name!r} is not configured.",
                    source=source_name,
                )
            resolved.append(source)
        return tuple(resolved)


class RouterTokenCounter:
    """Exact token count from the configured llama.cpp-compatible router."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        *,
        timeout: float = 15.0,
    ) -> None:
        self.base_url = (base_url or llm_config.base_url).rstrip("/")
        self.api_key = api_key if api_key is not None else llm_config.api_key
        self.timeout = timeout

    async def count(self, model: str, text: str) -> int:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        payload = {"model": model, "content": text, "add_special": False}
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/tokenize", json=payload, headers=headers
                )
                response.raise_for_status()
                body = response.json()
        except Exception as exc:
            raise ContextSourceError(
                "tokenization_unavailable",
                f"Exact tokenizer unavailable for model {model!r}: {exc}",
                source="llama-router",
            ) from exc
        tokens = body.get("tokens")
        if isinstance(tokens, list):
            return len(tokens)
        for key in ("count", "n_tokens"):
            if isinstance(body.get(key), int):
                return int(body[key])
        raise ContextSourceError(
            "tokenization_unavailable",
            f"Tokenizer response for model {model!r} had no token count.",
            source="llama-router",
        )

    async def count_chat(self, model: str, payload: Mapping[str, Any]) -> int:
        """Count the exact model-templated request accepted by llama.cpp.

        Newer servers expose a direct chat input-token endpoint. Older supported
        router builds expose the equivalent two-step operation: apply the model's
        chat template, then tokenize that rendered prompt.
        """
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        body = dict(payload)
        body["model"] = model
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/v1/chat/completions/input_tokens",
                    json=body,
                    headers=headers,
                )
                if response.status_code == 404:
                    # ``response_format`` is an out-of-band generation grammar in
                    # llama.cpp, not chat-template input. /apply-template accepts only
                    # the fields below; excluding the schema therefore preserves the
                    # exact prompt-token count rather than approximating it.
                    template_body = {
                        key: body[key]
                        for key in ("model", "messages", "tools", "tool_choice")
                        if key in body
                    }
                    template = await client.post(
                        f"{self.base_url}/apply-template",
                        json=template_body,
                        headers=headers,
                    )
                    template.raise_for_status()
                    prompt = template.json().get("prompt")
                    if not isinstance(prompt, str):
                        raise ValueError("apply-template response had no prompt")
                    tokenized = await client.post(
                        f"{self.base_url}/tokenize",
                        json={
                            "model": model,
                            "content": prompt,
                            "add_special": False,
                            "parse_special": True,
                        },
                        headers=headers,
                    )
                    tokenized.raise_for_status()
                    tokens = tokenized.json().get("tokens")
                    if isinstance(tokens, list):
                        return len(tokens)
                    raise ValueError("tokenize response had no tokens")
                response.raise_for_status()
                result = response.json()
        except Exception as exc:
            raise ContextSourceError(
                "tokenization_unavailable",
                f"Exact chat-template token count unavailable for model {model!r}: {exc}",
                source="llama-router",
            ) from exc
        count = result.get("input_tokens")
        if isinstance(count, int) and count >= 0:
            return count
        raise ContextSourceError(
            "tokenization_unavailable",
            f"Chat token-count response for model {model!r} had no input_tokens.",
            source="llama-router",
        )


def _inline_chart(messages: Sequence[Mapping[str, Any]]) -> str:
    user_contents: list[str] = []
    for message in messages:
        content = message.get("content")
        if message.get("role") != "user" or not isinstance(content, str):
            continue
        user_contents.append(content)
        marker = content.find(_CHART_MARKER)
        if marker >= 0:
            return content[marker + len(_CHART_MARKER) :].lstrip("\r\n")
        if any(_CHART_LINE.match(line.strip()) for line in content.splitlines()):
            return content
    if len(user_contents) >= 2:
        return user_contents[0]
    return ""


def is_chart_message(message: Mapping[str, Any]) -> bool:
    content = message.get("content")
    if message.get("role") != "user" or not isinstance(content, str):
        return False
    return _CHART_MARKER in content or any(
        _CHART_LINE.match(line.strip()) for line in content.splitlines()
    )


async def fit_message_history(
    messages: Sequence[Mapping[str, Any]],
    *,
    model: str,
    budget: ContextBudget,
    counter: TokenCounter,
    fixed_renderer: Callable[[Sequence[Mapping[str, Any]]], str],
    mandatory_text: str = "",
    mandatory_ids: Sequence[str] = (),
    input_measure: Optional[
        Callable[[Sequence[Mapping[str, Any]]], Awaitable[int]]
    ] = None,
) -> HistoryView:
    """Fit prior turns while preserving the current question and latest completed turn."""

    normalized = [dict(message) for message in messages]
    conversation = [
        index
        for index, message in enumerate(normalized)
        if message.get("role") in {"user", "assistant"}
        and not is_chart_message(message)
    ]
    current_user = next(
        (
            index
            for index in reversed(conversation)
            if normalized[index].get("role") == "user"
        ),
        None,
    )
    stripped = 0
    if current_user is not None:
        for index, message in enumerate(normalized):
            if index >= current_user or message.get("role") != "assistant":
                continue
            content = message.get("content")
            if not isinstance(content, str):
                continue
            cleaned, count = _CITATION_TOKEN.subn("", content)
            stripped += count
            message["content"] = re.sub(r"[ \t]{2,}", " ", cleaned).strip()

    completed: list[tuple[str, tuple[int, ...]]] = []
    active: list[int] = []
    turn_number = 0
    for index in conversation:
        if current_user is not None and index >= current_user:
            break
        role = normalized[index].get("role")
        if role == "user":
            if active and any(
                normalized[item].get("role") == "assistant" for item in active
            ):
                turn_number += 1
                completed.append((f"turn:{turn_number}", tuple(active)))
            active = [index]
        elif active:
            active.append(index)
    if active and any(normalized[item].get("role") == "assistant" for item in active):
        turn_number += 1
        completed.append((f"turn:{turn_number}", tuple(active)))

    def input_text(candidate: Sequence[Mapping[str, Any]]) -> str:
        fixed = fixed_renderer(candidate)
        return fixed + ("\n" if fixed and mandatory_text else "") + mandatory_text

    current_tokens = (
        await input_measure(normalized)
        if input_measure is not None
        else await counter.count(model, input_text(normalized))
    )
    removed: set[int] = set()
    dropped: list[str] = []
    # The most recent completed turn is protected. Older complete turns are dropped oldest-first.
    for turn_id, indices in completed[:-1]:
        if current_tokens <= budget.input_limit:
            break
        removed.update(indices)
        dropped.append(turn_id)
        candidate = [
            message for index, message in enumerate(normalized) if index not in removed
        ]
        current_tokens = (
            await input_measure(candidate)
            if input_measure is not None
            else await counter.count(model, input_text(candidate))
        )

    fitted = tuple(
        message for index, message in enumerate(normalized) if index not in removed
    )
    if current_tokens > budget.input_limit:
        raise InsufficientContextError(
            "The current question, latest completed turn, prompts, and mandatory evidence "
            "exceed the exact model input budget.",
            mandatory_ids=mandatory_ids,
        )
    return HistoryView(
        messages=fitted,
        dropped_turns=tuple(dropped),
        stripped_citation_tokens=stripped,
        fixed_input_tokens=current_tokens,
    )


def _query_features(question: str) -> tuple[set[str], set[str]]:
    tokens = {token.lower() for token in _QUERY_TOKEN.findall(question or "")}
    exact = {
        token.lower()
        for token in _QUERY_TOKEN.findall(question or "")
        if any(character.isdigit() for character in token)
        or any(separator in token for separator in ("-", "_", ":", "/"))
    }
    exact.update(phrase.lower().strip() for phrase in _QUOTED.findall(question or ""))
    return tokens, {item for item in exact if item}


def _ranked_records(
    records: Sequence[EvidenceRecord], question: str
) -> list[tuple[EvidenceRecord, str]]:
    query_tokens, exact_terms = _query_features(question)

    def features(record: EvidenceRecord) -> tuple[int, int]:
        text = record.text.lower()
        ordered_record_tokens = [token.lower() for token in _QUERY_TOKEN.findall(text)]
        record_tokens = set(ordered_record_tokens)

        def exact_match(term: str) -> bool:
            term_tokens = [token.lower() for token in _QUERY_TOKEN.findall(term)]
            if not term_tokens:
                return False
            width = len(term_tokens)
            return any(
                ordered_record_tokens[index : index + width] == term_tokens
                for index in range(len(ordered_record_tokens) - width + 1)
            )

        exact = int(
            any(exact_match(term) for term in exact_terms)
        )
        overlap = len(query_tokens & record_tokens)
        return exact, overlap

    def recency(record: EvidenceRecord) -> int:
        try:
            return int((record.date or "").replace("-", ""))
        except ValueError:
            return 0

    mandatory = [(record, "mandatory") for record in records if record.mandatory]
    remaining = [record for record in records if not record.mandatory]
    exact = [record for record in remaining if features(record)[0]]
    rest = [record for record in remaining if record not in exact]
    exact.sort(
        key=lambda record: (
            -features(record)[1],
            -recency(record),
            -record.source_priority,
            record.stable_id,
        )
    )
    rest.sort(
        key=lambda record: (
            -features(record)[1],
            -recency(record),
            -record.source_priority,
            record.stable_id,
        )
    )
    return (
        mandatory
        + [(record, "exact_match") for record in exact]
        + [(record, "ranked") for record in rest]
    )


async def select_context(
    ledger: EvidenceLedger,
    *,
    question: str,
    model: str,
    budget: ContextBudget,
    counter: TokenCounter,
    fixed_text: str = "",
    input_measure: Optional[Callable[[str], Awaitable[int]]] = None,
) -> ContextView:
    if budget.input_limit <= 0:
        raise InsufficientContextError(
            "Reserved output tokens consume the entire context window.",
            mandatory_ids=tuple(
                record.stable_id for record in ledger.records if record.mandatory
            ),
        )

    full_text = (
        fixed_text + ("\n" if fixed_text and ledger.render() else "") + ledger.render()
    )
    full_tokens = (
        await input_measure(ledger.render())
        if input_measure is not None
        else await counter.count(model, full_text)
    )
    if full_tokens <= budget.input_limit:
        return ContextView(
            records=ledger.records,
            record_indices=tuple(range(1, len(ledger.records) + 1)),
            mode="full",
            included=tuple(
                IncludedRecord(record.stable_id, "full_context")
                for record in ledger.records
            ),
            excluded=(),
            input_tokens=full_tokens,
            input_limit=budget.input_limit,
            original_text=ledger.render(),
            preamble=ledger.preamble,
        )

    ranked = _ranked_records(ledger.records, question)
    ranked_reasons = {record.stable_id: reason for record, reason in ranked}
    source_indices = {
        id(record): index for index, record in enumerate(ledger.records, 1)
    }
    mandatory = [record for record, reason in ranked if reason == "mandatory"]
    mandatory_text = ledger.preamble + _render_records(
        mandatory, [source_indices[id(record)] for record in mandatory]
    )
    mandatory_input = (
        fixed_text + ("\n" if fixed_text and mandatory_text else "") + mandatory_text
    )
    mandatory_tokens = (
        await input_measure(mandatory_text)
        if input_measure is not None
        else await counter.count(model, mandatory_input)
    )
    if mandatory_tokens > budget.input_limit:
        raise InsufficientContextError(
            "Mandatory evidence exceeds the exact model input budget.",
            mandatory_ids=tuple(record.stable_id for record in mandatory),
        )

    selected = list(mandatory)
    excluded: list[ExcludedRecord] = []
    current_tokens = mandatory_tokens
    mandatory_ids = {record.stable_id for record in mandatory}
    for record, reason in ranked:
        if record.stable_id in mandatory_ids:
            continue
        trial_records = selected + [record]
        trial_chart = ledger.preamble + _render_records(
            trial_records, [source_indices[id(item)] for item in trial_records]
        )
        trial_input = (
            fixed_text + ("\n" if fixed_text and trial_chart else "") + trial_chart
        )
        trial_tokens = (
            await input_measure(trial_chart)
            if input_measure is not None
            else await counter.count(model, trial_input)
        )
        if trial_tokens <= budget.input_limit:
            selected.append(record)
            current_tokens = trial_tokens
        else:
            excluded.append(
                ExcludedRecord(
                    record.stable_id,
                    f"token_budget_after_{reason}",
                )
            )

    return ContextView(
        records=tuple(selected),
        record_indices=tuple(source_indices[id(record)] for record in selected),
        mode="selected",
        included=tuple(
            IncludedRecord(record.stable_id, ranked_reasons[record.stable_id])
            for record in selected
        ),
        excluded=tuple(excluded),
        input_tokens=current_tokens,
        input_limit=budget.input_limit,
        preamble=ledger.preamble,
    )
