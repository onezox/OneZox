from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FallbackReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FALLBACK_REASON_UNSPECIFIED: _ClassVar[FallbackReason]
    FALLBACK_REASON_BREAKER_OPEN: _ClassVar[FallbackReason]
    FALLBACK_REASON_QUOTA_EXHAUSTED: _ClassVar[FallbackReason]

class BreakerState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BREAKER_STATE_UNSPECIFIED: _ClassVar[BreakerState]
    BREAKER_STATE_CLOSED: _ClassVar[BreakerState]
    BREAKER_STATE_OPEN: _ClassVar[BreakerState]
    BREAKER_STATE_HALF_OPEN: _ClassVar[BreakerState]
FALLBACK_REASON_UNSPECIFIED: FallbackReason
FALLBACK_REASON_BREAKER_OPEN: FallbackReason
FALLBACK_REASON_QUOTA_EXHAUSTED: FallbackReason
BREAKER_STATE_UNSPECIFIED: BreakerState
BREAKER_STATE_CLOSED: BreakerState
BREAKER_STATE_OPEN: BreakerState
BREAKER_STATE_HALF_OPEN: BreakerState

class ChatMessage(_message.Message):
    __slots__ = ("role", "content")
    ROLE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    role: str
    content: str
    def __init__(self, role: _Optional[str] = ..., content: _Optional[str] = ...) -> None: ...

class InvokeParams(_message.Message):
    __slots__ = ("max_tokens", "temperature")
    MAX_TOKENS_FIELD_NUMBER: _ClassVar[int]
    TEMPERATURE_FIELD_NUMBER: _ClassVar[int]
    max_tokens: int
    temperature: float
    def __init__(self, max_tokens: _Optional[int] = ..., temperature: _Optional[float] = ...) -> None: ...

class InvokeRequest(_message.Message):
    __slots__ = ("request_id", "worker_ref", "messages", "params")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    WORKER_REF_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    worker_ref: str
    messages: _containers.RepeatedCompositeFieldContainer[ChatMessage]
    params: InvokeParams
    def __init__(self, request_id: _Optional[str] = ..., worker_ref: _Optional[str] = ..., messages: _Optional[_Iterable[_Union[ChatMessage, _Mapping]]] = ..., params: _Optional[_Union[InvokeParams, _Mapping]] = ...) -> None: ...

class InvokeResponse(_message.Message):
    __slots__ = ("delta", "fallback")
    DELTA_FIELD_NUMBER: _ClassVar[int]
    FALLBACK_FIELD_NUMBER: _ClassVar[int]
    delta: Delta
    fallback: FallbackSignal
    def __init__(self, delta: _Optional[_Union[Delta, _Mapping]] = ..., fallback: _Optional[_Union[FallbackSignal, _Mapping]] = ...) -> None: ...

class Delta(_message.Message):
    __slots__ = ("request_id", "content", "finish_reason", "is_final", "prefix_cache_handle", "input_tokens", "output_tokens")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    FINISH_REASON_FIELD_NUMBER: _ClassVar[int]
    IS_FINAL_FIELD_NUMBER: _ClassVar[int]
    PREFIX_CACHE_HANDLE_FIELD_NUMBER: _ClassVar[int]
    INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    content: str
    finish_reason: str
    is_final: bool
    prefix_cache_handle: str
    input_tokens: int
    output_tokens: int
    def __init__(self, request_id: _Optional[str] = ..., content: _Optional[str] = ..., finish_reason: _Optional[str] = ..., is_final: bool = ..., prefix_cache_handle: _Optional[str] = ..., input_tokens: _Optional[int] = ..., output_tokens: _Optional[int] = ...) -> None: ...

class FallbackSignal(_message.Message):
    __slots__ = ("request_id", "provider", "reason")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    provider: str
    reason: FallbackReason
    def __init__(self, request_id: _Optional[str] = ..., provider: _Optional[str] = ..., reason: _Optional[_Union[FallbackReason, str]] = ...) -> None: ...

class InvokeEmbeddingRequest(_message.Message):
    __slots__ = ("worker_ref", "input")
    WORKER_REF_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    worker_ref: str
    input: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, worker_ref: _Optional[str] = ..., input: _Optional[_Iterable[str]] = ...) -> None: ...

class Embedding(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, values: _Optional[_Iterable[float]] = ...) -> None: ...

class InvokeEmbeddingResponse(_message.Message):
    __slots__ = ("embeddings",)
    EMBEDDINGS_FIELD_NUMBER: _ClassVar[int]
    embeddings: _containers.RepeatedCompositeFieldContainer[Embedding]
    def __init__(self, embeddings: _Optional[_Iterable[_Union[Embedding, _Mapping]]] = ...) -> None: ...

class ProviderHealthRequest(_message.Message):
    __slots__ = ("provider",)
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    provider: str
    def __init__(self, provider: _Optional[str] = ...) -> None: ...

class ProviderStatus(_message.Message):
    __slots__ = ("provider", "breaker_state", "quota_headroom")
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    BREAKER_STATE_FIELD_NUMBER: _ClassVar[int]
    QUOTA_HEADROOM_FIELD_NUMBER: _ClassVar[int]
    provider: str
    breaker_state: BreakerState
    quota_headroom: float
    def __init__(self, provider: _Optional[str] = ..., breaker_state: _Optional[_Union[BreakerState, str]] = ..., quota_headroom: _Optional[float] = ...) -> None: ...

class ProviderHealthResponse(_message.Message):
    __slots__ = ("statuses",)
    STATUSES_FIELD_NUMBER: _ClassVar[int]
    statuses: _containers.RepeatedCompositeFieldContainer[ProviderStatus]
    def __init__(self, statuses: _Optional[_Iterable[_Union[ProviderStatus, _Mapping]]] = ...) -> None: ...
