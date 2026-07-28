from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ChatMessage(_message.Message):
    __slots__ = ("role", "content")
    ROLE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    role: str
    content: str
    def __init__(self, role: _Optional[str] = ..., content: _Optional[str] = ...) -> None: ...

class ChatCompletionRequest(_message.Message):
    __slots__ = ("model", "messages", "stream", "max_tokens", "temperature")
    MODEL_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    STREAM_FIELD_NUMBER: _ClassVar[int]
    MAX_TOKENS_FIELD_NUMBER: _ClassVar[int]
    TEMPERATURE_FIELD_NUMBER: _ClassVar[int]
    model: str
    messages: _containers.RepeatedCompositeFieldContainer[ChatMessage]
    stream: bool
    max_tokens: int
    temperature: float
    def __init__(self, model: _Optional[str] = ..., messages: _Optional[_Iterable[_Union[ChatMessage, _Mapping]]] = ..., stream: bool = ..., max_tokens: _Optional[int] = ..., temperature: _Optional[float] = ...) -> None: ...

class ChatCompletionChoice(_message.Message):
    __slots__ = ("index", "message", "finish_reason")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    FINISH_REASON_FIELD_NUMBER: _ClassVar[int]
    index: int
    message: ChatMessage
    finish_reason: str
    def __init__(self, index: _Optional[int] = ..., message: _Optional[_Union[ChatMessage, _Mapping]] = ..., finish_reason: _Optional[str] = ...) -> None: ...

class ChatCompletionResponse(_message.Message):
    __slots__ = ("id", "model", "choices")
    ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    CHOICES_FIELD_NUMBER: _ClassVar[int]
    id: str
    model: str
    choices: _containers.RepeatedCompositeFieldContainer[ChatCompletionChoice]
    def __init__(self, id: _Optional[str] = ..., model: _Optional[str] = ..., choices: _Optional[_Iterable[_Union[ChatCompletionChoice, _Mapping]]] = ...) -> None: ...

class ChatCompletionChunkDelta(_message.Message):
    __slots__ = ("role", "content")
    ROLE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    role: str
    content: str
    def __init__(self, role: _Optional[str] = ..., content: _Optional[str] = ...) -> None: ...

class ChatCompletionChunkChoice(_message.Message):
    __slots__ = ("index", "delta", "finish_reason")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    DELTA_FIELD_NUMBER: _ClassVar[int]
    FINISH_REASON_FIELD_NUMBER: _ClassVar[int]
    index: int
    delta: ChatCompletionChunkDelta
    finish_reason: str
    def __init__(self, index: _Optional[int] = ..., delta: _Optional[_Union[ChatCompletionChunkDelta, _Mapping]] = ..., finish_reason: _Optional[str] = ...) -> None: ...

class ChatCompletionChunk(_message.Message):
    __slots__ = ("id", "model", "choices")
    ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    CHOICES_FIELD_NUMBER: _ClassVar[int]
    id: str
    model: str
    choices: _containers.RepeatedCompositeFieldContainer[ChatCompletionChunkChoice]
    def __init__(self, id: _Optional[str] = ..., model: _Optional[str] = ..., choices: _Optional[_Iterable[_Union[ChatCompletionChunkChoice, _Mapping]]] = ...) -> None: ...

class ResponsesRequest(_message.Message):
    __slots__ = ("model", "input", "stream")
    MODEL_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    STREAM_FIELD_NUMBER: _ClassVar[int]
    model: str
    input: str
    stream: bool
    def __init__(self, model: _Optional[str] = ..., input: _Optional[str] = ..., stream: bool = ...) -> None: ...

class ResponsesResponse(_message.Message):
    __slots__ = ("id", "model", "output_text")
    ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_TEXT_FIELD_NUMBER: _ClassVar[int]
    id: str
    model: str
    output_text: str
    def __init__(self, id: _Optional[str] = ..., model: _Optional[str] = ..., output_text: _Optional[str] = ...) -> None: ...

class EmbeddingsRequest(_message.Message):
    __slots__ = ("model", "input")
    MODEL_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    model: str
    input: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, model: _Optional[str] = ..., input: _Optional[_Iterable[str]] = ...) -> None: ...

class Embedding(_message.Message):
    __slots__ = ("index", "values")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    index: int
    values: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, index: _Optional[int] = ..., values: _Optional[_Iterable[float]] = ...) -> None: ...

class EmbeddingsResponse(_message.Message):
    __slots__ = ("model", "data")
    MODEL_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    model: str
    data: _containers.RepeatedCompositeFieldContainer[Embedding]
    def __init__(self, model: _Optional[str] = ..., data: _Optional[_Iterable[_Union[Embedding, _Mapping]]] = ...) -> None: ...

class Model(_message.Message):
    __slots__ = ("id", "owned_by")
    ID_FIELD_NUMBER: _ClassVar[int]
    OWNED_BY_FIELD_NUMBER: _ClassVar[int]
    id: str
    owned_by: str
    def __init__(self, id: _Optional[str] = ..., owned_by: _Optional[str] = ...) -> None: ...

class ModelsListResponse(_message.Message):
    __slots__ = ("data",)
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: _containers.RepeatedCompositeFieldContainer[Model]
    def __init__(self, data: _Optional[_Iterable[_Union[Model, _Mapping]]] = ...) -> None: ...
