from gateway.v1 import gateway_pb2 as _gateway_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RequestKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REQUEST_KIND_UNSPECIFIED: _ClassVar[RequestKind]
    REQUEST_KIND_CHAT_COMPLETION: _ClassVar[RequestKind]
    REQUEST_KIND_RESPONSES: _ClassVar[RequestKind]
    REQUEST_KIND_EMBEDDINGS: _ClassVar[RequestKind]
REQUEST_KIND_UNSPECIFIED: RequestKind
REQUEST_KIND_CHAT_COMPLETION: RequestKind
REQUEST_KIND_RESPONSES: RequestKind
REQUEST_KIND_EMBEDDINGS: RequestKind

class Identity(_message.Message):
    __slots__ = ("org_id", "user_id", "project_id", "conversation_id")
    ORG_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CONVERSATION_ID_FIELD_NUMBER: _ClassVar[int]
    org_id: str
    user_id: str
    project_id: str
    conversation_id: str
    def __init__(self, org_id: _Optional[str] = ..., user_id: _Optional[str] = ..., project_id: _Optional[str] = ..., conversation_id: _Optional[str] = ...) -> None: ...

class SubmitRequest(_message.Message):
    __slots__ = ("request_id", "identity", "kind", "model", "messages", "stream", "max_tokens", "temperature")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    STREAM_FIELD_NUMBER: _ClassVar[int]
    MAX_TOKENS_FIELD_NUMBER: _ClassVar[int]
    TEMPERATURE_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    identity: Identity
    kind: RequestKind
    model: str
    messages: _containers.RepeatedCompositeFieldContainer[_gateway_pb2.ChatMessage]
    stream: bool
    max_tokens: int
    temperature: float
    def __init__(self, request_id: _Optional[str] = ..., identity: _Optional[_Union[Identity, _Mapping]] = ..., kind: _Optional[_Union[RequestKind, str]] = ..., model: _Optional[str] = ..., messages: _Optional[_Iterable[_Union[_gateway_pb2.ChatMessage, _Mapping]]] = ..., stream: bool = ..., max_tokens: _Optional[int] = ..., temperature: _Optional[float] = ...) -> None: ...

class SubmitResponse(_message.Message):
    __slots__ = ("request_id", "content", "finish_reason", "is_final")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    FINISH_REASON_FIELD_NUMBER: _ClassVar[int]
    IS_FINAL_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    content: str
    finish_reason: str
    is_final: bool
    def __init__(self, request_id: _Optional[str] = ..., content: _Optional[str] = ..., finish_reason: _Optional[str] = ..., is_final: bool = ...) -> None: ...
