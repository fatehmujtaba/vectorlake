from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EventContext(_message.Message):
    __slots__ = ("event_id", "image_id", "tenant_id", "dedup_scope_id", "occurred_at_epoch_ms", "traceparent", "schema_version", "event_type")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    IMAGE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DEDUP_SCOPE_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_EPOCH_MS_FIELD_NUMBER: _ClassVar[int]
    TRACEPARENT_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    image_id: str
    tenant_id: str
    dedup_scope_id: str
    occurred_at_epoch_ms: int
    traceparent: str
    schema_version: int
    event_type: str
    def __init__(self, event_id: _Optional[str] = ..., image_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., dedup_scope_id: _Optional[str] = ..., occurred_at_epoch_ms: _Optional[int] = ..., traceparent: _Optional[str] = ..., schema_version: _Optional[int] = ..., event_type: _Optional[str] = ...) -> None: ...

class ImageReceivedV1(_message.Message):
    __slots__ = ("context", "bucket", "object_key", "object_version", "size_bytes", "upload_intent_id")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    OBJECT_VERSION_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    UPLOAD_INTENT_ID_FIELD_NUMBER: _ClassVar[int]
    context: EventContext
    bucket: str
    object_key: str
    object_version: str
    size_bytes: int
    upload_intent_id: str
    def __init__(self, context: _Optional[_Union[EventContext, _Mapping]] = ..., bucket: _Optional[str] = ..., object_key: _Optional[str] = ..., object_version: _Optional[str] = ..., size_bytes: _Optional[int] = ..., upload_intent_id: _Optional[str] = ...) -> None: ...

class ImageDuplicateV1(_message.Message):
    __slots__ = ("context", "duplicate_group_id", "representative_image_id", "duplicate_class", "policy_version", "decision_record_id")
    class DuplicateClass(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DUPLICATE_CLASS_UNSPECIFIED: _ClassVar[ImageDuplicateV1.DuplicateClass]
        EXACT: _ClassVar[ImageDuplicateV1.DuplicateClass]
        PIXEL: _ClassVar[ImageDuplicateV1.DuplicateClass]
        DERIVED: _ClassVar[ImageDuplicateV1.DuplicateClass]
    DUPLICATE_CLASS_UNSPECIFIED: ImageDuplicateV1.DuplicateClass
    EXACT: ImageDuplicateV1.DuplicateClass
    PIXEL: ImageDuplicateV1.DuplicateClass
    DERIVED: ImageDuplicateV1.DuplicateClass
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    REPRESENTATIVE_IMAGE_ID_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_CLASS_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    DECISION_RECORD_ID_FIELD_NUMBER: _ClassVar[int]
    context: EventContext
    duplicate_group_id: str
    representative_image_id: str
    duplicate_class: ImageDuplicateV1.DuplicateClass
    policy_version: str
    decision_record_id: str
    def __init__(self, context: _Optional[_Union[EventContext, _Mapping]] = ..., duplicate_group_id: _Optional[str] = ..., representative_image_id: _Optional[str] = ..., duplicate_class: _Optional[_Union[ImageDuplicateV1.DuplicateClass, str]] = ..., policy_version: _Optional[str] = ..., decision_record_id: _Optional[str] = ...) -> None: ...

class ImageInferenceReadyV1(_message.Message):
    __slots__ = ("context", "duplicate_group_id", "bucket", "object_key", "model_route")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    MODEL_ROUTE_FIELD_NUMBER: _ClassVar[int]
    context: EventContext
    duplicate_group_id: str
    bucket: str
    object_key: str
    model_route: str
    def __init__(self, context: _Optional[_Union[EventContext, _Mapping]] = ..., duplicate_group_id: _Optional[str] = ..., bucket: _Optional[str] = ..., object_key: _Optional[str] = ..., model_route: _Optional[str] = ...) -> None: ...

class EmbeddingGeneratedV1(_message.Message):
    __slots__ = ("context", "embedding_id", "duplicate_group_id", "model_name", "model_version", "preprocess_version", "vector", "dimension", "vector_ref")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    EMBEDDING_ID_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    PREPROCESS_VERSION_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    VECTOR_REF_FIELD_NUMBER: _ClassVar[int]
    context: EventContext
    embedding_id: str
    duplicate_group_id: str
    model_name: str
    model_version: str
    preprocess_version: str
    vector: _containers.RepeatedScalarFieldContainer[float]
    dimension: int
    vector_ref: str
    def __init__(self, context: _Optional[_Union[EventContext, _Mapping]] = ..., embedding_id: _Optional[str] = ..., duplicate_group_id: _Optional[str] = ..., model_name: _Optional[str] = ..., model_version: _Optional[str] = ..., preprocess_version: _Optional[str] = ..., vector: _Optional[_Iterable[float]] = ..., dimension: _Optional[int] = ..., vector_ref: _Optional[str] = ...) -> None: ...

class EmbeddingArchivedV1(_message.Message):
    __slots__ = ("context", "embedding_id", "archive_bucket", "archive_key", "archived_at_epoch_ms")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    EMBEDDING_ID_FIELD_NUMBER: _ClassVar[int]
    ARCHIVE_BUCKET_FIELD_NUMBER: _ClassVar[int]
    ARCHIVE_KEY_FIELD_NUMBER: _ClassVar[int]
    ARCHIVED_AT_EPOCH_MS_FIELD_NUMBER: _ClassVar[int]
    context: EventContext
    embedding_id: str
    archive_bucket: str
    archive_key: str
    archived_at_epoch_ms: int
    def __init__(self, context: _Optional[_Union[EventContext, _Mapping]] = ..., embedding_id: _Optional[str] = ..., archive_bucket: _Optional[str] = ..., archive_key: _Optional[str] = ..., archived_at_epoch_ms: _Optional[int] = ...) -> None: ...

class ImageIndexedV1(_message.Message):
    __slots__ = ("context", "embedding_id", "collection_name", "indexed_at_epoch_ms")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    EMBEDDING_ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    INDEXED_AT_EPOCH_MS_FIELD_NUMBER: _ClassVar[int]
    context: EventContext
    embedding_id: str
    collection_name: str
    indexed_at_epoch_ms: int
    def __init__(self, context: _Optional[_Union[EventContext, _Mapping]] = ..., embedding_id: _Optional[str] = ..., collection_name: _Optional[str] = ..., indexed_at_epoch_ms: _Optional[int] = ...) -> None: ...

class ProcessingFailedV1(_message.Message):
    __slots__ = ("context", "stage", "error_code", "retryable", "safe_public_message")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    STAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    SAFE_PUBLIC_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    context: EventContext
    stage: str
    error_code: str
    retryable: bool
    safe_public_message: str
    def __init__(self, context: _Optional[_Union[EventContext, _Mapping]] = ..., stage: _Optional[str] = ..., error_code: _Optional[str] = ..., retryable: _Optional[bool] = ..., safe_public_message: _Optional[str] = ...) -> None: ...
