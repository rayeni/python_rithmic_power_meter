# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: base.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='base.proto',
  package='rti',
  serialized_pb=_b('\n\nbase.proto\x12\x03rti\"\x1d\n\x04\x42\x61se\x12\x15\n\x0btemplate_id\x18\xe3\xb6\t \x02(\x05')
)
_sym_db.RegisterFileDescriptor(DESCRIPTOR)




_BASE = _descriptor.Descriptor(
  name='Base',
  full_name='rti.Base',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='template_id', full_name='rti.Base.template_id', index=0,
      number=154467, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=19,
  serialized_end=48,
)

DESCRIPTOR.message_types_by_name['Base'] = _BASE

Base = _reflection.GeneratedProtocolMessageType('Base', (_message.Message,), dict(
  DESCRIPTOR = _BASE,
  __module__ = 'base_pb2'
  # @@protoc_insertion_point(class_scope:rti.Base)
  ))
_sym_db.RegisterMessage(Base)


# @@protoc_insertion_point(module_scope)
