class Primitive(object):
  pass


class String(Primitive):
  pass


class Number(Primitive):
  pass


class Blob(Primitive):
  pass


class Bit(Primitive):
  pass


class Integer(Number):
  pass


class Float(Number):
  pass


class Date(Primitive):
  pass


class Column(object):
  def __init__(self, column_type, nullable=False, autoincrement=False, primary_key=False, generator=None, analyze=None):
    if autoincrement == True and not isinstance(column_type, Integer):
      raise ValueError(
          '`autoincrement` can only be used on columns of type `Integer`')

    self.column_type = column_type
    self.nullable = nullable
    self.autoincrement = autoincrement
    self.primary_key = primary_key
    self.generator = generator
    self.analyze = analyze
