import logging
from typing import Dict, List
from .driver import Driver
from .schema import Column

_log = logging.getLogger(__name__)


class TableMetadata(object):
  def __init__(self, name, columns: Dict[str, Column], primary_key: List[str]):
    self.name = name
    self.columns = columns
    self.primary_key = primary_key


class ModelMetadata(object):
  def __init__(self, metadata: Dict[str, TableMetadata], initializer=None):
    self._metadata = metadata
    self._initializer = initializer

  def create_db(self, driver: Driver):
    driver.begin_transaction()
    try:
      for table_name, table_info in self._metadata.items():
        _log.info('Creating table `%s`' % table_name)
        driver.create_table(table_name, table_info)

      if not self._initializer is None:
        _log.info('Invoking database initializer')
        self._initializer(driver)

      driver.commit()
    except:
      driver.rollback()
      raise

  def collection(self, name: str):
    return self._metadata[name]


class Document(object):
  def __init__(self, **attrs):
    metadata = getattr(self, '__table__')
    for attr_name in metadata.columns:
      if attr_name in attrs:
        setattr(self, attr_name, attrs[attr_name])
      else:
        setattr(self, attr_name, None)


def get_model_builder():
  class _ModelBuilder(Document):
    initializer = None
    metadata: ModelMetadata = None

    @staticmethod
    def build():
      metadata = {}
      document_types = _ModelBuilder.__subclasses__()
      table_name = columns = primary_key = None
      for dt in document_types:
        table_name = getattr(dt, '__tablename__')
        columns = {k: v for k, v in vars(dt).items() if isinstance(v, Column)}
        primary_key = [k for k, v in columns.items() if v.primary_key]
        metadata[table_name] = TableMetadata(
            name=table_name,
            columns=columns,
            primary_key=primary_key
        )
        dt.__table__ = metadata[table_name]
      _ModelBuilder.metadata = ModelMetadata(
          metadata, initializer=_ModelBuilder.initializer)

    @staticmethod
    def database_initializer(fn):
      if not _ModelBuilder.initializer is None:
        raise Exception('Database initializer already set')

      _ModelBuilder.initializer = fn
      return fn

  return _ModelBuilder
