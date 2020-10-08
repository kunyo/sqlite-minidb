from abc import ABC, abstractmethod


class QueryResponse(object):
  def __init__(self, data, total, offset, page_size):
    self.data = data
    self.total = total
    self.offset = offset
    self.page_size = page_size


class PartitionKey(object):
  def __init__(self, **kwargs):
    for attr_name in kwargs:
      setattr(self, attr_name, kwargs[attr_name])


class Driver(ABC):
  MAX_PAGE_SIZE = 1000

  @abstractmethod
  def create_table(self):
    pass

  @abstractmethod
  def begin_transaction(self):
    pass

  @abstractmethod
  def commit(self):
    pass

  @abstractmethod
  def rollback(self):
    pass

  @abstractmethod
  def count(self, t, criteria=None, partition_key=None, term=None):
    pass

  @abstractmethod
  def find_one(self, t, key):
    pass

  @abstractmethod
  def find(self, t, criteria=None, sort=None, limit=None, offset=None, partition_key=None, term=None):
    pass

  @abstractmethod
  def add(self, t, model):
    pass

  @abstractmethod
  def update(self, t, model):
    pass

  @abstractmethod
  def remove(self, t, key):
    pass

  @abstractmethod
  def query(self, t, sql):
    pass

  def search(self, t, criteria=None, sort=None, limit=None, offset=None, partition_key=None, term=None):
    if limit is None:
      limit = self.__class__.MAX_PAGE_SIZE
    elif limit < 0:
      raise ValueError('"limit" must be greater than 0')
    elif limit > self.__class__.MAX_PAGE_SIZE:
      raise ValueError('"limit" exceeds the configured MAX_PAGE_SIZE')
    offset = offset or 0

    total = self.count(
        t, criteria=criteria, partition_key=partition_key, term=term
    )
    
    data = self.find(
        t, criteria=criteria, sort=sort,
        limit=limit, offset=offset, partition_key=partition_key, term=term
    )

    return QueryResponse(data, total, offset, limit)

  class DataValidationError(Exception):
    def __init__(self, collection_name, errors):
      super().__init__(
          "Data validation failed for collection `%s`:\n%s"
          % (collection_name, "\n".join(errors))
      )

  class DriverNotSupportedError(Exception):
    def __init__(self, name):
      super().__init__('The driver `%s` is not supported' % name)

  class AlreadyConnectedError(Exception):
    def __init__(self):
      super().__init__('Driver is already connected')

  class InvalidConnectionStateError(Exception):
    def __init__(self, expected, actual):
      super().__init__('Invalid connection state. Expected: %s; actual: %s' %
                       (expected, actual))

  class AlreadyInTransactionError(Exception):
    def __init__(self):
      super().__init__('A transaction is already open on this connection')

  class NotInTransactionError(Exception):
    def __init__(self):
      super().__init__('Not in transaction')

  class UnaffectedRowsError(Exception):
    def __init__(self):
      super().__init__('Update affected 0 rows')
