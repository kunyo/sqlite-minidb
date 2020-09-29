from abc import ABC, abstractmethod


class Driver(ABC):
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
  def count(self, t, criteria=None):
    pass
  @abstractmethod
  def find_one(self, t, key):
    pass

  @abstractmethod
  def find(self, t, criteria=None, sort=None, limit=None, offset=None):
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
