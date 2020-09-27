import os
from .driver import Driver
from .model import get_model_builder
from .sqlite import SqliteDriver

_DRIVERS = {
    'sqlite': SqliteDriver
}


class Factory(object):
  def __init__(self, driver: str, opts: dict = {}):
    self._driver = driver
    self._opts = opts

  def get_driver(self):
    return get_driver(self._driver, self._opts)


def get_driver(driver: str, opts: dict = {}):
  if not driver in _DRIVERS:
    raise Driver.DriverNotSupportedError(driver)

  d = _DRIVERS[driver](**opts)
  return d


def database_exists(driver: str, opts: dict = {}):
  if driver == 'sqlite':
    if not 'db_file' in opts:
      raise ValueError(
          '`opts`: `db_file` option is required when creating Sqlite databases')
    return os.path.exists(opts['db_file'])
  raise NotImplementedError()
