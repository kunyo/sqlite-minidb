import logging
import sqlite3
import time
from datetime import datetime

from .driver import Driver
from .model import ModelMetadata, TableMetadata
from .schema import Bit, Blob, Column, Date, Float, Integer, String

_log = logging.getLogger(__name__)


@Driver.register
class SqliteDriver(object):
  def __init__(self, db_file=':memory:'):
    self._con = None
    self._db_file = db_file
    self._in_transaction = False

  def connect(self):
    if not self._con is None:
      raise Driver.AlreadyConnectedError()

    self._con = sqlite3.connect(
        self._db_file,
        isolation_level=None,
    )

  def close(self):
    if self._con is None:
      raise Driver.InvalidConnectionStateError(
          expected='open', actual='closed')

    self._con.close()

  def begin_transaction(self):
    if not self._in_transaction:
      self._execute('BEGIN TRANSACTION')
      self._in_transaction = True
      return
    raise Driver.AlreadyInTransactionError()

  def commit(self):
    if self._in_transaction:
      self._con.commit()
      self._in_transaction = False
      return
    raise Driver.NotInTransactionError()

  def rollback(self):
    if self._in_transaction:
      self._con.rollback()
      self._in_transaction = False
      return
    raise Driver.NotInTransactionError()

  def create_table(self, name: str, metadata: TableMetadata):
    _log.info('Creating table `%s`' % name)

    def _typename_sql(t):
      if isinstance(t, Integer) or isinstance(t, Bit):
        return "INTEGER"
      if isinstance(t, Float):
        return "REAL"
      if isinstance(t, String):
        return "TEXT"
      if isinstance(t, Date):
        return "INTEGER"
      if isinstance(t, Blob):
        return "BLOB"
      raise NotImplementedError()

    def _column_sql(name: str, c: Column):
      typename_sql = _typename_sql(c.column_type)
      result = '%s %s' % (name, typename_sql)
      if c.autoincrement and c.primary_key:
        result += ' PRIMARY KEY AUTOINCREMENT'
      if not c.nullable:
        result += ' NOT NULL'
      else:
        result += ' NULL'
      return result

    primary_key_names = [
        k for k, v in metadata.columns.items() if v.primary_key]
    has_autoincrement_pkey = len(
        [k for k, v in metadata.columns.items() if v.primary_key and v.autoincrement]) > 0
    has_composite_pkey = len(primary_key_names) > 1

    if has_autoincrement_pkey and has_composite_pkey:
      raise Exception(
          "Error creating table `%s`: `autoincrement` is not supported on models using composite primary keys" % name)

    column_sql_list = [_column_sql(k, v)
                       for k, v in metadata.columns.items()]
    table_opts_sql = ''
    if has_composite_pkey:
      primary_key_sql = ','.join(primary_key_names)
      column_sql_list.append('PRIMARY KEY (%s)' % primary_key_sql)
      table_opts_sql += ' WITHOUT ROWID'

    sql = 'CREATE TABLE %s (\n' % name \
        + ',\n'.join(column_sql_list)\
        + '\n) %s' % table_opts_sql

    try:
      self._execute(sql)
      _log.debug('SQL statement executed: %s' % sql)
    except:
      _log.error('SQL statement failed: %s' % sql)
      raise

  def add(self, t: type, model):
    schema: TableMetadata = t.__table__
    if model is None:
      raise ValueError('`model` cannot be None')

    errors = []
    row = {}
    self._map_row(model, schema, row, errors)
    if len(errors) > 0:
      raise Driver.DataValidationError(schema.name, errors)

    col_names = row.keys()
    col_values = [row[n] for n in col_names]
    c = self._con.cursor()
    self._execute(
        "INSERT INTO %s (%s) VALUES (%s)"
        % (
            schema.name,
            ",".join(col_names),
            ",".join(["?" for n in col_names]),
        ),
        col_values,
        cursor=c
    )

    has_autoincrement_pkey = len(
        schema.primary_key) == 1 and schema.columns[schema.primary_key[0]].autoincrement
    if has_autoincrement_pkey:
      setattr(model, schema.primary_key[0], c.lastrowid)

  def update(self, t: type, model):
    schema: TableMetadata = t.__table__
    if model is None:
      raise ValueError('`model` cannot be None')
    if len(schema.primary_key) > 1:
      criteria = {attr_name: getattr(model, attr_name)
                  for attr_name in schema.primary_key}
    else:
      criteria = {schema.primary_key[0]: getattr(
          model, schema.primary_key[0])}

    errors = []
    row = {}
    self._map_row(model, schema, row, errors)
    if len(errors) > 0:
      raise Driver.DataValidationError(schema.name, errors)

    col_names = row.keys()
    sql_params = [row[n] for n in col_names]
    set_sql = ', '.join(['%s = ?' % cn for cn in col_names])
    criteria_sql = self._format_criteria(criteria, sql_params)
    c = self._con.cursor()
    self._execute(
        "UPDATE %s SET %s WHERE %s"
        % (
            schema.name,
            set_sql,
            criteria_sql,
        ),
        sql_params,
        cursor=c
    )
    if c.rowcount == 0:
      raise Driver.UnaffectedRowsError()

  def find(self, t: type, criteria: dict):
    raise NotImplementedError()

  def find_one(self, t: type, key):
    schema: TableMetadata = t.__table__
    if len(schema.primary_key) > 1:
      criteria = {attr_name: key[attr_name]
                  for attr_name in schema.primary_key}
    else:
      criteria = {schema.primary_key[0]: key}

    sql_params = []

    all_columns = list(schema.columns.keys())
    result_map = all_columns
    select_sql = ', '.join(all_columns)
    criteria_sql = self._format_criteria(criteria, sql_params)
    sql = 'SELECT %s FROM %s WHERE %s' % (
        select_sql, schema.name, criteria_sql)
    for row in self._execute(sql, sql_params):
      return t(**{attr_name: self._decode(row[result_map.index(attr_name)], schema.columns[attr_name].column_type) for attr_name in result_map})

  def _decode(self, value, column_type):
    if not value is None:
      if isinstance(column_type, Date):
        return datetime.fromtimestamp(value)
      return value

  def _encode(self, value, column_type):
    if not value is None:
      if isinstance(column_type, Date):
        return value.timestamp()
      return value

  def _execute(self, sql: str, sql_params: list = [], cursor=None):
    target = self._con if cursor is None else cursor
    begin_mtime = int(time.time() * 1000)
    elapsed_mtime = None
    error = None
    try:
      return target.execute(sql, sql_params)
    except sqlite3.OperationalError as err:
      error = err
      raise
    finally:
      state_txt = 'completed' if error is None else 'failed'
      log_fn = _log.debug if error is None else _log.error
      elapsed_mtime = int(time.time() * 1000) - begin_mtime
      log_fn('Sql statement %s:\nsql: %s\nduration_ms: %d' %
             (state_txt, sql, elapsed_mtime))

  def _format_criteria(self, criteria: dict, sql_params: list):
    if not isinstance(criteria, dict):
      raise ValueError('`criteria` must be instance of `dict`')
    if len(criteria) == 0:
      raise ValueError('`criteria` cannot be empty')

    criteria_sql_list = []
    for k, v in criteria.items():
      criteria_args = v if isinstance(v, list) else [v]
      if k[0] == "$":
        op_name = k[1:]
        criteria_sql_list.append(self._format_operator(
            op_name, criteria_args, sql_params))
        continue

      criteria_sql_list.append('%s = ?' % k)
      sql_params.append(v)

    criteria_sql = ' AND '.join(criteria_sql_list)
    if len(criteria_sql_list) > 1:
      criteria_sql = '(%s)' % criteria_sql

    return criteria_sql

  def _format_operator(self, left, right, name, args, sql_params):
    operators = {
        'eq': lambda name, v: '',
        'gt': lambda argv: argv[0]
    }
    if not name in operators:
      raise ValueError('`name`: invalid operator: `%s`' % name)

    return operators[name](*args)

  def _map_row(self, model, schema, row, errors):
    attr_info: Column = None
    attr_value = None
    for attr_name in schema.columns:
      attr_info = schema.columns[attr_name]

      # Skip autoincrement columns
      if attr_info.autoincrement:
        continue

      if attr_info.generator:
        attr_value = attr_info.generator()
        setattr(model, attr_name, attr_value)
      else:
        attr_value = getattr(model, attr_name)

      if attr_value is None and not attr_info.nullable:
        errors.append(
            '`%s`: value is null but the column is marked as not nullable'
            % attr_name
        )
        continue

      row[attr_name] = self._encode(attr_value, attr_info.column_type)

  def __enter__(self):
    self.connect()
    return self

  def __exit__(self, type, value, tb):
    if not tb is None and self._con.in_transaction:
      self._con.rollback()

    self._con.close()
    del self._con
