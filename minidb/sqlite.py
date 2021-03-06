import logging
import sqlite3
import time
from datetime import datetime

from .driver import Driver
from .model import ModelMetadata, TableMetadata
from .schema import Bit, Blob, Column, Date, Float, Integer, String

_log = logging.getLogger(__name__)


def _and(*criterias):
  result = ' AND '.join(criterias)
  if len(criterias) > 1:
    result = '(' + result + ')'
  return result


def _or(*criterias):
  result = ' OR '.join(criterias)
  if len(criterias) > 1:
    result = '(' + result + ')'
  return result


class SqliteDriver(Driver):
  FTS_TABLE_PREFIX = 'fts5__'

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

    self._execute(
        'CREATE TABLE %s (\n' % name
        + ',\n'.join(column_sql_list)
        + '\n) %s' % table_opts_sql
    )

    analyzed_columns = [col_name for col_name,
                        c in metadata.columns.items() if not c.analyze is None]
    if len(analyzed_columns) > 0:
      index_table_name = "%s%s" % (self.__class__.FTS_TABLE_PREFIX, name)
      index_column_names = primary_key_names + analyzed_columns

      self._execute(
          'CREATE VIRTUAL TABLE %s USING fts5(%s)'
          % (index_table_name, ', '.join(index_column_names))
      )

      def _trigger(table_name, event_name, sql):
        return 'CREATE TRIGGER %s__%s AFTER %s ON %s BEGIN\n%s\nEND;' \
            % (table_name, event_name.lower(), event_name.upper(), table_name, sql)

      self._executescript(_trigger(
          table_name=metadata.name,
          event_name="INSERT",
          sql="INSERT INTO %s (%s) VALUES (%s);" % (
              index_table_name,
              ', '.join(index_column_names),
              ', '.join(
                  ['new.%s' % col_name for col_name in index_column_names]),
          )
      ))
      self._executescript(_trigger(
          table_name=metadata.name,
          event_name="DELETE",
          sql="INSERT INTO %s (%s, %s) VALUES ('delete', %s);" % (
              index_table_name,
              index_table_name,
              ', '.join(index_column_names),
              ', '.join(
                  ['old.%s' % col_name for col_name in index_column_names]),
          )
      ))
      self._executescript(_trigger(
          table_name=metadata.name,
          event_name="UPDATE",
          sql="INSERT INTO %s (%s, %s) VALUES ('delete', %s);" % (
              index_table_name,
              index_table_name,
              ', '.join(index_column_names),
              ', '.join(
                  ['old.%s' % col_name for col_name in index_column_names]),
          ) + "INSERT INTO %s (%s) VALUES (%s);" % (
              index_table_name,
              ', '.join(index_column_names),
              ', '.join(
                  ['new.%s' % col_name for col_name in index_column_names]),
          )
      ))

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

    col_names = [attr_name for attr_name in row.keys(
    ) if not schema.columns[attr_name].primary_key]
    sql_params = [row[n] for n in col_names]
    set_sql = ', '.join(['%s = ?' % cn for cn in col_names])
    criteria_sql = self._format_criteria(criteria, sql_params, schema)
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

  def count(self, t: type, criteria=None, partition_key=None, term=None):
    if not term is None:
      return self._count_ft(t, term, criteria, partition_key)

    schema: TableMetadata = t.__table__

    sql_params = []

    criteria_sql = 'WHERE ' + self._format_criteria(
        criteria, sql_params, schema, partition_key) if not criteria is None or not partition_key is None else ''
    sql = 'SELECT COUNT(*) FROM %s %s' % (
        schema.name, criteria_sql
    )

    for (count,) in self._execute(sql, sql_params):
      return count

  def _count_ft(self, t: type, term, criteria=None, partition_key=None):
    schema: TableMetadata = t.__table__
    analyzed_columns = [col_name for col_name,
                        c in schema.columns.items() if c.analyze]
    sql_params = []

    where_sql = [
      "%s%s MATCH '%s'" % (self.__class__.FTS_TABLE_PREFIX, schema.name, term)
    ]

    join_on_sql = [
        _and(*["a.%s = b.%s" % (col_name, col_name)
               for col_name in schema.primary_key])
    ]

    if not criteria is None:
      join_on_sql.append(self._format_criteria(
          criteria, sql_params, schema, partition_key=partition_key, alias="b"))

    if not partition_key is None:
      where_sql.insert(0, self._format_key(
          vars(partition_key), sql_params, schema, prefix='a'))

    sql = "SELECT COUNT(*) " \
        + "FROM %s%s a " % (self.__class__.FTS_TABLE_PREFIX, schema.name) \
        + "INNER JOIN %s b ON %s " % (schema.name, _and(*join_on_sql)) \
        + "WHERE %s " % _and(*where_sql)

    for (count,) in self._execute(sql, sql_params):
      return count

  def _find_ft(self, t: type, term, criteria=None, sort=None, limit=None, offset=None, partition_key=None):
    schema: TableMetadata = t.__table__
    all_columns = [col_name for col_name in schema.columns]
    sql_params = []

    where_sql = [
      "%s%s MATCH '%s'" % (self.__class__.FTS_TABLE_PREFIX, schema.name, term)
    ]

    join_on_sql = [
        _and(*["a.%s = b.%s" % (col_name, col_name)
               for col_name in schema.primary_key])
    ]

    if not criteria is None:
      join_on_sql.append(self._format_criteria(
          criteria, sql_params, schema, partition_key=partition_key, alias="b"))

    if not partition_key is None:
      where_sql.insert(0, self._format_key(
          vars(partition_key), sql_params, schema, prefix='a'))

    sql = "SELECT %s " % (', '.join(['b.%s' % col_name for col_name in all_columns])) \
        + "FROM %s%s a " % (self.__class__.FTS_TABLE_PREFIX, schema.name) \
        + "INNER JOIN %s b ON %s " % (schema.name, _and(*join_on_sql)) \
        + "WHERE %s " % _and(*where_sql) \
        + "ORDER BY %s " % ('a.rank' if sort is None else self._format_sort(sort, 'b')) \
        + ("LIMIT %d OFFSET %d" % (limit, offset) if not limit is None else "")

    result = []
    for row in self._execute(sql, sql_params):
      result.append(t(**{attr_name: self._decode(row[all_columns.index(
          attr_name)], schema.columns[attr_name].column_type) for attr_name in all_columns}))

    return result

  def find(self, t: type, criteria=None, sort=None, limit=None, offset=None, partition_key=None, term=None):

    if not term is None:
      return self._find_ft(t, term, criteria, sort, limit, offset, partition_key)

    schema: TableMetadata = t.__table__

    sql_params = []

    all_columns = list(schema.columns.keys())
    result_map = all_columns
    select_sql = ', '.join(all_columns)
    where_sql = 'WHERE ' + self._format_criteria(
        criteria, sql_params, schema, partition_key) if not criteria is None or not partition_key is None else ''
    sort_sql = self._format_sort(sort) if not sort is None else ''
    limit_sql = 'LIMIT %d OFFSET %d' % (
        limit, offset or 0) if not limit is None else ''
    sql = 'SELECT %s FROM %s %s %s %s' % (
        select_sql, schema.name, where_sql, sort_sql, limit_sql)
    result = []
    for row in self._execute(sql, sql_params):
      result.append(t(**{attr_name: self._decode(row[result_map.index(
          attr_name)], schema.columns[attr_name].column_type) for attr_name in result_map}))

    return result

  def find_one(self, t: type, key):
    schema: TableMetadata = t.__table__
    if len(schema.primary_key) > 1:
      criteria = {attr_name: key[attr_name]
                  for attr_name in schema.primary_key}
    else:
      criteria = {schema.primary_key[0]: key}

    results = self.find(t, criteria)
    for x in results:
      return x

  def remove(self):
    raise NotImplementedError()

  def query(self, t, sql, sql_params=None):
    schema: TableMetadata = t.__table__
    all_columns = [col_name for col_name in schema.columns]
    result = []
    for row in self._execute(sql, sql_params):
      result.append(t(**{attr_name: self._decode(row[all_columns.index(
          attr_name)], schema.columns[attr_name].column_type) for attr_name in all_columns}))

    return result

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

  def _executescript(self, sql: str, cursor=None):
    target = self._con if cursor is None else cursor
    begin_mtime = int(time.time() * 1000)
    elapsed_mtime = None
    error = None
    try:
      return target.executescript(sql)
    except sqlite3.OperationalError as err:
      error = err
      raise
    finally:
      state_txt = 'completed' if error is None else 'failed'
      log_fn = _log.debug if error is None else _log.error
      elapsed_mtime = int(time.time() * 1000) - begin_mtime
      log_fn('Sql statement %s:\nsql: %s\nduration_ms: %d' %
             (state_txt, sql, elapsed_mtime))

  def _format_criteria(self, criteria: dict, sql_params: list, schema: TableMetadata, partition_key=None, alias=None):
    if not criteria is None:
      if not isinstance(criteria, dict):
        raise TypeError('`criteria` must be instance of `dict`')
      if len(criteria) == 0:
        raise ValueError('`criteria` cannot be empty')

    criteria_sql_list = []
    if not criteria is None:
      criteria_args = col_info = col_name = None
      for k, v in criteria.items():
        criteria_args = v if isinstance(v, list) else [v]
        if k[0] == "$":
          op_name = k[1:]
          criteria_sql_list.append(self._format_operator(
              op_name, criteria_args, sql_params))
          continue

        col_info = schema.columns.get(k)
        col_name = k if alias is None else '%s.%s' % (alias, k)
        criteria_sql_list.append('%s = ?' % col_name)
        sql_params.append(self._encode(v, col_info.column_type))

    if partition_key:
      criteria_sql_list.append(self._format_key(
          vars(partition_key), sql_params, schema, prefix=alias))

    criteria_sql = ' AND '.join(criteria_sql_list)
    if len(criteria_sql_list) > 1:
      criteria_sql = '(%s)' % criteria_sql

    return criteria_sql

  def _format_key(self, values: dict, sql_params: list, schema: TableMetadata, prefix=None):
    criterias_sql = []
    col_info = col_name = None
    for k, v in values.items():
      col_info = schema.columns.get(k)
      col_name = k if prefix is None else '%s.%s' % (prefix, k)
      criterias_sql.append('%s = ?' % col_name)
      sql_params.append(self._encode(v, col_info.column_type))

    result = ' AND '.join(criterias_sql)
    if len(criterias_sql) > 1:
      result = '(' + result + ')'
    return result

  def _format_operator(self, left, right, name, args, sql_params):
    operators = {
        'eq': lambda name, v: '',
        'gt': lambda argv: argv[0]
    }
    if not name in operators:
      raise ValueError('`name`: invalid operator: `%s`' % name)

    return operators[name](*args)

  def _format_sort(self, criteria, prefix=None):
    if not isinstance(criteria, list):
      raise TypeError('`criteria` must be an instance of `list`')

    if len(criteria) == 0:
      raise ValueError('`criteria` does not contain any item')

    criterias_sql = []
    for c in criteria:
      if not isinstance(c, tuple):
        raise TypeError('`criteria` items must be instances of `tuple`')

      if len(c) != 2:
        raise ValueError('`criteria` items must contain exactly 2 values')

      attr_name, sort_dir = c

      if sort_dir != 'ASC' and sort_dir != 'DESC':
        raise ValueError(
            '`criteria` items sort direction must be either "ASC" or "DESC"')

      if not prefix is None:
        attr_name = prefix + '.' + attr_name

      criterias_sql.append('%s %s' % (attr_name, sort_dir))

    return 'ORDER BY ' + ', '.join(criterias_sql)

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
