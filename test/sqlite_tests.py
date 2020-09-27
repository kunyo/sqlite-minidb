import unittest
import minidb
from datetime import datetime
from minidb.schema import *

Model = minidb.get_model_builder()


class TestDocumentWithAutoIncrementPkey(Model):
  __tablename__ = 'TestDocumentWithAutoIncrementPkey'
  id = Column(Integer(), autoincrement=True, primary_key=True)
  name = Column(String(), nullable=False)


class TestDocumentWithGeneratedAttribute(Model):
  __tablename__ = 'TestDocumentWithGeneratedAttribute'
  id = Column(Integer(), generator=lambda: 1234567890, primary_key=True)


class TestDocumentWithDate(Model):
  __tablename__ = 'TestDocumentWithDate'
  id = Column(Integer(), autoincrement=True, primary_key=True)
  created_on = Column(Date())


class TestDatabaseInitializer(Model):
  __tablename__ = 'TestDatabaseInitializer'
  id = Column(Integer(), primary_key=True)
  name = Column(String())


@Model.database_initializer
def _initialize_db(driver: minidb.Driver):
  driver.add(
    TestDatabaseInitializer, 
    TestDatabaseInitializer(
      id=9999,
      name='created by database initializer'
    )
  )


Model.build()


class SqliteDriverTestCase(unittest.TestCase):
  def setUp(self):
    self._db = minidb.get_driver('sqlite', {'db_file': ':memory:'})
    self._db.connect()
    Model.metadata.create_db(self._db)

  def tearDown(self):
    self._db.close()

  def test_databaseInitialized(self):
    doc = self._db.find_one(TestDatabaseInitializer, 9999)

    assert not doc is None
    assert doc.id == 9999
    assert doc.name == 'created by database initializer'

  def test_mustCreateAndRetrieveDocWithAutoIncrementPkey(self):
    expected_doc = TestDocumentWithAutoIncrementPkey(name='foobar')
    self._db.add(TestDocumentWithAutoIncrementPkey, expected_doc)

    assert expected_doc.id > 0

    doc = self._db.find_one(TestDocumentWithAutoIncrementPkey, expected_doc.id)

    assert not doc is None
    assert doc.id == expected_doc.id
    assert doc.name == expected_doc.name

  def test_mustCreateAndRetrieveDocWithGeneratedAttribute(self):
    expected = TestDocumentWithGeneratedAttribute()
    self._db.add(TestDocumentWithGeneratedAttribute, expected)

    assert expected.id == 1234567890

    doc = self._db.find_one(TestDocumentWithGeneratedAttribute, 1234567890)
    assert not doc is None
    assert doc.id == 1234567890

  def test_dateTimePrecisionLoss(self):
    created_on = datetime.utcnow().replace(microsecond=999999)
    expected = TestDocumentWithDate(created_on=created_on)
    self._db.add(TestDocumentWithDate, expected)

    doc = self._db.find_one(TestDocumentWithDate, expected.id)
    assert not doc is None
    assert doc.created_on == created_on
