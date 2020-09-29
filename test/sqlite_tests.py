import json
import os
import unittest
import minidb
from datetime import datetime
from minidb.schema import Column, Bit, Date, Integer, String

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


class TestDatabaseUpdate(Model):
  __tablename__ = 'TestDatabaseUpdate'
  id = Column(Integer(), primary_key=True)
  name = Column(String())


class TestFind(Model):
  __tablename__ = 'TestFind'
  id = Column(String(), primary_key=True)
  first_name = Column(String())
  last_name = Column(String())
  email = Column(String())
  state = Column(String())
  locked = Column(Bit())
  created_on = Column(Date())
  updated_on = Column(Date())


@Model.database_initializer
def _initialize_db(driver: minidb.Driver):
  driver.add(
      TestDatabaseInitializer,
      TestDatabaseInitializer(
          id=9999,
          name='created by database initializer'
      )
  )

  driver.add(
      TestDatabaseUpdate,
      TestDatabaseUpdate(
          id=9999,
          name='created by database initializer'
      )
  )

  with open(os.path.join(os.path.dirname(__file__), 'find_test_data.json'), 'rb') as fh:
    test_data = json.loads(fh.read())

  for td in test_data:
    td['created_on'] = datetime.fromtimestamp(td['created_on'])
    td['updated_on'] = datetime.fromtimestamp(td['updated_on'])
    
    doc = TestFind(**td)
    driver.add(TestFind, doc)


Model.build()


class SqliteDriverTestCase(unittest.TestCase):
  @classmethod
  def setUpClass(self):
    self._db = minidb.get_driver('sqlite', {'db_file': ':memory:'})
    self._db.connect()
    Model.metadata.create_db(self._db)

  @classmethod
  def tearDownClass(self):
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

  def test_databaseUpdate(self):
    expected_value = 'value has been updated'
    doc = self._db.find_one(TestDatabaseUpdate, 9999)

    assert not doc is None

    doc.name = expected_value

    self._db.update(TestDatabaseUpdate, doc)

    doc = self._db.find_one(TestDatabaseUpdate, 9999)

    assert not doc is None

    assert doc.name == expected_value

  def test_count(self):
    count = self._db.count(TestFind)

    assert count == 1000

  def test_countWithCriteria(self):
    count = self._db.count(TestFind, {'state':'active'})

    assert count == 327    

  def test_findOne(self):
    doc_id = "4ff5ea13-29d1-4b22-80ba-07eda00eeeb1"
    doc = self._db.find_one(TestFind, doc_id)

    assert not doc is None
    assert doc.id == doc_id
    assert doc.first_name == "Robert"
    assert doc.last_name == "Maldonado"
    assert doc.email == "yjohnson@hansen.info"
    assert doc.state == "deleted"
    assert doc.locked == False
    assert doc.created_on == datetime(2020, 3, 26, 15, 48, 8)
    assert doc.updated_on == datetime(2020, 9, 21, 8, 59, 58)
  
  def test_findAll(self):
    docs = self._db.find(TestFind)

    assert not docs is None
    assert len(docs) == 1000

  def test_find(self):
    docs = self._db.find(
      TestFind, 
      criteria={'state':'active'}, 
      sort=[('first_name', 'ASC')]
    )

    assert not docs is None
    assert len(docs) == 327
    for i in range(0, len(docs)):
      if i > 0:
        assert docs[i-1].first_name <= docs[i].first_name