import logging
import json
import os
import unittest
import minidb
from datetime import datetime
from minidb.schema import Column, Bit, Date, Integer, String
from minidb.driver import PartitionKey

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
  client_id = Column(String(), primary_key=True)
  id = Column(String(), primary_key=True)
  first_name = Column(String())
  last_name = Column(String())
  email = Column(String())
  state = Column(String())
  locked = Column(Bit())
  created_on = Column(Date())
  updated_on = Column(Date())


class TestFullText(Model):
  __tablename__ = 'TestFullText'
  client_id = Column(String(), primary_key=True)
  id = Column(String(), primary_key=True)
  title = Column(String(), analyze=True)
  text = Column(String(), analyze=True)
  published = Column(Bit())


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

  with open(os.path.join(os.path.dirname(__file__), 'fulltext_test_data.json'), 'rb') as fh:
    test_data = json.loads(fh.read())

  for td in test_data:
    doc = TestFullText(**td)
    driver.add(TestFullText, doc)


logging.basicConfig(level=logging.DEBUG)
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

  def test_createAndRetrieveDocWithAutoIncrementPkey(self):
    expected_doc = TestDocumentWithAutoIncrementPkey(name='foobar')
    self._db.add(TestDocumentWithAutoIncrementPkey, expected_doc)

    assert expected_doc.id > 0

    doc = self._db.find_one(TestDocumentWithAutoIncrementPkey, expected_doc.id)

    assert not doc is None
    assert doc.id == expected_doc.id
    assert doc.name == expected_doc.name

  def test_createAndRetrieveDocWithGeneratedAttribute(self):
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
    partition_key = PartitionKey(
        client_id="9103d3e3-8155-4664-add1-149124d1d9bc")
    count = self._db.count(
        TestFind, {'state': 'active'}, partition_key=partition_key)

    assert count == 103

  def test_findOne(self):
    partition_key = '9103d3e3-8155-4664-add1-149124d1d9bc'
    doc_id = '7801eb5c-4993-4da5-8f01-81a44d091e36'
    doc = self._db.find_one(
        TestFind, {'id': doc_id, 'client_id': partition_key})

    assert not doc is None
    assert doc.id == doc_id
    assert doc.first_name == "Jack"
    assert doc.last_name == "Meyer"
    assert doc.email == "jennifer62@hotmail.com"
    assert doc.state == "deleted"
    assert doc.locked == True
    assert doc.created_on == datetime(2020, 9, 5, 20, 47, 43)
    assert doc.updated_on == datetime(2020, 3, 7, 18, 8, 47)

  def test_findAll(self):
    docs = self._db.find(TestFind)

    assert not docs is None
    assert len(docs) == 1000

  def test_findAllWithPartitionKey(self):
    partition_key = PartitionKey(
        client_id="9103d3e3-8155-4664-add1-149124d1d9bc")
    docs = self._db.find(TestFind, partition_key=partition_key)

    assert not docs is None
    assert len(docs) == 314

  def test_find(self):
    partition_key = PartitionKey(
        client_id="9103d3e3-8155-4664-add1-149124d1d9bc")
    docs = self._db.find(
        TestFind,
        criteria={'state': 'active'},
        sort=[('first_name', 'ASC')],
        partition_key=partition_key
    )

    assert not docs is None
    assert len(docs) == 103
    for i in range(0, len(docs)):
      if i > 0:
        assert docs[i-1].first_name <= docs[i].first_name

  def test_search(self):
    partition_key = PartitionKey(
        client_id="9103d3e3-8155-4664-add1-149124d1d9bc")
    response = self._db.search(
        TestFind,
        criteria={'state': 'active'},
        sort=[('first_name', 'ASC')],
        partition_key=partition_key
    )

    assert not response is None
    assert response.total == 103
    assert response.page_size == 1000

    docs = response.data

    for i in range(0, len(docs)):
      if i > 0:
        assert docs[i-1].first_name <= docs[i].first_name, "Invalid sort order"

  def test_fullTextSearch(self):
    term = 'early'
    partition_key = PartitionKey(
        client_id="5a5dfa8d-b821-42c8-ba52-52f4657e18a3")
    response = self._db.search(
        TestFullText,
        partition_key=partition_key,
        term=term
    )

    assert not response is None
    assert response.total == 86
    assert response.page_size == 1000

    docs = response.data

    for i in range(0, len(docs)):
      assert docs[i].title.find(term) != -1 or docs[i].text.find(term) != -1

def test_fullTextSearchWithCustomSort(self):
    term = 'early'
    partition_key = PartitionKey(
        client_id="5a5dfa8d-b821-42c8-ba52-52f4657e18a3")
    response = self._db.search(
        TestFullText,
        partition_key=partition_key,
        sort=[('text', 'DESC',), ('title', 'DESC',)],
        term=term
    )

    assert not response is None
    assert response.total == 86
    assert response.page_size == 1000

    docs = response.data

    for i in range(0, len(docs)):
      assert docs[i].title.find(term) != -1 or docs[i].text.find(term) != -1, "Returned doc contains no instance of term" 

    for i in range(0, len(docs)):
      if i > 0:
        assert docs[i-1].text >= docs[i].text and docs[i-1].title >= docs[i].title, "Invalid sort order"
        
