import unittest
import minidb
from minidb.schema import *

Model = minidb.get_model_builder()


class TestDocumentWithAutoIncrementPkey(Model):
  __tablename__ = 'TestDocumentWithAutoIncrementPkey'
  id = Column(Integer(), autoincrement=True, primary_key=True)
  name = Column(String(), nullable=False)


class TestDocumentWithGeneratedAttribute(Model):
  __tablename__ = 'TestDocumentWithGeneratedAttribute'
  id = Column(Integer(), generator=lambda: 1234567890, primary_key=True)


Model.build()


class SqliteDriverTestCase(unittest.TestCase):
  def setUp(self):
    self._db = minidb.get_driver('sqlite', {'db_file': ':memory:'})
    self._db.connect()
    Model.metadata.create_db(self._db)

  def tearDown(self):
    self._db.close()

  def test_mustCreateAndRetrieveDocWithAutoIncrementPkey(self):
    expected_doc = TestDocumentWithAutoIncrementPkey(name='foobar')
    self._db.add(TestDocumentWithAutoIncrementPkey, expected_doc)

    assert expected_doc.id > 0

    doc = self._db.find_one(TestDocumentWithAutoIncrementPkey, expected_doc.id)

    assert not doc is None
    assert doc.id == expected_doc.id
    assert doc.name == expected_doc.name

  def test_mustCreateAndRetrieveDocWithGeneratedAttribute(self):
    expected_doc = TestDocumentWithGeneratedAttribute()
    self._db.add(TestDocumentWithGeneratedAttribute, expected_doc)

    assert expected_doc.id == 1234567890

    doc = self._db.find_one(TestDocumentWithGeneratedAttribute, 1234567890)
    assert not doc is None
    assert doc.id == 1234567890
