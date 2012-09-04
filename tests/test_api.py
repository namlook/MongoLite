#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2009-2011, Nicolas Clairon
# All rights reserved.
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the University of California, Berkeley nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import unittest

from mongolite import Document, Connection, DBRef,\
    ConnectionError, OperationFailure, ObjectId
from mongolite.schema_document import SchemaDocument

class ApiTestCase(unittest.TestCase):
    def setUp(self):
        self.connection = Connection()
        self.col = self.connection['test']['mongolite']
        
    def tearDown(self):
        self.connection.drop_database('test')
        self.connection.drop_database('othertest')

    def test_save(self):
        class MyDoc(Document):
            skeleton = {
                "bla":{
                    "foo":unicode,
                    "bar":int,
                },
                "spam":[],
            }
        self.connection.register([MyDoc])
        mydoc = self.col.MyDoc()
        mydoc["bla"]["foo"] = u"bar"
        mydoc["bla"]["bar"] = 42
        mydoc.save()
        assert isinstance(mydoc['_id'], ObjectId)

        saved_doc = self.col.find_one({"bla.bar":42})
        for key, value in mydoc.iteritems():
            assert saved_doc[key] == value

        mydoc = self.col.MyDoc()
        mydoc["bla"]["foo"] = u"bar"
        mydoc["bla"]["bar"] = 43
        mydoc.save()

        saved_doc = self.col.find_one({"bla.bar":43})
        for key, value in mydoc.iteritems():
            assert saved_doc[key] == value

    def test_save_without_collection(self):
        class MyDoc(Document):
            skeleton = {
                "foo":int,
            }
        self.connection.register([MyDoc])
        mydoc = MyDoc()
        mydoc["foo"] = 1
        self.assertRaises(ConnectionError, mydoc.save)

    def test_delete(self):
        class MyDoc(Document):
            skeleton = {
                "foo":int,
            }
        self.connection.register([MyDoc])
        mydoc = self.col.MyDoc()
        mydoc['_id'] = 'foo'
        mydoc["foo"] = 1
        mydoc.save()
        assert self.col.MyDoc.find().count() == 1
        mydoc = self.col.MyDoc.get_from_id('foo')
        assert mydoc['foo'] == 1
        mydoc.delete()
        assert self.col.MyDoc.find().count() == 0
        
    def test_generate_skeleton(self):
        class A(SchemaDocument):
            skeleton = {
                "a":{"foo":int},
                "bar":unicode
            }
        a = A(gen_skel=False)
        assert a == {}
        a.generate_skeleton()
        assert a == {"a":{"foo":None}, "bar":None}, a

    def test_generate_skeleton2(self):
        class A(SchemaDocument):
            skeleton = {
                "a":{"foo":[int]},
                "bar":{unicode:{"egg":int}}
            }
        a = A(gen_skel=False)
        assert a == {}
        a.generate_skeleton()
        assert a == {"a":{"foo":[]}, "bar":{}}, a

    def test_generate_skeleton3(self):
        class A(SchemaDocument):
            skeleton = {
                "a":{"foo":[int], "spam":{"bla":unicode}},
                "bar":{unicode:{"egg":int}}
            }
        a = A(gen_skel=False)
        assert a == {}
        a.generate_skeleton()
        assert a == {"a":{"foo":[], "spam":{"bla":None}}, "bar":{}}, a

    def test_get_from_id(self):
        class MyDoc(Document):
            skeleton = {
                "foo":int,
            }
        self.connection.register([MyDoc])
        mydoc = self.col.MyDoc()
        mydoc["_id"] = "bar"
        mydoc["foo"] = 3
        mydoc.save()
        fetched_doc = self.col.MyDoc.get_from_id("bar")
        assert mydoc == fetched_doc
        assert callable(fetched_doc) is False
        assert isinstance(fetched_doc, MyDoc)
        raw_doc = self.col.get_from_id('bar')
        assert mydoc == raw_doc
        assert not isinstance(raw_doc, MyDoc)

    def test_find(self):
        class MyDoc(Document):
            skeleton = {
                "foo":int,
                "bar":{"bla":int},
            }
        self.connection.register([MyDoc])
        for i in range(10):
            mydoc = self.col.MyDoc()
            mydoc["foo"] = i
            mydoc["bar"]['bla'] = i
            mydoc.save()
        for i in self.col.MyDoc.find({"foo":{"$gt":4}}):
            assert isinstance(i, MyDoc), (i, type(i))
        docs_list = [i["foo"] for i in self.col.MyDoc.find({"foo":{"$gt":4}})]
        assert docs_list == [5,6,7,8,9]
        # using limit/count
        assert self.col.MyDoc.find().count() == 10, self.col.MyDoc.find().count()
        assert self.col.MyDoc.find().limit(1).count() == 10, self.col.MyDoc.find().limit(1).count()
        assert self.col.MyDoc.find().where('this.foo').count() == 9 #{'foo':0} is not taken
        assert self.col.MyDoc.find().where('this.bar.bla').count() == 9 #{'foo':0} is not taken
        assert self.col.MyDoc.find().hint([('foo', 1)])
        assert [i['foo'] for i in self.col.MyDoc.find().sort('foo', -1)] == [9,8,7,6,5,4,3,2,1,0]
        allPlans = self.col.MyDoc.find().explain()['allPlans']
        assert allPlans == [{u'cursor': u'BasicCursor', u'indexBounds': {}}]
        next_doc =  self.col.MyDoc.find().sort('foo',1).next()
        assert callable(next_doc) is False
        assert isinstance(next_doc, MyDoc)
        assert next_doc['foo'] == 0
        assert len(list(self.col.MyDoc.find().skip(3))) == 7, len(list(self.col.MyDoc.find().skip(3)))
        from mongolite.cursor import Cursor
        assert isinstance(self.col.MyDoc.find().skip(3), Cursor)

    def test_find_one(self):
        class MyDoc(Document):
            skeleton = {
                "foo":int
            }
        self.connection.register([MyDoc])
        assert self.col.MyDoc.find_one() is None
        mydoc = self.col.MyDoc()
        mydoc['foo'] = 0
        mydoc.save()
        mydoc = self.col.MyDoc.find_one()
        assert mydoc["foo"] == 0
        assert isinstance(mydoc, MyDoc)
        for i in range(10):
            mydoc = self.col.MyDoc()
            mydoc["foo"] = i
            mydoc.save()
        one_doc = self.col.MyDoc.find_one()
        assert callable(one_doc) is False
        raw_mydoc = self.col.find_one()
        assert one_doc == raw_mydoc

    def test_find_and_modify(self):
        @self.connection.register
        class DocA(Document):
            __database__ = 'test'
            __collection__ = 'doca'
            structure = {'title': unicode, 'rank': int}

        for i in range(10):
            self.connection.DocA({'title': unicode(i), 'rank': i}).save()

        doc = self.connection.DocA.find_and_modify({'rank':3}, {'$set':{'title': u'coucou'}})
        new_doc = self.connection.DocA.find_one({'rank':3})
        self.assertEqual(doc['title'], '3')
        self.assertEqual(new_doc['title'], 'coucou')
        self.assertEqual(isinstance(doc, DocA), True)

        @self.connection.register
        class DocA(Document):
            structure = {'title': unicode, 'rank': int}

        for i in range(10):
            self.connection.test.doca2.save({'title': unicode(i), 'rank': i})

        doc = self.connection.test.doca2.DocA.find_and_modify({'rank':3}, {'$set':{'title': u'coucou'}})
        new_doc = self.connection.test.doca2.DocA.find_one({'rank':3})
        self.assertEqual(doc['title'], '3')
        self.assertEqual(new_doc['title'], 'coucou')
        self.assertEqual(isinstance(doc, DocA), True)

    def test_find_random(self):
        class MyDoc(Document):
            skeleton = {
                "foo":int
            }
        self.connection.register([MyDoc])
        assert self.col.find_random() is None
        assert self.col.MyDoc.find_random() is None
        for i in range(50):
            mydoc = self.col.MyDoc()
            mydoc["foo"] = i
            mydoc.save()
        raw_mydoc = self.col.find_random()
        mydoc = self.col.MyDoc.find_random()
        assert callable(mydoc) is False
        assert isinstance(mydoc, MyDoc)
        assert mydoc != raw_mydoc, (mydoc, raw_mydoc)

    def test_query_with_passing_collection(self):
        class MyDoc(Document):
            skeleton = {
                'foo':int,
            }
        self.connection.register([MyDoc])

        mongolite = self.connection.test.mongolite

        # boostraping
        for i in range(10):
            mydoc = mongolite.MyDoc()
            mydoc['_id'] = unicode(i)
            mydoc['foo'] = i
            mydoc.save()

        # get_from_id
        fetched_doc = mongolite.MyDoc.get_from_id('4')
        assert fetched_doc.collection == mongolite

        # all
        fetched_docs = mongolite.MyDoc.find({'foo':{'$gt':2}})
        assert fetched_docs.count() == 7
        for doc in fetched_docs:
            assert doc.collection == mongolite

    def test_connection(self):
        class DocA(Document):
            skeleton = {
                "doc_a":{'foo':int},
            }
        self.connection.register([DocA])
        assertion = True
        try:
            DocA.connection
        except AttributeError:
            assertion = True
        assert assertion
        try:
            DocA.db
        except AttributeError:
            assertion = True
        assert assertion
        try:
            DocA.collection
        except AttributeError:
            assertion = True
        assert assertion

        assert self.col.DocA.connection == Connection("localhost", 27017)
        assert self.col.DocA.collection == Connection("localhost", 27017)['test']['mongolite']
        assert self.col.DocA.db == Connection("localhost", 27017)['test']

    def test_all_with_dynamic_collection(self):
        class Section(Document):
            skeleton = {"section":int}
        self.connection.register([Section])

        s = self.connection.test.section.Section()
        s['section'] = 1
        s.save()

        s = self.connection.test.section.Section()
        s['section'] = 2
        s.save()

        s = self.connection.test.other_section.Section()
        s['section'] = 1
        s.save()

        s = self.connection.test.other_section.Section()
        s['section'] = 2
        s.save()


        sect_col = self.connection.test.section
        sects = [s.collection.name == 'section' and s.db.name == 'test' for s in sect_col.Section.find({})] 
        assert len(sects) == 2, len(sects)
        assert any(sects)

        sect_col = self.connection.test.other_section
        sects = [s.collection.name == 'other_section' and s.db.name == 'test' for s in sect_col.Section.find({})] 
        assert len(sects) == 2
        assert any(sects)

    def test_get_collection_with_connection(self):
        class Section(Document):
            skeleton = {"section":int}
        connection = Connection('127.0.0.3')
        connection.register([Section])
        col = connection.test.mongolite
        assert col.database.connection == col.Section.connection
        assert col.database.name == 'test' == col.Section.db.name
        assert col.name == 'mongolite' == col.Section.collection.name

    def test_get_size(self):
        class MyDoc(Document):
            skeleton = {
                "doc":{"foo":int, "bla":unicode},
            }

        mydoc = MyDoc()
        mydoc['doc']['foo'] = 3
        mydoc['doc']['bla'] = u'bla bla'
        assert len(mydoc.get_son_object()) == 41, len(mydoc.get_son_object())

        mydoc['doc']['bla'] = u'bla bla'+'b'*12
        assert len(mydoc.get_son_object()) == 41+12

    def test_get_with_no_wrap(self):
        class MyDoc(Document):
            skeleton = {"foo":int}
        self.connection.register([MyDoc])

        for i in xrange(2000):
            mydoc = self.col.MyDoc()
            mydoc['foo'] = i
            mydoc.save()

        import time
        start = time.time()
        wrapped_mydocs = [i for i in self.col.MyDoc.find()]
        end = time.time()
        wrap_time = end-start

        start = time.time()
        mydocs = [i for i in self.col.find().sort('foo', -1)]
        end = time.time()
        no_wrap_time = end-start

        assert no_wrap_time < wrap_time

        assert isinstance(wrapped_mydocs[0], MyDoc)
        assert not isinstance(mydocs[0], MyDoc), type(mydocs[0])
        assert [i['foo'] for i in mydocs] == list(reversed(range(2000))), [i['foo'] for i in mydocs]
        assert mydocs[0]['foo'] == 1999, mydocs[0]['foo']

        assert not isinstance(self.col.find().sort('foo', -1).next(), MyDoc)

    def test_get_dbref(self):
        class MyDoc(Document):
            skeleton = {"foo":int}
        self.connection.register([MyDoc])

        mydoc = self.col.MyDoc()
        mydoc['_id'] = u'1'
        mydoc['foo'] = 1
        mydoc.save()
        
        mydoc = self.connection.test.othercol.MyDoc()
        mydoc['_id'] = u'2'
        mydoc['foo'] = 2
        mydoc.save()

        mydoc = self.connection.othertest.mongolite.MyDoc()
        mydoc['_id'] = u'3'
        mydoc['foo'] = 3
        mydoc.save()

        mydoc = self.col.MyDoc.find_one({'foo':1})
        assert mydoc.get_dbref(), DBRef(u'mongolite', u'1', u'test')

        mydoc = self.connection.test.othercol.MyDoc.find_one({'foo':2})
        assert mydoc.get_dbref() == DBRef(u'othercol', u'2', u'test')

        mydoc = self.connection.othertest.mongolite.MyDoc.find_one({'foo':3})
        assert mydoc.get_dbref() == DBRef(u'mongolite', u'3', u'othertest')

    def test__hash__(self):
        class MyDoc(Document):
            skeleton = {"foo":int}
        self.connection.register([MyDoc])

        mydoc = self.col.MyDoc()
        mydoc['foo'] = 1
        self.assertRaises(TypeError, hash, mydoc)

        mydoc.save()
        hash(mydoc)

    def test_non_callable(self):
        class MyDoc(Document):
            skeleton = {"foo":int}
        self.connection.register([MyDoc])
        mydoc = self.col.MyDoc()
        self.assertRaises(TypeError, mydoc)
        assert callable(mydoc) is False


    def test_bad_call(self):
        class MyDoc(Document):
            skeleton = {"foo":int}
        self.assertRaises(TypeError, self.connection.test.col.MyDoc)
        self.connection.register([MyDoc])
        self.connection.test.col.MyDoc()
        self.assertRaises(TypeError, self.connection.test.col.Bla)
        self.assertRaises(TypeError, self.connection.test.Bla)

    def test_distinct(self):
        class Doc(Document):
            skeleton = {
                "foo": unicode,
                "bla": int
            }
        self.connection.register([Doc])

        for i in range(15):
            if i % 2 == 0:
                foo = u"blo"
            else:
                foo = u"bla"
            doc = self.col.Doc(doc={'foo':foo, 'bla':i})
            doc.save()
 
        self.assertEqual(self.col.find().distinct('foo'), [u'blo', u'bla'])
        self.assertEqual(self.col.find().distinct('bla'), range(15))

    def test_explain(self):
        class MyDoc(Document):
            skeleton = {
                "foo":int,
                "bar":{"bla":int},
            }
        self.connection.register([MyDoc])
        for i in range(10):
            mydoc = self.col.MyDoc()
            mydoc["foo"] = i
            mydoc["bar"]['bla'] = i
            mydoc.save()
        explain1 = self.col.MyDoc.find({"foo":{"$gt":4}}).explain()
        explain2 = self.col.find({'foo':{'gt':4}}).explain()
        explain1.pop('n')
        explain2.pop('n')
        assert explain1 == explain2, (explain1, explain2)

    def test_with_long(self):
        class Doc(Document):
            skeleton = {
                "foo":long,
                "bar":unicode,
            }
        self.connection.register([Doc])
        doc = self.col.Doc()
        doc['foo'] = 12L
        doc.save()
        fetch_doc = self.col.Doc.find_one()
        fetch_doc['bar'] = u'egg'
        fetch_doc.save()

    def test_passing_collection_in_argument(self):
        class MyDoc(Document):
            skeleton = {
                'foo':unicode
            }
        doc = MyDoc(collection=self.col)
        doc['foo'] = u'bla'
        doc.save()
        
    def test_reload(self):
        class MyDoc(Document):
            skeleton = {
                'foo':{
                    'bar':unicode,
                    'eggs':{'spam':int},
                },
                'bla':unicode
            }
        self.connection.register([MyDoc])

        doc = self.col.MyDoc()
        self.assertRaises(KeyError, doc.reload)
        doc['_id'] = 3
        doc['foo']['bar'] = u'mybar'
        doc['foo']['eggs']['spam'] = 4
        doc['bla'] = u'ble'
        self.assertRaises(OperationFailure, doc.reload)
        doc.save()

        doc['bla'] = u'bli'

        self.col.update({'_id':doc['_id']}, {'$set':{'foo.eggs.spam':2}})

        doc.reload()
        assert doc == {'_id': 3, 'foo': {u'eggs': {u'spam': 2}, u'bar': u'mybar'}, 'bla': u'ble'}

    def test_rewind(self):
        class MyDoc(Document):
            skeleton = {
                'foo':int,
            }
        self.connection.register([MyDoc])

        for i in range(10):
            doc = self.col.MyDoc() 
            doc['foo'] = i
            doc.save()

        cur = self.col.MyDoc.find()
        for i in cur:
            assert isinstance(i, MyDoc), type(MyDoc)
        try:
            cur.next()
        except StopIteration:
            pass
        cur.rewind()
        for i in cur:
            assert isinstance(i, MyDoc), type(MyDoc)
        for i in cur.rewind():
            assert isinstance(i, MyDoc), type(MyDoc)
            
            
    def test_decorator(self):
        @self.connection.register
        class MyDoc(Document):
            skeleton = {
                'foo':int,
            }

        mydoc = self.col.MyDoc()
        mydoc['foo'] = 3
        mydoc.save()

        raw_doc = self.col.MyDoc.find_one()
        self.assertEqual(raw_doc['foo'], 3)
        assert isinstance(raw_doc, MyDoc)


    def test_collection_name_filled(self):

        @self.connection.register
        class MyDoc(Document):
            __collection__ = 'mydoc'
            skeleton = {
                'foo':int,
            }

        mydoc = self.connection.test.MyDoc()
        mydoc['foo'] = 3
        mydoc.save()
        self.assertEqual(mydoc.collection.name, 'mydoc')

        raw_doc = self.connection.test.MyDoc.find_one()
        self.assertEqual(self.col.MyDoc.find_one(), None)
        self.assertEqual(raw_doc['foo'], 3)
        self.assertEqual(raw_doc, mydoc)
        assert isinstance(raw_doc, MyDoc)

        mydoc = self.col.MyDoc()
        mydoc['foo'] = 3
        mydoc.save()
        self.assertEqual(mydoc.collection.name, 'mongolite')

        raw_doc = self.col.MyDoc.find_one()
        self.assertEqual(raw_doc['foo'], 3)
        self.assertEqual(raw_doc, mydoc)
        assert isinstance(raw_doc, MyDoc)

    def test_database_name_filled(self):

        failed = False
        @self.connection.register
        class MyDoc(Document):
            __database__ = 'mydoc'
            skeleton = {
                'foo':int,
            }
        try:
            doc = self.connection.MyDoc()
        except AttributeError, e:
            failed = True
            self.assertEqual(str(e), 'MyDoc: __collection__ attribute not '
              'found. You cannot specify the `__database__` attribute '
              'without the `__collection__` attribute')
        self.assertEqual(failed, True)

        @self.connection.register
        class MyDoc(Document):
            __database__ = 'test'
            __collection__ = 'mydoc'
            skeleton = {
                'foo':int,
            }

        # test directly from a connection
        mydoc = self.connection.MyDoc()
        mydoc['foo'] = 3
        mydoc.save()
        self.assertEqual(mydoc.collection.name, 'mydoc')
        self.assertEqual(mydoc.collection.database.name, 'test')
        self.assertEqual(self.col.MyDoc.find_one(), None)

        raw_doc = self.connection.MyDoc.find_one()
        self.assertEqual(raw_doc['foo'], 3)
        self.assertEqual(raw_doc, mydoc)
        assert isinstance(raw_doc, MyDoc)

        # test directly from a database
        mydoc = self.connection.othertest.MyDoc()
        mydoc['foo'] = 3
        mydoc.save()
        self.assertEqual(mydoc.collection.name, 'mydoc')
        self.assertEqual(mydoc.collection.database.name, 'othertest')
        self.assertEqual(self.col.MyDoc.find_one(), None)

        raw_doc = self.connection.othertest.MyDoc.find_one()
        self.assertEqual(raw_doc['foo'], 3)
        self.assertEqual(raw_doc, mydoc)
        assert isinstance(raw_doc, MyDoc)

        # and still can use it via a collection
        mydoc = self.col.MyDoc()
        mydoc['foo'] = 3
        mydoc.save()
        self.assertEqual(mydoc.collection.name, 'mongolite')
        self.assertEqual(mydoc.collection.database.name, 'test')

        raw_doc = self.col.MyDoc.find_one()
        self.assertEqual(raw_doc['foo'], 3)
        self.assertEqual(raw_doc, mydoc)
        assert isinstance(raw_doc, MyDoc)


    def test_no_collection_in_virtual_document(self):
        @self.connection.register
        class Root(Document):
            __database__ = "test"

        @self.connection.register
        class DocA(Root):
           __collection__ = "doca"
           skeleton = {'title':unicode}

        doc = self.connection.DocA()
        doc['title'] = u'foo'
        doc.save()

        self.assertEqual(self.connection.test.doca.find_one(), doc)


    def test_basestring_type(self):
        @self.connection.register
        class DocA(Document):
           __database__ = 'test'
           __collection__ = "doca"
           skeleton = {'title':unicode}

        doc = self.connection.DocA()
        doc['title'] = 'foo'
        doc.save()
        new_doc = self.connection.DocA.find_one({'_id':doc['_id']})
        self.assertEqual(new_doc, {'_id': doc['_id'], 'title': 'foo'})

        @self.connection.register
        class DocA(Document):
           __database__ = 'test'
           __collection__ = "doca"
           authorized_types = Document.authorized_types+[str]
           skeleton = {'title':str}

        doc = self.connection.DocA()
        doc['title'] = u'foo'
        doc.save()
        new_doc = self.connection.DocA.find_one({'_id':doc['_id']})
        self.assertEqual(new_doc, {'_id': doc['_id'], 'title': 'foo'})

        @self.connection.register
        class DocA(Document):
           __database__ = 'test'
           __collection__ = "doca"
           skeleton = {'title':basestring}

        doc = self.connection.DocA()
        doc['title'] = u'foo'
        doc.save()
        doc['title'] = 'foo'
        doc.save()
        new_doc = self.connection.DocA.find_one({'_id':doc['_id']})
        self.assertEqual(new_doc, doc)

    def test_float_and_int_types(self):
        @self.connection.register
        class DocA(Document):
           __database__ = 'test'
           __collection__ = "doca"
           skeleton = {'foo':int}

        doc = self.connection.DocA()
        doc['foo'] = 3.0
        doc.save()
        new_doc = self.connection.DocA.find_one({'_id':doc['_id']})
        self.assertEqual(new_doc, {u'_id': doc['_id'], u'foo': 3.0})

        @self.connection.register
        class DocA(Document):
           __database__ = 'test'
           __collection__ = "doca"
           authorized_types = Document.authorized_types+[str]
           skeleton = {'foo':float}

        doc = self.connection.DocA()
        doc['foo'] = 2
        doc.save()
        new_doc = self.connection.DocA.find_one({'_id':doc['_id']})
        self.assertEqual(doc, {'foo': 2, '_id': doc['_id']})


        @self.connection.register
        class DocA(Document):
           __database__ = 'test'
           __collection__ = "doca"
           skeleton = {'foo':None}

        doc = self.connection.DocA()
        doc['foo'] = 3
        doc.save()
        doc['foo'] = 2.0
        doc.save()

        new_doc = self.connection.DocA.find_one({'_id':doc['_id']})
        self.assertEqual(new_doc, doc)

    def test_no_skeleton(self):
        @self.connection.register
        class DocA(Document):
            __database__ = 'test'
            __collection__ = 'doca'

        doc = self.connection.DocA()
        doc['doo'] = 4
        doc['foo'] = {'bar': 3}
        doc.save()

        self.assertEqual(self.connection.DocA.find_one(), {u'_id': doc['_id'], u'foo': {u'bar': 3}, u'doo': 4})

    def test_no___collection_and_get_from_database(self):
        @self.connection.register
        class DocA(Document):
            __database__ = 'test'
        assertion = False
        try:
            self.connection.DocA()
        except AttributeError:
            assertion = True
        self.assertTrue(assertion)

    def test_unwrapped_cursor(self):
        self.assertEqual(self.col.count(), 0)
        doc_id = self.col.save({}, safe=True)
        self.assertEqual(self.col.count(), 1)
        try:
            self.col.find(_id=doc_id)[0]
        except TypeError:
            self.fail("Cursor.__getitem__ raised TypeError unexpectedly!")
