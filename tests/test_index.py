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

from mongolite import Connection, Document, OperationFailure, BadIndexError, INDEX_GEO2D, INDEX_ASCENDING, INDEX_DESCENDING

class IndexTestCase(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(safe=True)
        self.col = self.connection['test']['mongolite']
        
    def tearDown(self):
        self.connection['test'].drop_collection('mongolite')
        self.connection = None
    
    def test_index_basic(self):
        @self.connection.register
        class Movie(Document):
            skeleton = {
                'other':{
                    'deep':unicode,
                },
                'notindexed':unicode,
            }
            optional = {
                'standard':unicode,
            }

            indexes = [
                {
                    'fields':[('standard',1),('other.deep',1)],
                    'unique':True,
                },
            ]
        self.col.Movie.generate_indexes()
        movie = self.col.Movie()
        movie['standard'] = u'test'
        movie['other']['deep'] = u'testdeep'
        movie['notindexed'] = u'notthere'
        movie.save()

        db = self.connection.test
        item = db['system.indexes'].find_one({'ns':'test.mongolite', 'name': 'standard_1_other.deep_1', 'unique':True})
        assert item is not None, 'No Index Found'

        movie = self.col.Movie()
        movie['standard'] = u'test'
        movie['other']['deep'] = u'testdeep'
        self.assertRaises(OperationFailure, movie.save)

    def test_index_basic_dynamic_collection(self):
        @self.connection.register
        class Movie(Document):
            skeleton = {
                'other':{
                    'deep':unicode,
                },
                'notindexed':unicode,
            }
            optional = {
                'standard':unicode,
            }
            
            indexes = [
                {
                    'fields':[('standard',1),('other.deep',1)],
                    'unique':True,
                },
            ]
        self.col.Movie.generate_indexes()
        movie = self.col.Movie()
        movie['standard'] = u'test'
        movie['other']['deep'] = u'testdeep'
        movie['notindexed'] = u'notthere'
        movie.save()
        
        db = self.connection.test
        item = db['system.indexes'].find_one({'ns':'test.mongolite', 'name': 'standard_1_other.deep_1', 'unique':True})
        assert item is not None, 'No Index Found'

        movie = self.col.Movie()
        movie['standard'] = u'test'
        movie['other']['deep'] = u'testdeep'
        self.assertRaises(OperationFailure, movie.save)

        self.connection.test.othercol.Movie.generate_indexes()
        db = self.connection.test
        item = db['system.indexes'].find_one({'ns':'test.othercol', 'name': 'standard_1_other.deep_1', 'unique':True})
        assert item is not None, 'No Index Found'


    def test_index_single_without_generation(self):
        class Movie(Document):
            skeleton = {
                'standard':unicode,
            }
            
            indexes = [
                {
                    'fields':'standard',
                    'unique':True,
                },
            ]
        self.connection.register([Movie])
        movie = self.col.Movie()
        movie['standard'] = u'test'
        movie.save()
        
        db = self.connection.test
        item = db['system.indexes'].find_one({'ns':'test.mongolite', 'name':'standard_1', 'unique':True, 'key':{'standard':1}})
        
        assert item is None, 'Index is found'
        
    def test_index_single(self):
        class Movie(Document):
            skeleton = {
                'standard':unicode,
            }
            
            indexes = [
                {
                    'fields':'standard',
                    'unique':True,
                },
            ]
        self.connection.register([Movie])
        self.col.Movie.generate_indexes()
        movie = self.col.Movie()
        movie['standard'] = u'test'
        movie.save()
        
        db = self.connection.test
        item = db['system.indexes'].find_one({'ns':'test.mongolite', 'name':'standard_1', 'unique':True, 'key':{'standard':1}})
        
        assert item is not None, 'No Index Found'

    def test_index_multi(self):
        class Movie(Document):
            skeleton = {
                'standard':unicode,
                'other':{
                    'deep':unicode,
                },
                'notindexed':unicode,
                'alsoindexed':unicode,
            }
            
            indexes = [
                {
                    'fields':'standard',
                    'unique':True,
                },
                {
                    'fields':[('alsoindexed', 1), ('other.deep', -1)],
                    'unique':True,
                },
            ]
        self.connection.register([Movie])
        self.col.Movie.generate_indexes()
        movie = self.col.Movie()
        movie['standard'] = u'test'
        movie.save()
        
        db = self.connection.test
        item = db['system.indexes'].find_one({'ns':'test.mongolite', 'name':'standard_1', 'unique':True, 'key':{'standard':1}})
        index2 = db['system.indexes'].find_one({'ns':'test.mongolite', 'name': 'alsoindexed_1_other.deep_-1', 'unique':True})
        
        assert item is not None, 'No Index Found'
        assert index2 is not None, 'Index not found'

        movie = self.col.Movie()
        movie['standard'] = u'test'
        self.assertRaises(OperationFailure, movie.save)

    def test_index_multi2(self):
        class Movie(Document):
            skeleton = {
                'standard':unicode,
                'other':{
                    'deep':unicode,
                },
                'notindexed':unicode,
                'alsoindexed':unicode,
            }
            
            indexes = [
                {
                    'fields':'standard',
                    'unique':True,
                },
                {
                    'fields':'other.deep',
                    'unique':True,
                },
            ]
        self.connection.register([Movie])
        self.col.Movie.generate_indexes()
        movie = self.col.Movie()
        movie['standard'] = u'test'
        movie['other']['deep'] = u'foo'
        movie.save()
        
        db = self.connection.test
        item = db['system.indexes'].find_one({'ns':'test.mongolite', 'name':'standard_1', 'unique':True, 'key':{'standard':1}})
        index2 = db['system.indexes'].find_one({'ns':'test.mongolite', 'name': 'other.deep_1', 'unique':True})
        
        assert item is not None, 'No Index Found'
        assert index2 is not None, 'Index not found'

        movie = self.col.Movie()
        movie['standard'] = u'test'
        self.assertRaises(OperationFailure, movie.save)

        movie = self.col.Movie()
        movie['other']['deep'] = u'foo'
        self.assertRaises(OperationFailure, movie.save)

    def test_index_direction(self):
        class Movie(Document):
            skeleton = {
                'standard':unicode,
                'other':{
                    'deep':unicode,
                },
                'notindexed':unicode,
                'alsoindexed':unicode,
            }
            
            indexes = [
                {
                    'fields':[('standard',INDEX_DESCENDING)],
                    'unique':True,
                },
                {
                    'fields':[('alsoindexed',INDEX_ASCENDING), ('other.deep',INDEX_DESCENDING)],
                    'unique':True,
                },
            ]
        self.connection.register([Movie])
        self.col.Movie.generate_indexes()
        movie = self.col.Movie()
        movie['standard'] = u'test'
        movie.save()
        
        db = self.connection.test
        index1 = db['system.indexes'].find_one({'ns':'test.mongolite', 'name':'standard_-1', 'unique':True})
        index2 = db['system.indexes'].find_one({'ns':'test.mongolite', 'name': 'alsoindexed_1_other.deep_-1', 'unique':True})
        
        assert index1 is not None, 'No Index Found'
        assert index2 is not None, 'Index not found'

    def test_index_direction_GEO2D(self):
        class Movie(Document):
            skeleton = {
                'standard':unicode,
                'other':{
                    'deep':unicode,
                },
                'notindexed':unicode,
                'alsoindexed':unicode,
            }

            indexes = [
                {
                    'fields':[('standard',INDEX_GEO2D)],
                    'unique':True,
                },
                {
                    'fields':[('alsoindexed',INDEX_GEO2D), ('other.deep',INDEX_DESCENDING)],
                    'unique':True,
                },
            ]
        self.connection.register([Movie])
        self.col.Movie.generate_indexes()
        movie = self.col.Movie()
        movie['standard'] = u'test'
        movie.save()

        db = self.connection.test
        index1 = db['system.indexes'].find_one({'ns':'test.mongolite', 'name':'standard_2d', 'unique':True})
        index2 = db['system.indexes'].find_one({'ns':'test.mongolite', 'name': 'alsoindexed_2d_other.deep_-1', 'unique':True})

        assert index1 is not None, 'No Index Found'
        assert index2 is not None, 'Index not found'

    def test_bad_index_descriptor(self):
        failed = False
        try:
            class Movie(Document):
                skeleton = {'standard':unicode}
                indexes = [{'unique':True}]
        except BadIndexError, e:
            self.assertEqual(str(e), "`fields` key must be specify in indexes")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                skeleton = {
                    'standard':unicode,
                }
                indexes = [
                    {
                        'fields':'std',
                    },
                ]
        except ValueError, e:
            self.assertEqual(str(e), "Error in indexes: can't find std in skeleton or optional")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                skeleton = {
                    'standard':unicode,
                }
                indexes = [
                    {
                        'fields':{'standard':1},
                    },
                ]
        except BadIndexError, e:
            self.assertEqual(str(e), "fields must be a string or a list of tuples (got <type 'dict'> instead)")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                skeleton = {
                    'standard':unicode,
                }
                indexes = [
                    {
                        'fields':('standard',1, "blah"),
                    },
                ]
        except BadIndexError, e:
            self.assertEqual(str(e), "fields must be a string or a list of tuples (got <type 'tuple'> instead)")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                skeleton = {
                    'standard':unicode,
                }
                indexes = [
                    {
                        'fields':[('standard',"2")],
                    },
                ]
        except BadIndexError, e:
            self.assertEqual(str(e), "index direction must be INDEX_DESCENDING, INDEX_ASCENDING, INDEX_OFF, INDEX_ALL or INDEX_GEO2D. Got 2")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                skeleton = {
                    'standard':unicode,
                }
                indexes = [
                    {
                        'fields':(3,1),
                    },
                ]
        except BadIndexError, e:
            self.assertEqual(str(e), "fields must be a string or a list of tuples (got <type 'tuple'> instead)")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                skeleton = {
                    'standard':unicode,
                }
                indexes = [
                    {
                        'fields':[("blah",1)],
                    },
                ]
        except ValueError, e:
            self.assertEqual(str(e), "Error in indexes: can't find blah in skeleton or optional")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                skeleton = {
                    'standard':unicode,
                }
                indexes = [
                    {
                        'fields':[('standard',1), ('bla',1)],
                    },
                ]
        except ValueError, e:
            self.assertEqual(str(e), "Error in indexes: can't find bla in skeleton or optional")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                skeleton = {
                    'standard':unicode,
                }
                indexes = [
                    {
                        'fields':[('standard',3)],
                    },
                ]
        except BadIndexError, e:
            self.assertEqual(str(e), "index direction must be INDEX_DESCENDING, INDEX_ASCENDING, INDEX_OFF, INDEX_ALL or INDEX_GEO2D. Got 3")
            failed = True
        self.assertEqual(failed, True)

        failed = False
        try:
            class Movie(Document):
                skeleton = {
                    'standard':unicode,
                }
                indexes = [
                    {
                        'fields':'std',
                    },
                ]
        except ValueError, e:
            self.assertEqual(str(e), "Error in indexes: can't find std in skeleton or optional")
            failed = True
        self.assertEqual(failed, True)

    def test_index_ttl(self):
        class Movie(Document):
            skeleton = {
                'standard':unicode,
            }
            
            indexes = [
                {
                    'fields':'standard',
                    'unique':True,
                    'ttl': 86400
                },
        # If indexes are still broken validation will choke on the ttl
            ]
        self.connection.register([Movie])
        self.col.Movie.generate_indexes()
        movie = self.col.Movie()
        movie['standard'] = u'test'
        movie.save()
        
        db = self.connection.test
        item = db['system.indexes'].find_one({'ns':'test.mongolite', 'name':'standard_1', 'unique':True, 'key':{'standard':1}})
        
        assert item is not None, 'No Index Found'

    def test_index_simple_inheritance(self):
        class DocA(Document):
            skeleton = {
                'standard':unicode,
            }
            
            indexes = [
                {
                    'fields':'standard',
                    'unique':True,
                },
            ]

        class DocB(DocA):
            skeleton = {
                'docb':unicode,
            }
            
        self.connection.register([DocA, DocB])
        self.assertEqual(self.col.DocB.indexes, [{'fields': 'standard', 'unique':True}])
        self.col.DocB.generate_indexes()
        docb = self.col.DocB()
        docb['standard'] = u'test'
        docb['docb'] = u'foo'
        docb.save()
        
        db = self.connection.test
        item = db['system.indexes'].find_one({'ns':'test.mongolite', 'name':'standard_1', 'unique':True, 'key':{'standard':1}})
        
        assert item is not None, 'No Index Found'

    def test_index_inheritance(self):
        class DocA(Document):
            skeleton = {
                'standard':unicode,
            }
            
            indexes = [
                {
                    'fields':'standard',
                    'unique':True,
                },
            ]

        class DocB(DocA):
            skeleton = {
                'docb':unicode,
            }
            indexes = [
                {
                    'fields':'docb',
                    'unique':True,
                },
            ]
        self.connection.register([DocA, DocB])
        self.assertEqual(self.col.DocB.indexes, [{'fields': 'docb', 'unique': True}, {'fields': 'standard', 'unique': True}])
        self.col.DocB.generate_indexes()

            
        docb = self.col.DocB()
        docb['standard'] = u'test'
        docb['docb'] = u'foo'
        docb.save()
        
        db = self.connection.test
        item = db['system.indexes'].find_one({'ns':'test.mongolite', 'name':'standard_1', 'unique':True, 'key':{'standard':1}})
        item = db['system.indexes'].find_one({'ns':'test.mongolite', 'name':'docb_1', 'unique':True, 'key':{'docb':1}})
        
        assert item is not None, 'No Index Found'


    def test_index_real_world(self):
        import datetime
        class MyDoc(Document):
            skeleton = {
                "mydoc":{
                    "creation_date":datetime.datetime,
                }
            }
            indexes = [{'fields':[('mydoc.creation_date',-1), ('_id',1)]}]
        self.connection.register([MyDoc])

        date = datetime.datetime.utcnow()

        mydoc = self.col.MyDoc()
        mydoc['mydoc']['creation_date'] = date
        mydoc['_id'] = u'aaa'
        mydoc.save()


        mydoc3 = self.col.MyDoc()
        mydoc3['mydoc']['creation_date'] = date
        mydoc3['_id'] = u'bbb'
        mydoc3.save()

        import time
        time.sleep(1)
        date2 = datetime.datetime.utcnow()

        mydoc2 = self.col.MyDoc()
        mydoc2['mydoc']['creation_date'] = date2
        mydoc2['_id'] = u'aa'
        mydoc2.save()

        time.sleep(1)
        date3 = datetime.datetime.utcnow()

        mydoc4 = self.col.MyDoc()
        mydoc4['mydoc']['creation_date'] = date3
        mydoc4['_id'] = u'ccc'
        mydoc4.save()

        #self.col.ensure_index([('mydoc.creation_date',-1), ('_id',1)])
        self.col.MyDoc.generate_indexes()
        results = [i['_id'] for i in self.col.MyDoc.find().sort([('mydoc.creation_date',-1),('_id',1)])]
        self.assertEqual(results, ['ccc', 'aa', 'aaa', 'bbb'])
        self.col.MyDoc.generate_indexes()

    def test_index_pymongo(self):
        import datetime
        date = datetime.datetime.utcnow()
        import pymongo
        collection = pymongo.Connection()['test']['test_index']

        mydoc = {'mydoc':{'creation_date':date}, '_id':u'aaa'}
        collection.insert(mydoc)

        mydoc2 = {'mydoc':{'creation_date':date}, '_id':u'bbb'}
        collection.insert(mydoc2)

        import time
        time.sleep(1)
        date2 = datetime.datetime.utcnow()

        mydoc3 = {'mydoc':{'creation_date':date2}, '_id':u'aa'}
        collection.insert(mydoc3)

        time.sleep(1)
        date3 = datetime.datetime.utcnow()

        mydoc4 = {'mydoc':{'creation_date':date3}, '_id':u'ccc'}
        collection.insert(mydoc4)

        collection.ensure_index([('mydoc.creation_date',-1), ('_id',1)])
        #print list(collection.database.system.indexes.find())

        results = [i['_id'] for i in collection.find().sort([('mydoc.creation_date',-1),('_id',1)])]
        print results
        assert results  == [u'ccc', u'aa', u'aaa', u'bbb'], results

    def test_index_inheritance2(self):
        class A(Document):
            skeleton = {
                'a':{
                    'title':unicode,
                }
            }
            indexes = [{'fields':'a.title'}]

        class B(A):
            skeleton = {
                'b':{
                    'title':unicode,
                }
            }
            indexes = [{'fields':'b.title'}]


        class C(Document):
            skeleton = {
                'c':{
                    'title':unicode,
                }
            }
            indexes = [{'fields':'c.title'}]

        class D(B, C):
            skeleton = {
                'd':{
                    'title':unicode,
                }
            }

        self.connection.register([D])
        doc = self.col.D()
        self.assertEqual(doc.indexes, [{'fields': 'b.title'}, {'fields': 'a.title'}, {'fields': 'c.title'}])

    def test_index_with_default_direction(self):
        class MyDoc(Document):
            skeleton = {
                'foo': unicode,
                'bar': int
            }
            indexes = [
                {'fields': [('foo', 1), ('bar', -1)]},
            ]
        self.connection.register([MyDoc])
        self.col.MyDoc.generate_indexes()
        for i in range(10):
           doc = self.col.MyDoc()
           doc['foo'] = unicode(i)
           doc['bar'] = i
           doc.save()
        assert self.col.database.system.indexes.find_one({'name': 'foo_1_bar_-1'})

    def test_index_with_check(self):
        @self.connection.register
        class MyDoc(Document):
            skeleton = {
                'foo': dict,
                'bar': int
            }
            indexes = [
                    {'fields': 'foo.title', 'check':False},
            ]
        self.col.MyDoc.generate_indexes()
        for i in range(10):
           doc = self.col.MyDoc()
           doc['foo']['title'] = unicode(i)
           doc['bar'] = i
           doc.save()
        assert self.col.database.system.indexes.find_one({'name': 'foo.title_1'})

    def test_index_with_check_is_true(self):
        @self.connection.register
        class MyDoc(Document):
            skeleton = {
                'foo': unicode,
                'bar': int
            }
            indexes = [
                    {'fields': [('foo', 1)], 'check':True},
            ]
        self.col.MyDoc.generate_indexes()
        for i in range(10):
           doc = self.col.MyDoc()
           doc['foo'] = unicode(i)
           doc['bar'] = i
           doc.save()
        assert self.col.database.system.indexes.find_one({'name': 'foo_1'})

