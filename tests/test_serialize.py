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
    ConnectionError, OperationFailure, ObjectId, json_util
from mongolite.schema_document import SchemaDocument

import json

class SerializeTestCase(unittest.TestCase):
    def setUp(self):
        self.connection = Connection()
        self.col = self.connection['test']['mongolite']
        
    def tearDown(self):
        self.connection.drop_database('test')
        self.connection.drop_database('othertest')

    def test_simple_serialize(self):
        @self.connection.register
        class Foo(Document):
            __database__ = 'test'
            __collection__ = 'foo'
            skeleton = {
                "bar":int,
            }

        @self.connection.register
        class MyDoc(Document):
            __database__ = 'test'
            __collection__ = 'mydocs'
            skeleton = {
                "foo":ObjectId,
            }

            @property
            def foo(self):
                return self.db.Foo.get_from_id(self['foo'])

        foo = self.connection.Foo()
        foo['bar'] = 42
        foo.save()

        mydoc = self.connection.MyDoc()
        mydoc['foo'] = foo['_id']
        mydoc.save()
        
        mydoc = self.connection.MyDoc.get_from_id(mydoc['_id'])
        self.assertEqual(mydoc.foo['_id'], foo['_id'])
        self.assertEqual(mydoc, {'_id':mydoc['_id'], 'foo': foo['_id']})
        self.assertEqual(mydoc.serialize(), {'_id': mydoc['_id'], 'foo': {'_id':foo['_id'], 'bar':42}})
        self.assertEqual(json.dumps(mydoc, default=json_util.default), 
          '{"_id": {"$oid": "%s"}, "foo": {"$oid": "%s"}}' % (mydoc['_id'], foo['_id']))
        self.assertEqual(json.dumps(mydoc.serialize(), default=json_util.default),
          '{"_id": {"$oid": "%s"}, "foo": {"_id": {"$oid": "%s"}, "bar": 42}}' % (mydoc['_id'], foo['_id']))

    def test_simple_serialize_cursor(self):
        @self.connection.register
        class Foo(Document):
            __database__ = 'test'
            __collection__ = 'foos'
            skeleton = {
                "bar":int,
            }

        @self.connection.register
        class MyDoc(Document):
            __database__ = 'test'
            __collection__ = 'mydocs'
            skeleton = {
                "foos":[ObjectId],
            }

            @property
            def foos(self):
                return self.db.Foo.find({'_id':{'$in':self['foos']}})

        foo = self.connection.Foo()
        foo['bar'] = 42
        foo.save()

        foo2 = self.connection.Foo()
        foo2['bar'] = 3 
        foo2.save()

        mydoc = self.connection.MyDoc()
        mydoc['foos'] = [foo['_id'], foo2['_id']]
        mydoc.save()
        
        mydoc = self.connection.MyDoc.get_from_id(mydoc['_id'])
        for _foo in mydoc.foos:
            self.assertTrue(_foo['_id'] in [foo['_id'], foo2['_id']])

        self.assertEqual(mydoc, {'_id':mydoc['_id'], 'foos': [foo['_id'], foo2['_id']]})

        self.assertEqual(mydoc.serialize(), {'_id': mydoc['_id'],
          'foos': [{'_id':foo['_id'], 'bar':42}, {'_id':foo2['_id'], 'bar': 3}]
        })

        self.assertEqual(json.dumps(mydoc, default=json_util.default), 
          '{"_id": {"$oid": "%s"}, "foos": [{"$oid": "%s"}, {"$oid": "%s"}]}' % (
          mydoc['_id'], foo['_id'], foo2['_id']))

        self.assertEqual(json.dumps(mydoc.serialize(), default=json_util.default),
          '{"_id": {"$oid": "%s"}, "foos": [{"_id": {"$oid": "%s"}, "bar": 42}, {"_id": {"$oid": "%s"}, "bar": 3}]}' % (
          mydoc['_id'], foo['_id'], foo2['_id']))

    def test_simple_serialize_without_property(self):
        @self.connection.register
        class Foo(Document):
            __database__ = 'test'
            __collection__ = 'foo'
            skeleton = {
                "bar":int,
            }

        @self.connection.register
        class MyDoc(Document):
            __database__ = 'test'
            __collection__ = 'mydocs'
            skeleton = {
                "foo":ObjectId,
            }

        foo = self.connection.Foo()
        foo['bar'] = 42
        foo.save()

        mydoc = self.connection.MyDoc()
        mydoc['foo'] = foo['_id']
        mydoc.save()
        
        mydoc = self.connection.MyDoc.get_from_id(mydoc['_id'])
        self.assertEqual(mydoc, {'_id':mydoc['_id'], 'foo': foo['_id']})
        self.assertEqual(mydoc.serialize(), {'_id': mydoc['_id'], 'foo': foo['_id']})
        self.assertEqual(json.dumps(mydoc, default=json_util.default), 
          '{"_id": {"$oid": "%s"}, "foo": {"$oid": "%s"}}' % (mydoc['_id'], foo['_id']))
        self.assertEqual(json.dumps(mydoc.serialize(), default=json_util.default),
          '{"_id": {"$oid": "%s"}, "foo": {"$oid": "%s"}}' % (mydoc['_id'], foo['_id']))

    def test_simple_serialize_default_property(self):
        @self.connection.register
        class Foo(Document):
            __database__ = 'test'
            __collection__ = 'foo'
            skeleton = {
                "bar":int,
            }

        @self.connection.register
        class MyDoc(Document):
            __database__ = 'test'
            __collection__ = 'mydocs'
            skeleton = {
                "spam":{
                    "foo":ObjectId,
                }
            }

            @property
            def spam__foo(self):
                return self.db.Foo.get_from_id(self['spam']['foo'])

        foo = self.connection.Foo()
        foo['bar'] = 42
        foo.save()

        mydoc = self.connection.MyDoc()
        mydoc['spam']['foo'] = foo['_id']
        mydoc.save()
        
        mydoc = self.connection.MyDoc.get_from_id(mydoc['_id'])
        self.assertEqual(mydoc.spam__foo['_id'], foo['_id'])
        self.assertEqual(mydoc, {'_id':mydoc['_id'], 'spam':{'foo': foo['_id']}})
        self.assertEqual(mydoc.serialize(), {'_id': mydoc['_id'], 'spam':{'foo': {'_id':foo['_id'], 'bar':42}}})
        self.assertEqual(json.dumps(mydoc, default=json_util.default), 
          '{"_id": {"$oid": "%s"}, "spam": {"foo": {"$oid": "%s"}}}' % (mydoc['_id'], foo['_id']))
        self.assertEqual(json.dumps(mydoc.serialize(), default=json_util.default),
          '{"_id": {"$oid": "%s"}, "spam": {"foo": {"_id": {"$oid": "%s"}, "bar": 42}}}' % (mydoc['_id'], foo['_id']))

    def test_simple_serialize_mapping(self):
        @self.connection.register
        class Foo(Document):
            __database__ = 'test'
            __collection__ = 'foo'
            skeleton = {
                "bar":int,
            }

        @self.connection.register
        class MyDoc(Document):
            __database__ = 'test'
            __collection__ = 'mydocs'
            skeleton = {
                "spam":{
                    "foo":ObjectId,
                }
            }
            serialize_mapping = {'spam.foo': 'foo'}

            @property
            def foo(self):
                return self.db.Foo.get_from_id(self['spam']['foo'])

        foo = self.connection.Foo()
        foo['bar'] = 42
        foo.save()

        mydoc = self.connection.MyDoc()
        mydoc['spam']['foo'] = foo['_id']
        mydoc.save()
        
        mydoc = self.connection.MyDoc.get_from_id(mydoc['_id'])
        self.assertEqual(mydoc.foo['_id'], foo['_id'])
        self.assertEqual(mydoc, {'_id':mydoc['_id'], 'spam':{'foo': foo['_id']}})
        self.assertEqual(mydoc.serialize(), {'_id': mydoc['_id'], 'spam':{'foo': {'_id':foo['_id'], 'bar':42}}})
        self.assertEqual(json.dumps(mydoc, default=json_util.default), 
          '{"_id": {"$oid": "%s"}, "spam": {"foo": {"$oid": "%s"}}}' % (mydoc['_id'], foo['_id']))
        self.assertEqual(json.dumps(mydoc.serialize(), default=json_util.default),
          '{"_id": {"$oid": "%s"}, "spam": {"foo": {"_id": {"$oid": "%s"}, "bar": 42}}}' % (mydoc['_id'], foo['_id']))



