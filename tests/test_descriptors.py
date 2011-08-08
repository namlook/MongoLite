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

from mongolite import Connection, Document

class DescriptorsTestCase(unittest.TestCase):
    def setUp(self):
        self.connection = Connection()
        self.col = self.connection['test']['mongolite']
        
    def tearDown(self):
        self.connection.drop_database('test')

    def test_default_values(self):
        class MyDoc(Document):
            skeleton = {
                "foo":int,
                "bla":unicode,
            }
            default_values = {"foo":42}
        mydoc = MyDoc()
        assert mydoc["foo"] == 42
        assert mydoc == {'foo':42, 'bla':None}, mydoc

    def test_default_values_with_optional(self):
        class MyDoc(Document):
            skeleton = {
                "foo":int,
                "bla":unicode,
            }
            optional = {
                "optfield": int,
            }
            default_values = {"foo":42, 'optfield':3}
        mydoc = MyDoc()
        self.assertEqual(mydoc["foo"], 42)
        self.assertEqual(mydoc.get('optfield'), 3)
        self.assertEqual(mydoc, {'foo':42, 'optfield': 3, 'bla':None})

    def test_default_values_nested(self):
        class MyDoc(Document):
            skeleton = {
                "bar":{
                    "foo":int,
                    "bla":unicode,
                }
            }
            default_values = {"bar.foo":42}
        mydoc = MyDoc()
        assert mydoc['bar']["foo"] == 42
        assert mydoc == {'bar':{'foo':42, 'bla':None}}, mydoc

    def test_default_values_nested_inheritance(self):
        import datetime
        class Core(Document):
            skeleton = {
                "core":{
                    "creation_date":datetime.datetime,
                }
            }
            default_values = {
                "core.creation_date": datetime.datetime(2010, 1, 1),
            }

        class MyDoc(Core):
            skeleton = {
                "bar":{
                    "foo":int,
                    "bla":unicode,
                }
            }
            default_values = {"bar.foo":42}

        class MyDoc2(MyDoc):
            skeleton = {
                "mydoc2":{
                    "toto":int
                }
            }
        mydoc = MyDoc2()
        assert mydoc['bar']["foo"] == 42
        assert mydoc == {'mydoc2': {'toto': None}, 'core': {'creation_date': datetime.datetime(2010, 1, 1, 0, 0)}, 'bar': {'foo': 42, 'bla': None}}

    def test_default_values_from_function(self):
        import time
        class MyDoc(Document):
            skeleton = {
                "foo":float
            }
            default_values = {"foo":time.time}
        mydoc = MyDoc()
        self.assertTrue(isinstance(mydoc['foo'], float))

    def test_default_values_from_function2(self):
        import time
        class Doc( Document ):
            skeleton = {
                "doc":{
                    "creation_date":float,
                    "updated_date": float,
                }
            }
            default_values = {
                "doc.creation_date": time.time,
                "doc.updated_date": time.time
            }
        doc = Doc()
        assert isinstance(doc['doc']['creation_date'], float), doc['doc']['creation_date']
        assert isinstance(doc['doc']['updated_date'], float)

    def test_default_values_from_function_nested(self):
        import time
        class MyDoc(Document):
            skeleton = {
                "foo":{"bar":float}
            }
            default_values = {"foo.bar":time.time}
        mydoc = MyDoc()
        self.assertTrue(isinstance(mydoc['foo']['bar'], float))
        self.assertTrue(mydoc['foo']['bar'] > 0)

    def test_default_list_values(self):
        class MyDoc(Document):
            skeleton = {
                "foo":[int]
            }
            default_values = {"foo":[42,3]}
        self.connection.register([MyDoc])
        mydoc = self.col.MyDoc()
        assert mydoc["foo"] == [42,3]
        mydoc['foo'] = [1,2,3]
        mydoc.save()
        mydoc = self.col.MyDoc()
        assert mydoc['foo'] == [42,3]

    def test_default_list_values_empty(self):
        class MyDoc(Document):
            skeleton = {
                "foo":list
            }
            default_values = {"foo":[3]}
        self.connection.register([MyDoc])
        mydoc = self.col.MyDoc()
        assert mydoc["foo"] == [3]
        mydoc['foo'].append(2)
        mydoc.save()
        mydoc = self.col.MyDoc()
        assert mydoc['foo'] == [3], mydoc


    def test_default_list_values_with_callable(self):
        def get_truth():
            return 42
        class MyDoc(Document):
            skeleton = {
                "foo":[int]
            }
            default_values = {"foo":[get_truth,3]}
        mydoc = MyDoc()
        assert mydoc["foo"] == [42,3]

    def test_default_list_nested_values(self):
        class MyDoc(Document):
            skeleton = {
                "foo":{
                    "bar":[int]
                }
            }
            default_values = {"foo.bar":[42,3]}
        mydoc = MyDoc()
        assert mydoc["foo"]["bar"] == [42,3]

    def test_default_dict_values(self):
        class MyDoc(Document):
            skeleton = {
                "foo":dict
            }
            default_values = {"foo":{"bar":42}}
        self.connection.register([MyDoc])
        mydoc = self.col.MyDoc()
        assert mydoc["foo"] == {"bar":42}, mydoc
        mydoc['foo'] = {'bar':1}
        mydoc.save()
        mydoc = self.col.MyDoc()
        assert mydoc["foo"] == {"bar":42}, mydoc

    def test_default_dict_values_empty(self):
        class MyDoc(Document):
            skeleton = {
                "foo":dict
            }
            default_values = {"foo":{}}
        self.connection.register([MyDoc])
        mydoc = self.col.MyDoc()
        print id(mydoc.skeleton['foo']), id(mydoc['foo']), id(mydoc.default_values['foo'])
        assert mydoc["foo"] == {}, mydoc
        mydoc['foo'][u'bar'] = 1
        mydoc.save()
        mydoc2 = self.col.MyDoc()
        print id(mydoc2.skeleton['foo']), id(mydoc2['foo']), id(mydoc2.default_values['foo'])
        assert mydoc2["foo"] == {}, mydoc

        class MyDoc(Document):
            skeleton = {
                "foo":{unicode:int}
            }
            default_values = {"foo":{}}
        self.connection.register([MyDoc])
        mydoc = self.col.MyDoc()
        assert mydoc["foo"] == {}, mydoc
        mydoc['foo'][u'bar'] = 1
        mydoc.save()
        mydoc2 = self.col.MyDoc()
        assert mydoc2["foo"] == {}, mydoc


    def test_default_dict_values_with_callable(self):
        def get_truth():
            return {'bar':42}
        class MyDoc(Document):
            skeleton = {
                "foo":{}
            }
            default_values = {"foo":get_truth}
        mydoc = MyDoc()
        assert mydoc["foo"] == {"bar":42}, mydoc
          
    def test_default_dict_checked_values(self):
        class MyDoc(Document):
            skeleton = {
                "foo":{unicode:int}
            }
            default_values = {"foo":{u"bar":42}}
        mydoc = MyDoc()
        assert mydoc["foo"] == {"bar":42}, mydoc

    def test_default_dict_nested_checked_values(self):
        class MyDoc(Document):
            skeleton = {
                "foo":{unicode:{"bla":int, "ble":unicode}}
            }
            default_values = {"foo":{u"bar":{"bla":42, "ble":u"arf"}}}
        mydoc = MyDoc()
        assert mydoc["foo"] == {u"bar":{"bla":42, "ble":u"arf"}}, mydoc

    def test_default_values_with_dict_in_list(self):
        @self.connection.register
        class MyDoc(Document):
            skeleton = {
                'bar': [{'foo':unicode}]
            }
            default_values = {
                'bar': [{'foo': u'bla'}]
            }
        doc = self.col.MyDoc()
        assert doc['bar'] == [{'foo': u'bla'}]

    def test_bad_default_values(self):
        failed = False
        try:
            class MyDoc(Document):
                skeleton = {
                    "foo":{"bar":int},
                }
                default_values = {"foo.bla":2}
        except ValueError, e:
            failed = True
            self.assertEqual(str(e), "Error in default_values: can't find foo.bla in skeleton")
        self.assertEqual(failed, True)
