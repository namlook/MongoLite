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

from mongolite import Document, Connection, StructureError
from mongolite.schema_document import SchemaDocument

class StructureTestCase(unittest.TestCase):
    def setUp(self):
        self.connection = Connection()
        self.col = self.connection['test']['mongolite']
        
    def tearDown(self):
        self.connection['test'].drop_collection('mongolite')

    def test_no_skeleton(self):
        class MyDoc(SchemaDocument):
            pass

        doc = MyDoc()
        self.assertEqual(doc, {})

    def test_empty_skeleton(self):
        class MyDoc(SchemaDocument):
            skeleton = {}
        self.assertEqual(MyDoc(), {})

    def test_skeleton_not_dict(self):
        failed = False
        try:
            class MyDoc(SchemaDocument):
                skeleton = 3
        except StructureError:
            failed = True
        self.assertEqual(failed, True)

    def test_load_with_dict(self):
        doc = {"foo":1, "bla":{"bar":u"spam"}}
        class MyDoc(SchemaDocument):
            skeleton = {"foo":int, "bla":{"bar":unicode}}
        mydoc = MyDoc(doc)
        self.assertEqual(mydoc, doc)
        
    def test_simple_skeleton(self):
        class MyDoc(SchemaDocument):
            skeleton = {
                "foo":unicode,
                "bar":int
            }
        self.assertEqual(MyDoc(), {"foo":None, "bar":None})

    def test_simple_skeleton_and_optional(self):
        class MyDoc(SchemaDocument):
            skeleton = {
                'reqfoo': unicode,
            }
            optional = {
                'optfoo': int,
            }
        self.assertEqual(MyDoc(), {'reqfoo':None, 'optfoo':None})

    def test_missed_field(self):
        doc = {"foo":u"arf"}
        class MyDoc(SchemaDocument):
            skeleton = {
                "foo":unicode,
                "bar":{"bla":int}
            }
        mydoc = MyDoc(doc)
        self.assertEqual(mydoc, {'foo': u'arf'})

    def test_unknown_field(self):
        class MyDoc(SchemaDocument):
            skeleton = {
                "foo":unicode,
            }
        mydoc = MyDoc()
        self.assertEqual(mydoc, {'foo':None})

    def test_None(self):
        class MyDoc(SchemaDocument):
            skeleton = {
                "foo":None,
                "bar":{
                    "bla":None
                }
            }
        mydoc = MyDoc()
        self.assertEqual(mydoc, {'foo': None, 'bar': {'bla': None}})

    def test_big_nested_skeleton(self):
        class MyDoc(SchemaDocument):
            skeleton = {
                "1":{
                    "2":{
                        "3":{
                            "4":{
                                "5":{
                                    "6":{
                                        "7":int,
                                        "8":{
                                            unicode:{int:int}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        mydoc = MyDoc()
        self.assertEqual(mydoc._namespaces, ['1', '1.2', '1.2.3', '1.2.3.4', '1.2.3.4.5', '1.2.3.4.5.6', '1.2.3.4.5.6.8', '1.2.3.4.5.6.8.$unicode', '1.2.3.4.5.6.8.$unicode.$int', '1.2.3.4.5.6.7'])
        self.assertEqual(mydoc, {'1': {'2': {'3': {'4': {'5': {'6': {'8': {}, '7': None}}}}}}})
 
    def test_big_nested_skeleton_mongo_document(self):
        class MyDoc(Document):
            skeleton = {
                "1":{
                    "2":{
                        "3":{
                            "4":{
                                "5":{
                                    "6":{
                                        "7":int,
                                        "8":{
                                            unicode:{unicode:int}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        self.connection.register([MyDoc])
        mydoc = self.col.MyDoc()
        self.assertEqual(mydoc, {'1': {'2': {'3': {'4': {'5': {'6': {'8': {}, '7': None}}}}}}})
        self.assertEqual(mydoc._namespaces, ['1', '1.2', '1.2.3', '1.2.3.4', '1.2.3.4.5', '1.2.3.4.5.6', '1.2.3.4.5.6.8', '1.2.3.4.5.6.8.$unicode', '1.2.3.4.5.6.8.$unicode.$unicode', '1.2.3.4.5.6.7'])
        mydoc['1']['2']['3']['4']['5']['6']['7'] = 8
        mydoc['1']['2']['3']['4']['5']['6']['8'] = {u"bla":{"3":u"bla"}}
        mydoc.save()
            
    def test_field_changed(self):
        class MyDoc(Document):
            skeleton = {
                'foo':int,
                'bar':unicode,
            }
        self.connection.register([MyDoc])
        
        doc = self.col.MyDoc()
        doc['foo'] = 3
        doc.save()

        class MyDoc(Document):
            skeleton = {
                'foo':int,
                'arf': unicode,
            }
        self.connection.register([MyDoc])
        
        fetched_doc = self.col.MyDoc.find_one()
        fetched_doc['foo'] = 2
        fetched_doc.save()

    def test_exception_bad_skeleton(self):
        import datetime
        failed = False
        try:
            class MyDoc(SchemaDocument):
                skeleton = {
                    'topic': unicode,
                    'when': datetime.datetime.utcnow,
                }
        except TypeError, e:
            assert str(e).startswith("MyDoc: <built-in method utcnow of type object at "), str(e)
            assert str(e).endswith("is not a type")
            failed = True
        self.assertEqual(failed, True)

