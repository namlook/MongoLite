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

from mongolite import Document, Connection
from mongolite.schema_document import SchemaDocument

class InheritanceTestCase(unittest.TestCase):
    def setUp(self):
        self.connection = Connection()
        self.col = self.connection['test']['mongolite']
        
    def tearDown(self):
        self.connection['test'].drop_collection('mongolite')

    def test_simple_inheritance(self):
        class A(SchemaDocument):
            skeleton = {
                "a":{"foo":int}
            }

        class B(A):
            skeleton = {
                "b":{"bar":unicode}
            }

        assert B() == {"a":{"foo":None}, "b":{"bar":None}}, B()

    def test_simple_inheritance_with_only_optional(self):
        class A(SchemaDocument):
            optional = {
                'optfoo': int,
            }

        class B(A):
            optional = {
                'optbar': float,
            }

        self.assertEqual(B(), {'optfoo':None, 'optbar':None})
 
    def test_simple_inheritance_with_optional(self):
        class A(SchemaDocument):
            skeleton = {
                'reqfoo': unicode,
            }
            optional = {
                'optfoo': int,
            }

        class B(A):
            optional = {
                'optbar': float,
            }

        self.assertEqual(B(), {'reqfoo':None, 'optfoo':None, 'optbar':None})

    def test_gen_doc(self):
        class A(SchemaDocument):
            skeleton = {
                'reqfoo': unicode,
            }
            optional = {
                'optfoo': int,
            }

        class B(A):
            """this is a test doc"""
            optional = {
                'optbar': float,
            }

        self.assertEqual(B.__doc__, """this is a test doc
    required fields: {
        reqfoo : <type 'unicode'>
    }
    optional fields: {
        optbar : <type 'float'>
        optfoo : <type 'int'>
    }
""")
 
    def test_default_values_inheritance(self):
        class A(SchemaDocument):
            skeleton = {
                "a":{"foo":int}
            }
            default_values = {"a.foo":3}

        class B(A):
            skeleton = {
                "b":{"bar":unicode}
            }

        assert B() == {"a":{"foo":3}, "b":{"bar":None}}
 
        class C(A):
            skeleton = {
                "c":{"spam":unicode}
            }
            default_values = {"a.foo":5}

        assert C() == {"a":{"foo":5}, "c":{"spam":None}}, C()

    def test_default_values_inheritance_with_optional(self):
        class A(SchemaDocument):
            skeleton = {
                "a":{"foo":int}
            }
            optional = {
                "bar": int
            }
            default_values = {"a.foo":3, "bar": 42}

        class B(A):
            skeleton = {
                "b":{"bar":unicode}
            }

        assert B() == {"a":{"foo":3}, "b":{"bar":None}, "bar": 42}

        class C(A):
            skeleton = {
                "c":{"spam":unicode}
            }
            optional = {
                "bla": unicode,
            }
            default_values = {"a.foo":5, "bla": u"foo"}

        assert C() == {"a":{"foo":5}, "c":{"spam":None}, "bar": 42, "bla": u"foo"}, C()

    def test_default_values_inheritance_with_function(self):
        from datetime import datetime
        class A(SchemaDocument):
            skeleton = {
                "a":{"foo":datetime}
            }
            default_values = {"a.foo":datetime.utcnow}

        class B(A):
            skeleton = {
                "b":{"bar":unicode}
            }

        assert isinstance(B()['a']['foo'], datetime)
 
        class C(A):
            skeleton = {
                "c":{"spam":unicode}
            }
            default_values = {"a.foo":datetime(2008,8,8)}

        assert C() == {"a":{"foo":datetime(2008, 8, 8)}, "c":{"spam":None}}, C()

    def test_complete_inheritance(self):
        class A(SchemaDocument):
            skeleton = {
                "a":{"foo":int}
            }
            default_values = {"a.foo":3}

        class B(A):
            skeleton = {
                "b":{"bar":unicode}
            }
            default_values = {"a.foo":5}

        b =  B()
        assert b == {"a":{"foo":5}, "b":{"bar":None}}
 
        class C(B):
            skeleton = {
                "c":{"spam":unicode}
            }

        c =  C()
        assert c == {"a":{"foo":5}, "b":{"bar":None}, "c":{"spam":None}}, C()

    def test_polymorphism(self):
        class A(SchemaDocument):
            skeleton = {
                "a":{"foo":int}
            }
            default_values = {"a.foo":3}

        class B(SchemaDocument):
            skeleton = {
                "b":{"bar":unicode}
            }
            required_fields = ['b.bar']

        b =  B()
        assert b == {"b":{"bar":None}}
 
        class C(A,B):
            skeleton = {
                "c":{"spam":unicode}
            }
            default_values = {"a.foo":5}

        c =  C()
        assert c == {"a":{"foo":5}, "b":{"bar":None}, "c":{"spam":None}}, C()
   
    def test_simple_manual_inheritance(self):
        class A(SchemaDocument):
            auto_inheritance = False
            skeleton = {
                "a":{"foo":int}
            }

        class B(A):
            skeleton = {
                "b":{"bar":unicode}
            }
            skeleton.update(A.skeleton)

        assert B() == {"a":{"foo":None}, "b":{"bar":None}}
 
    def test_default_values_manual_inheritance(self):
        class A(SchemaDocument):
            auto_inheritance = False
            skeleton = {
                "a":{"foo":int}
            }
            default_values = {"a.foo":3}

        class B(A):
            skeleton = {
                "b":{"bar":unicode}
            }
            skeleton.update(A.skeleton)
            default_values = A.default_values

        assert B() == {"a":{"foo":3}, "b":{"bar":None}}
 
        class C(A):
            skeleton = {
                "c":{"spam":unicode}
            }
            skeleton.update(A.skeleton)
            default_values = A.default_values
            default_values.update({"a.foo":5})

        assert C() == {"a":{"foo":5}, "c":{"spam":None}}, C()
  
    def test_inheritance_with_document(self):
        class Doc(Document):
            skeleton = {
                "doc":{'bla':int},
            }
        class DocA(Doc):
            skeleton = {
                "doc_a":{'foo':int},
            }
        class DocB(DocA):
            skeleton = {
                "doc_b":{"bar":int},
            }
        self.connection.register([Doc, DocA, DocB])
        assert self.col.DocA() == {'doc': {'bla': None}, 'doc_a': {'foo': None}}
        assert self.col.DocB() == {'doc': {'bla': None}, 'doc_b': {'bar': None}, 'doc_a': {'foo': None}}
        # creating DocA
        for i in range(10):
            mydoc = self.col.DocA()
            mydoc['doc']["bla"] = i+1
            mydoc['doc_a']["foo"] = i
            mydoc.save()
        assert self.col.find_one({'_id':mydoc['_id']}) == {u'doc': {u'bla': 10}, u'_id': mydoc['_id'], u'doc_a': {u'foo': 9}}
        # creating DocB
        for i in range(5):
            mydoc = self.col.DocB()
            mydoc['doc']["bla"] = i+1
            mydoc['doc_a']["foo"] = i
            mydoc['doc_b']["bar"] = i+2
            mydoc.save()
        assert self.col.find_one({'_id':mydoc['_id']}) == {u'doc': {u'bla': 5}, u'_id': mydoc['_id'], u'doc_b': {u'bar': 6}, u'doc_a': {u'foo': 4}}


