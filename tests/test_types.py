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

from mongolite.schema_document import SchemaDocument
from mongolite import Document, Connection, StructureError, AuthorizedTypeError

class TypesTestCase(unittest.TestCase):

    def setUp(self):
        self.connection = Connection()
        self.col = self.connection['test']['mongolite']
        
    def tearDown(self):
        self.connection.drop_database('test')


    def test_authorized_type(self):
       for auth_type in SchemaDocument.authorized_types:
            if auth_type is dict:
                auth_type = {}
            class MyDoc(SchemaDocument):
                skeleton = { "foo":auth_type }
            if type(auth_type) is dict:
                self.assertEqual(MyDoc(), {"foo":{}})
            elif auth_type is list:
                self.assertEqual(MyDoc(), {"foo":[]})
            else:
                assert MyDoc() == {"foo":None}, auth_type
 
    def test_not_authorized_type(self):
        for unauth_type in [set]:
            failed = False
            try:
                class MyDoc(SchemaDocument):
                    skeleton = { "foo":[unauth_type] }
            except StructureError, e:
                self.assertEqual(str(e), "MyDoc: %s is not an authorized type" % unauth_type)
                failed = True
            self.assertEqual(failed, True)
            failed = False
            try:
                class MyDoc(SchemaDocument):
                    skeleton = { "foo":(unauth_type) }
            except StructureError, e:
                self.assertEqual(str(e), "MyDoc: %s is not an authorized type" % unauth_type)
                failed = True
            self.assertEqual(failed, True)
            failed = False
            try:
                class MyDoc2(SchemaDocument):
                    skeleton = { 'foo':[{int:unauth_type }]}
            except StructureError, e:
                self.assertEqual(str(e), "MyDoc2: %s is not an authorized type" % unauth_type)
                failed = True
            self.assertEqual(failed, True)
            failed = False
            try:
                class MyDoc3(SchemaDocument):
                    skeleton = { 'foo':[{unauth_type:int }]}
            except AuthorizedTypeError, e:
                self.assertEqual(str(e), "MyDoc3: %s is not an authorized type" % unauth_type)
                failed = True
            self.assertEqual(failed, True)

        failed = False
        try:
            class MyDoc4(SchemaDocument):
                skeleton = {1:unicode}
        except StructureError, e:
            self.assertEqual(str(e), "MyDoc4: 1 must be a basestring or a type")
            failed = True
        self.assertEqual(failed, True)

    def test_typed_tuple(self):
        @self.connection.register
        class MyDoc(Document):
            skeleton = {
                "foo":(int, unicode, float)
            }
        mydoc = self.col.MyDoc()
        self.assertEqual(mydoc, {'foo':[None, None, None]})
        mydoc['foo'] = [u"bla", 1, 4.0]
        mydoc.save()

    def test_nested_typed_tuple(self):
        @self.connection.register
        class MyDoc(Document):
            skeleton = {
                "foo":{'bar':(int, unicode, float)}
            }
        mydoc = self.col.MyDoc()
        self.assertEqual(mydoc, {'foo':{'bar': [None, None, None]}})
        mydoc['foo']['bar'] = [u"bla", 1, 4.0]
        mydoc.save()

    def test_saving_tuple(self):
        class MyDoc(Document):
            skeleton = { 'foo': (int, unicode, float) }
        self.connection.register([MyDoc])

        mydoc = self.col.MyDoc()
        assert mydoc == {'foo': [None, None, None]}, mydoc
        mydoc['foo'] = (1, u'a', 1.1) # note that this will be converted to list
        assert mydoc == {'foo': (1, u'a', 1.1000000000000001)}, mydoc
        mydoc.save()
        mydoc = self.col.find_one()

        class MyDoc(Document):
            skeleton = {'foo':[unicode]}
        self.connection.register([])
        self.connection.register([MyDoc])
        mydoc = self.col.MyDoc()
        mydoc['foo'] = (u'bla', u'bli', u'blu', u'bly')
        mydoc.save()
        mydoc = self.col.get_from_id(mydoc['_id'])


    def test_nested_typed_tuple_in_list(self):
        @self.connection.register
        class MyDoc(Document):
            skeleton = {
                "foo":{'bar':[(int, unicode, float)]}
            }
        mydoc = self.col.MyDoc()
        self.assertEqual(mydoc, {'foo': {'bar': []}})
        mydoc['foo']['bar'].append([None, u"bla", 3.1])
        mydoc['foo']['bar'][0][0] = 50
        mydoc.save()

    def test_with_custom_object(self):
        class MyDict(dict):
            pass
        @self.connection.register
        class MyDoc(Document):
            skeleton = {
                "foo":{unicode:int}
            }
        mydoc = self.col.MyDoc()
        self.assertEqual(mydoc, {'foo': {}})
        mydict = MyDict()
        mydict[u"foo"] = 3
        mydoc["foo"] = mydict
        mydoc.save()
 
    def test_custom_object_as_type(self):
        class MyDict(dict):
            pass
        @self.connection.register
        class MyDoc(Document):
            skeleton = {
                "foo":MyDict({unicode:int})
            }
        mydoc = self.col.MyDoc()
        mydict = MyDict()
        mydict[u"foo"] = 3
        mydoc["foo"] = mydict
        mydoc.save()
        self.assertTrue(isinstance(self.col.MyDoc.find_one()['foo'], dict))
        self.assertEqual(self.col.MyDoc.find_one(), {u'_id': mydoc['_id'], u'foo': {u'foo': 3}})
        mydoc['foo'] = {u"foo":"7"}
        mydoc.save()

        class MyInt(int):
            pass
        @self.connection.register
        class MyDoc(Document):
            skeleton = {
                "foo":MyInt,
            }
        mydoc = self.col.MyDoc()
        mydoc["foo"] = MyInt(3)
        mydoc['foo'] = 3
        mydoc.save()

    def test_adding_custom_type(self):
        class MyDoc(SchemaDocument):
            skeleton = {
                "foo":str,
            }
            authorized_types = SchemaDocument.authorized_types + [str]
        mydoc = MyDoc()
        self.assertEqual(mydoc, {'foo': None})
    
    def test_subclassed_type(self):
        """
        accept all subclass of supported type
        """
        class CustomFloat(float):
            pass
        @self.connection.register
        class MyDoc(Document):
            skeleton = {
                "foo":float,
            }
        mydoc = self.col.MyDoc()
        mydoc['foo'] = CustomFloat(4)
        mydoc.save()
        self.assertEqual(self.col.find_one(), {u'_id': mydoc['_id'], u'foo': 4.0})

    def test_uuid_type(self):
        import uuid
        @self.connection.register
        class MyDoc(Document):
            skeleton = {
                'uuid': uuid.UUID,
            }
        uid = uuid.uuid4()
        obj = self.col.MyDoc()
        obj['uuid'] = uid
        obj.save()

        assert isinstance(self.col.MyDoc.find_one()['uuid'], uuid.UUID)
