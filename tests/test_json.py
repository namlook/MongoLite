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
from mongolite.helpers import json_util_default, json_util_object_hook
import json
import datetime


class JsonTestCase(unittest.TestCase):
    def setUp(self):
        self.connection = Connection()
        self.col = self.connection['test']['mongolite']
        
    def tearDown(self):
        self.connection['test'].drop_collection('mongolite')

    def test_simple_to_json(self):
        @self.connection.register
        class MyDoc(Document):
            skeleton = {
                "bla":{
                    "foo":unicode,
                    "bar":int,
                    "egg":datetime.datetime,
                },
                "spam":[],
            }
        mydoc = self.col.MyDoc()
        mydoc['_id'] = u'mydoc'
        mydoc["bla"]["foo"] = u"bar"
        mydoc["bla"]["bar"] = 42
        mydoc['bla']['egg'] = datetime.datetime(2010, 1, 1)
        mydoc['spam'] = range(10)
        mydoc.save()
        json_doc = json.dumps(mydoc, default=json_util_default)
        self.assertEqual(json_doc, '{"_id": "mydoc", "bla": {"bar": 42, "foo": "bar", "egg": {"$date": 1262304000000}}, "spam": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]}')

        mydoc = self.col.MyDoc()
        mydoc['_id'] = u'mydoc2'
        mydoc["bla"]["foo"] = u"bar"
        mydoc["bla"]["bar"] = 42
        mydoc['spam'] = [datetime.datetime(2000, 1, 1), datetime.datetime(2008, 8, 8)]
        mydoc.save()
        json_doc = json.dumps(mydoc, default=json_util_default)
        self.assertEqual(json_doc, '{"_id": "mydoc2", "bla": {"bar": 42, "foo": "bar", "egg": null}, "spam": [{"$date": 946684800000}, {"$date": 1218153600000}]}')

    def test_simple_from_json(self):
        @self.connection.register
        class MyDoc(Document):
            skeleton = {
                "bla":{
                    "foo":unicode,
                    "bar":int,
                },
                "spam":[],
            }
        json_doc = '{"_id": "mydoc", "bla": {"bar": 42, "foo": "bar", "egg": {"$date": 1262304000000}}, "spam": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]}'
        doc = json.loads(json_doc, object_hook=json_util_object_hook)
        mydoc = self.col.MyDoc(doc)
        new_json_doc = json.dumps(mydoc, default=json_util_default)
        self.assertEqual(json.loads(json_doc, object_hook=json_util_object_hook),
            json.loads(new_json_doc, object_hook=json_util_object_hook))

    def test_from_json_with_oid(self):
        @self.connection.register
        class MyDoc(Document):
            skeleton = {
                "foo":unicode,
            }
        
        mydoc = self.col.MyDoc()
        mydoc['foo'] = u'bla'
        mydoc.save()
        json_doc = json.dumps(mydoc, default=json_util_default)
        self.assertEqual(json_doc, '{"foo": "bla", "_id": {"$oid": "%s"}}' % mydoc['_id'])
        doc = json.loads(json_doc, object_hook=json_util_object_hook)
        self.assertEqual(doc, {u'foo': u'bla', u'_id': mydoc['_id']})
