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
from gridfs import NoFile


class GridFSTestCase(unittest.TestCase):
    def setUp(self):
        self.connection = Connection()
        self.col = self.connection['test']['mongolite']
        
    def tearDown(self):
        self.connection.drop_database('test')

    def test_simple_gridfs(self):
        @self.connection.register
        class Doc(Document):
            use_gridfs = True
            skeleton = {
                'title':unicode,
            }
        doc = self.col.Doc()
        doc['title'] = u'Hello'
        doc.save()

        self.assertEqual(doc.fs._GridFS__collection.name, 'mongolitefs')
        self.assertEqual(doc.fs._GridFS__collection.name, self.col.name+'fs')

        self.assertRaises(NoFile, doc.fs.get_last_version, 'source', doc_id=doc['_id'])
        doc.fs.put("Hello World !", filename='source', doc_id=doc['_id'])
        obj = doc.fs.get_last_version(filename="source", doc_id=doc['_id'])
        self.assertTrue(doc.fs.exists(obj._id))
        self.assertEqual(doc.fs.get_last_version('source', doc_id=doc['_id']).read(), "Hello World !")

        doc = self.col.Doc.find_one({'title':'Hello'})
        self.assertEqual(doc.fs.get_last_version('source', doc_id=doc['_id']).read(), u"Hello World !")

        f = doc.fs.get_last_version('source', doc_id=doc['_id'])
        self.assertEqual(f.name, 'source')

        doc.fs.delete(f._id)

        assertion = False
        try:
            doc.fs.get_last_version('source', doc_id=doc['_id'])
        except NoFile:
            assertion = True
        self.assertTrue(assertion)

    def test_simple_unique_shared_gridfs(self):
        @self.connection.register
        class DocA(Document):
            use_gridfs = True
            __gridfs_collection__ = 'fs'
            skeleton = {
                'title':unicode,
            }
        doc = self.col.DocA()
        doc['title'] = u'Hello'
        doc.save()
        doc.fs.put('blah', filename='shared_file.txt')

        @self.connection.register
        class DocB(Document):
            use_gridfs = True
            __gridfs_collection__ = 'fs'
            skeleton = {
                'title':unicode,
            }
        doc = self.col.DocB()
        doc['title'] = u'Hello'
        doc.save()

        self.assertEqual(doc.fs.get_last_version('shared_file.txt').read(), "blah")

    def test_pymongo_compatibility(self):
        class Doc(Document):
            skeleton = {
                'title':unicode,
            }
            use_gridfs = True
        self.connection.register([Doc])
        doc = self.col.Doc()
        doc['title'] = u'Hello'
        doc.save()
        id = doc.fs.put("Hello World", filename="source")
        assert doc.fs.get(id).read() == 'Hello World'
        assert doc.fs.get_last_version("source").name == 'source'
        assert doc.fs.get_last_version("source").read() == 'Hello World'
        f = doc.fs.new_file(filename="source")
        f.write("New Hello World!")
        f.close()
        self.assertEqual(doc.fs.get_last_version('source').read(), 'New Hello World!')
        new_id = doc.fs.get_last_version("source")._id
        doc.fs.delete(new_id)
        self.assertEqual(doc.fs.get_last_version('source').read(), 'Hello World')
         
