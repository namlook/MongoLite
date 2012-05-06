#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2011, Nicolas Clairon
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

from mongolite.schema_document import SchemaProperties, SchemaDocument, STRUCTURE_KEYWORDS
from mongolite.helpers import DotedDict
from cursor import Cursor
from helpers import DotCollapsedDict, DotExpandedDict
from bson import BSON
from bson.binary import Binary
from bson.code import Code
from bson.dbref import DBRef
from bson.objectid import ObjectId
import pymongo
from gridfs import GridFS
import re
from copy import deepcopy
from uuid import UUID
import logging
import warnings

STRUCTURE_KEYWORDS += ['_id', '_ns', '_revision', '_version']

log = logging.getLogger(__name__)

from mongo_exceptions import ConnectionError, OperationFailure, BadIndexError

class DocumentProperties(SchemaProperties):
    def __new__(cls, name, bases, attrs):
        for base in bases:
            parent = base.__mro__[0]
            if getattr(parent, 'skeleton', None) or getattr(parent, 'optional', None):
                if parent.indexes:
                    if 'indexes' not in attrs:
                        attrs['indexes'] = []
                    for index in attrs['indexes']+parent.indexes:
                        if index not in attrs['indexes']:
                            attrs['indexes'].append(index)
        return SchemaProperties.__new__(cls, name, bases, attrs)

    @classmethod
    def _validate_descriptors(cls, attrs):
        SchemaProperties._validate_descriptors(attrs)
        if attrs.get('indexes'):
            for index in attrs['indexes']:
                if 'fields' not in index:
                    raise BadIndexError(
                          "`fields` key must be specify in indexes")
                if not index.get('check', True):
                    continue
                for key, value in index.iteritems():
                    if key == "fields":
                        if isinstance(value, basestring):
                            if value not in attrs['_namespaces'] and value not in STRUCTURE_KEYWORDS:
                                raise ValueError(
                                  "Error in indexes: can't find %s in skeleton or optional" % value )
                        elif isinstance(value, list):
                            for val in value:
                                if isinstance(val, tuple):
                                    field, direction = val
                                    if field not in attrs['_namespaces'] and field not in STRUCTURE_KEYWORDS:
                                        raise ValueError(
                                          "Error in indexes: can't find %s in skeleton or optional" % field )
                                    if not direction in [pymongo.DESCENDING, pymongo.ASCENDING, pymongo.OFF, pymongo.ALL, pymongo.GEO2D]:
                                        raise BadIndexError(
                                          "index direction must be INDEX_DESCENDING, INDEX_ASCENDING, INDEX_OFF, INDEX_ALL or INDEX_GEO2D. Got %s" % direction)
                                else:
                                    raise BadIndexError(
                                      "fields must be a string or a list of tuples (got %s instead)" % type(value))
                        else:
                            raise BadIndexError(
                              "fields must be a string or a list of tuples (got %s instead)" % type(value))

class Document(SchemaDocument):

    __metaclass__ = DocumentProperties

    type_field = '_type'

    serialize_mapping = {}

    indexes = None

    use_gridfs = False
    __gridfs_collection__ = None

    authorized_types = SchemaDocument.authorized_types + [
      Binary,
      ObjectId,
      DBRef,
      Code,
      UUID,
      type(re.compile("")),
    ]

    def __init__(self, doc=None, gen_skel=True, collection=None):
        self._authorized_types = self.authorized_types[:]
        super(Document, self).__init__(doc=doc, gen_skel=gen_skel, gen_auth_types=False)
        if self.type_field in self:
            self[self.type_field] = self.__class__.__name__
        # collection
        self.collection = collection
        if collection:
            self.db = collection.database
            self.connection = self.db.connection
            # gridfs
            if self.use_gridfs:
                gridcol = self.collection.name+'fs'
                if self.__gridfs_collection__:
                    gridcol = self.__gridfs_collection__
                self.fs = GridFS(database=self.db, collection=gridcol)
        else:
            self.fs = None

    def get_son_object(self):
        return BSON.encode(self)

    def find(self, *args, **kwargs):
        """
        Query the database.

        The `spec` argument is a prototype document that all results must
        match. For example if self si called MyDoc:

        >>> mydocs = db.test.MyDoc.find({"hello": "world"})

        only matches documents that have a key "hello" with value "world".
        Matches can have other keys *in addition* to "hello". The `fields`
        argument is used to specify a subset of fields that should be included
        in the result documents. By limiting results to a certain subset of
        fields you can cut down on network traffic and decoding time.

        `mydocs` is a cursor which yield MyDoc object instances.

        See pymongo's documentation for more details on arguments.
        """
        return self.collection.find(wrap=self._obj_class, *args, **kwargs)

    def find_one(self, *args, **kwargs):
        """
        Get the first object found from the database.

        See pymongo's documentation for more details on arguments.
        """
        return self.collection.find_one(wrap=self._obj_class, *args, **kwargs)

    def find_random(self):
        """
        return one random document from the collection
        """
        import random
        max = self.collection.count()
        if max:
            num = random.randint(0, max-1)
            return self.find().skip(num).next()

    def find_and_modify(self, *args, **kwargs):
        """
        Update and return an object.
        """
        return self.collection.find_and_modify(wrap=self._obj_class, *args, **kwargs)

    def get_from_id(self, id):
        """
        return the document which has the id
        """
        return self.find_one({"_id":id})

    def reload(self):
        # XXX remove ?
        """
        allow to refresh the document, so after using update(), it could reload
        its value from the database.
        
        Be carrefull : reload() will erase all unsaved values.

        If no _id is set in the document, a KeyError is raised.
        """
        warnings.warn("This method is deprecated and will be removed in a near future", 
          DeprecationWarning)
        old_doc = self.collection.get_from_id(self['_id'])
        if not old_doc:
            raise OperationFailure('Can not reload an unsaved document.'
              ' %s is not found in the database' % self['_id'])
        else:
            self.update(DotedDict(old_doc))

    def get_dbref(self):
        """
        return a pymongo DBRef instance related to the document
        """
        assert '_id' in self, "You must specify an '_id' for using this method"
        return DBRef(database=self.db.name, collection=self.collection.name, id=self['_id'])

    def save(self, *args, **kwargs):
        """
        save the document into the db.

        `save()` follow the pymongo.collection.save arguments
        """
        self.collection.save(self, *args, **kwargs)

    def delete(self):
        """
        delete the document from the collection from his _id.
        """
        self.collection.remove({'_id':self['_id']})

    def generate_indexes(self):
        """
        Ensures that all indexes described in self.indexes exist on the collection.
        """
        if self.indexes:
            for index in self.indexes:
                kwargs = {}
                kwargs.update(index)
                fields = kwargs.pop('fields')
                kwargs.pop('check', None)
                self.collection.ensure_index(fields, **kwargs)

    def serialize(self):
        doc = {}
        for k, v in DotCollapsedDict(self).iteritems():
            if "." in k:
                if self.serialize_mapping.get(k):
                    v = getattr(self, self.serialize_mapping[k])
                else:
                    default_property_key = '__'.join(k.split('.'))
                    if hasattr(self, default_property_key):
                        v = getattr(self, default_property_key)
            elif hasattr(self, k):
                v = getattr(self, k)
            if isinstance(v, Cursor):
                v = list(v)
            doc[k] = v
        return DotExpandedDict(doc)

    #
    # End of public API
    #

    def __hash__(self):
        if '_id' in self:
            value = self['_id']
            return value.__hash__()
        else:
            raise TypeError("A Document is not hashable if it is not saved. Save the document before hashing it")

    def __deepcopy__(self, memo={}):
        obj = self.__class__(doc=deepcopy(dict(self), memo), gen_skel=False, collection=self.collection)
        obj.__dict__ = self.__dict__.copy()
        return obj

    def __getattribute__(self, key):
        # XXX remove ?
        if key in ['collection', 'db', 'connection']:
            if self.__dict__.get(key) is None:
                raise ConnectionError('No collection found') 
        return super(Document, self).__getattribute__(key)
