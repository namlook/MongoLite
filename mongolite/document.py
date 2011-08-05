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

from mongolite.schema_document import SchemaDocument, STRUCTURE_KEYWORDS
from mongolite.helpers import DotedDict
from bson import BSON
from bson.binary import Binary
from bson.code import Code
from bson.dbref import DBRef
from pymongo.objectid import ObjectId
from gridfs import GridFS
import re
from copy import deepcopy
from uuid import UUID
import logging
import warnings

STRUCTURE_KEYWORDS += ['_id', '_ns', '_revision', '_version']

log = logging.getLogger(__name__)

from mongo_exceptions import ConnectionError, OperationFailure

class Document(SchemaDocument):

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

        if uuid is True, a uuid4 will be automatiquely generated
        else, the pymongo.ObjectId will be used.

        If validate is True, the `validate` method will be called before
        saving. Not that the `validate` method will be called *before* the
        uuid is generated.

        `save()` follow the pymongo.collection.save arguments
        """
        self.collection.save(self, *args, **kwargs)

    def delete(self):
        """
        delete the document from the collection from his _id.
        """
        self.collection.remove({'_id':self['_id']})

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
