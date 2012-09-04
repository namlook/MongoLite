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

from pymongo.collection import Collection as PymongoCollection
from cursor import Cursor

class Collection(PymongoCollection):

    def __init__(self, *args, **kwargs):
        self._documents = {}
        self._collections = {}
        super(Collection, self).__init__(*args, **kwargs)
        self._registered_documents = self.database.connection._registered_documents

    def __getattr__(self, key):
        if key in self._registered_documents:
            if not key in self._documents:
                self._documents[key] = self._registered_documents[key](collection=self)
            return self._documents[key]
        else:
            newkey = u"%s.%s" % (self.name, key)
            if not newkey in self._collections:
                self._collections[newkey] = Collection(self.database, newkey)
            return self._collections[newkey]

    def __call__(self, *args, **kwargs):
        if "." not in self._Collection__name:
            raise TypeError("'Collection' object is not callable. If you "
                            "meant to call the '%s' method on a 'Database' "
                            "object it is failing because no such method "
                            "exists." %
                            self._Collection__name)
        name = self._Collection__name.split(".")[-1]
        raise TypeError("'Collection' object is not callable. "
          "If you meant to call the '%s' method on a 'Collection' "
          "object it is failing because no such method exists.\n"
          "If '%s' is a Document then you may have forgotten to "
          "register it to the connection." % (name, name))

    def find(self, *args, **kwargs):
        return Cursor(self, *args, **kwargs)
    find.__doc__ = PymongoCollection.find.__doc__ + """
        added by mongolite::
            - `wrap` (optional): a class object used to wrap
            documents in the query result
    """

    def find_and_modify(self, *args, **kwargs):
        obj_class = kwargs.pop('wrap', None)
        doc = super(Collection, self).find_and_modify(*args, **kwargs)
        if obj_class:
            return self.collection[obj_class.__name__](doc)
        return doc
    find_and_modify.__doc__ = PymongoCollection.find_and_modify.__doc__ + """
        added by mongolite::
             - `wrap` (optional): a class object used to wrap
             documents in the query result
     """

    def get_from_id(self, id):
        # XXX remove ?
        """
        return the document wich has the id
        """
        return self.find_one({"_id":id})

    def find_random(self):
        """
        return one random document from the collection
        """
        import random
        max = self.count()
        if max:
            num = random.randint(0, max-1)
            return self.find().skip(num).next()

