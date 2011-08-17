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

import datetime
import logging
from copy import deepcopy

log = logging.getLogger(__name__)

from mongo_exceptions import StructureError, BadKeyError, AuthorizedTypeError

from helpers import DotCollapsedDict

# field wich does not need to be declared into the skeleton
STRUCTURE_KEYWORDS = []

class SchemaProperties(type):
    def __new__(cls, name, bases, attrs):
        attrs['_protected_field_names'] = set(
            ['_protected_field_names', '_namespaces', '_required_namespace'])
        for base in bases:
            parent = base.__mro__[0]
            if hasattr(parent, 'skeleton'):
                if parent.skeleton is not None:
                    if parent.skeleton:
                        if 'skeleton' not in attrs and parent.skeleton:
                            attrs['skeleton'] = parent.skeleton
                        else:
                            obj_skeleton = attrs.get('skeleton', {}).copy()
                            attrs['skeleton'] = parent.skeleton.copy()
                            attrs['skeleton'].update(obj_skeleton)
            if hasattr(parent, 'optional'):
                if parent.optional is not None:
                    if parent.optional:
                        if 'optional' not in attrs and parent.optional:
                            attrs['optional'] = parent.optional
                        else:
                            obj_optional = attrs.get('optional', {}).copy()
                            attrs['optional'] = parent.optional.copy()
                            attrs['optional'].update(obj_optional)
            if hasattr(parent, 'default_values'):
                if parent.default_values:
                    obj_default_values = attrs.get('default_values', {}).copy()
                    attrs['default_values'] = parent.default_values.copy()
                    attrs['default_values'].update(obj_default_values)
            if hasattr(parent, 'skeleton') or hasattr(parent, 'optional'):
                if attrs.get('authorized_types'):
                    attrs['authorized_types'] = list(set(parent.authorized_types).union(set(attrs['authorized_types'])))
        for mro in bases[0].__mro__:
            attrs['_protected_field_names'] = attrs['_protected_field_names'].union(list(mro.__dict__))
        attrs['_protected_field_names'] = list(attrs['_protected_field_names'])
        attrs['_namespaces'] = []
        attrs['_collapsed_struct'] = {}
        if (attrs.get('skeleton') or attrs.get('optional')) and name not in ["SchemaDocument", "Document"]:
            base = bases[0]
            if not attrs.get('authorized_types'):
                attrs['authorized_types'] = base.authorized_types
            if attrs.get('skeleton'):
                base._validate_skeleton(attrs['skeleton'], name, attrs.get('authorized_types'))
                attrs['_namespaces'].extend(list(base._SchemaDocument__walk_dict(attrs['skeleton'])))
                attrs['_collapsed_struct'].update(DotCollapsedDict(attrs['skeleton'], remove_under_type=True))
            if attrs.get('optional'):
                base._validate_skeleton(attrs['optional'], name, attrs.get('authorized_types'))
                attrs['_namespaces'].extend(list(base._SchemaDocument__walk_dict(attrs['optional'])))
                attrs['_collapsed_struct'].update(DotCollapsedDict(attrs['optional'], remove_under_type=True))
            cls._validate_descriptors(attrs)
        if (attrs.get('skeleton') or attrs.get('optional')):
            skel_doc = ""
            for k, v in attrs.get('skeleton', {}).iteritems():
                skel_doc += " "*8+k+" : "+str(v)+"\n"
            opt_doc = ""
            for k, v in attrs.get('optional', {}).iteritems():
                opt_doc += " "*8+k+" : "+str(v)+"\n"
            attrs['__doc__'] = attrs.get('__doc__', '')+"""
    required fields: {
%s    }
    optional fields: {
%s    }
""" % (skel_doc, opt_doc)
        return type.__new__(cls, name, bases, attrs)        

    @classmethod
    def _validate_descriptors(cls, attrs):
        for dv in attrs.get('default_values', {}):
            if not dv in attrs['_namespaces']:
                raise ValueError("Error in default_values: can't find %s in skeleton" % dv )

class SchemaDocument(dict):
    __metaclass__ = SchemaProperties
    
    skeleton = None
    optional = None
    default_values = {}

    authorized_types = [
      type(None),
      bool,
      int,
      long,
      float,
      unicode,
      str,
      basestring,
      list, 
      dict,
      datetime.datetime, 
    ]

    def __init__(self, doc=None, gen_skel=True, gen_auth_types=True):
        """
        doc : a dictionnary
        gen_skel : if True, generate automaticly the skeleton of the doc
            filled with NoneType each time validate() is called. Note that
            if doc is not {}, gen_skel is always False. If gen_skel is False,
            default_values cannot be filled.
        gen_auth_types: if True, generate automaticly the self.authorized_types
            attribute from self.authorized_types
        """
        if self.skeleton is None:
            self.skeleton = {}
        # init
        if doc:
            for k, v in doc.iteritems():
                self[k] = v
            gen_skel = False
        if gen_skel:
            self.generate_skeleton()
            if self.default_values:
                if self.skeleton:
                    self._set_default_fields(self, self.skeleton)
                if self.optional:
                    self._set_default_fields(self, self.optional)

    def generate_skeleton(self):
        """
        validate and generate the skeleton of the document
        from the skeleton (unknown values are set to None)
        """
        if self.skeleton:
            self.__generate_skeleton(self, self.skeleton)
        if self.optional:
            self.__generate_skeleton(self, self.optional)

    #
    # Public API end
    #
 
    @classmethod
    def __walk_dict(cls, dic):
        # thanks jean_b for the patch
        for key, value in dic.items():
            if isinstance(value, dict) and len(value):
                if type(key) is type:
                    yield '$%s' % key.__name__
                else:
                    yield key
                for child_key in cls.__walk_dict(value):
                    if type(key) is type:
                        new_key = "$%s" % key.__name__
                    else:
                        new_key = key
                    #if type(child_key) is type:
                    #    new_child_key = "$%s" % child_key.__name__
                    #else:
                    if type(child_key) is not type:
                        new_child_key = child_key
                    yield '%s.%s' % (new_key, new_child_key)
            elif type(key) is type:
                yield '$%s' % key.__name__
#            elif isinstance(value, list) and len(value):
#                if isinstance(value[0], dict):
#                    for child_key in cls.__walk_dict(value[0]):
#                        #if type(key) is type:
#                        #    new_key = "$%s" % key.__name__
#                        #else:
#                        if type(key) is not type:
#                            new_key = key
#                        #if type(child_key) is type:
#                        #    new_child_key = "$%s" % child_key.__name__
#                        #else:
#                        if type(child_key) is not type:
#                            new_child_key = child_key
#                        yield '%s.%s' % (new_key, new_child_key)
#                else:
#                    if type(key) is not type:
#                        yield key
#                    #else:
#                    #    yield ""
            else:
                if type(key) is not type:
                    yield key
                #else:
                #    yield ""

    @classmethod
    def _validate_skeleton(cls, skeleton, name, authorized_types):
        """
        validate if all fields in self.skeleton are in authorized types.
        """
        ##############
        def __validate_skeleton(struct, name,  authorized):
            if type(struct) is type:
                if struct not in authorized_types:
                    if struct not in authorized_types:
                        raise StructureError("%s: %s is not an authorized type" % (name, struct))
            elif isinstance(struct, dict):
                for key in struct:
                    if isinstance(key, basestring):
                        if "." in key: 
                            raise BadKeyError(
                              "%s: %s must not contain '.'" % (name, key))
                        if key.startswith('$'): 
                            raise BadKeyError(
                              "%s: %s must not start with '$'" % (name, key))
                    elif type(key) is type:
                        if not key in authorized_types:
                            raise AuthorizedTypeError(
                              "%s: %s is not an authorized type" % (name, key))
                    else:
                        raise StructureError(
                          "%s: %s must be a basestring or a type" % (name, key))
                    if struct[key] is None:
                        pass
                    elif isinstance(struct[key], dict):
                        __validate_skeleton(struct[key], name, authorized_types)
                    elif isinstance(struct[key], list):
                        __validate_skeleton(struct[key], name, authorized_types)
                    elif isinstance(struct[key], tuple):
                        __validate_skeleton(struct[key], name, authorized_types)
                    elif isinstance(struct[key], SchemaProperties):
                        pass
                    elif hasattr(struct[key], 'skeleton'):
                        __validate_skeleton(struct[key], name, authorized_types)
                    elif (struct[key] not in authorized_types):
                        ok = False
                        for auth_type in authorized_types:
                            if struct[key] is None:
                                ok = True
                            else:
                                try:
                                    if isinstance(struct[key], auth_type) or issubclass(struct[key], auth_type):
                                        ok = True
                                except TypeError:
                                    raise TypeError("%s: %s is not a type" % (name, struct[key]))
                        if not ok:
                            raise StructureError(
                              "%s: %s is not an authorized type" % (name, struct[key]))
            elif isinstance(struct, list) or isinstance(struct, tuple):
                for item in struct:
                    __validate_skeleton(item, name, authorized_types)
            elif isinstance(struct, SchemaProperties):
                pass
            else:
                ok = False
                for auth_type in authorized_types:
                    if isinstance(struct, auth_type):
                        ok = True
                if not ok:
                    raise StructureError(
                      "%s: %s is not an authorized_types" % (name, struct))
        #################
        if skeleton is None:
            raise StructureError(
              "%s.skeleton must not be None" % name)
        if not isinstance(skeleton, dict):
            raise StructureError(
              "%s.skeleton must be a dict instance" % name)
        __validate_skeleton(skeleton, name, authorized_types)

    def _set_default_fields(self, doc, struct, path = ""):
        # TODO check this out, this method must be restructured
        for key in struct:
            new_key = key
            new_path = ".".join([path, new_key]).strip('.')
            #
            # default_values :
            # if the value is None, check if a default value exist.
            # if exists, and it is a function then call it otherwise,
            # juste feed it
            #
            if type(key) is not type:
                if doc[key] is None and new_path in self.default_values:
                    new_value = self.default_values[new_path]
                    if callable(new_value):
                        new_value = new_value()
                    elif isinstance(new_value, dict):
                        new_value = deepcopy(new_value)
                    elif isinstance(new_value, list):
                        new_value = new_value[:]
                    doc[key] = new_value
            #
            # if the value is a dict, we have a another skeleton to validate
            #
            if isinstance(struct[key], dict):
                #
                # if the dict is still empty into the document we build
                # it with None values
                #
                if len(struct[key]) and\
                  not [i for i in struct[key].keys() if type(i) is type]:
                    self._set_default_fields(doc[key], struct[key], new_path)
                else:
                    if new_path in self.default_values:
                        new_value = self.default_values[new_path]
                        if callable(new_value):
                            new_value = new_value()
                        elif isinstance(new_value, dict):
                            new_value = deepcopy(new_value)
                        elif isinstance(new_value, list):
                            new_value = new_value[:]
                        doc[key] = new_value
            elif isinstance(struct[key], list):
                if new_path in self.default_values:
                    for new_value in self.default_values[new_path]:
                        if callable(new_value):
                            new_value = new_value()
                        elif isinstance(new_value, dict):
                            new_value = deepcopy(new_value)
                        elif isinstance(new_value, list):
                            new_value = new_value[:]
                        doc[key].append(new_value)  
            else: # what else
                if new_path in self.default_values:
                    new_value = self.default_values[new_path]
                    if callable(new_value):
                        new_value = new_value()
                    elif isinstance(new_value, dict):
                        new_value = deepcopy(new_value)
                    elif isinstance(new_value, list):
                        new_value = new_value[:]
                    doc[key] = new_value

    def __generate_skeleton(self, doc, struct, path = ""):
        for key in struct:
            if type(key) is type:
                new_key = "$%s" % key.__name__
            else:
                new_key = key
            new_path = ".".join([path, new_key]).strip('.')
            #
            # Automatique generate the skeleton with NoneType
            #
            if type(key) is not type and key not in doc:
                if isinstance(struct[key], dict):
                    if callable(struct[key]):
                        doc[key] = struct[key]()
                    else:
                        doc[key] = type(struct[key])()
                elif struct[key] is dict:
                    doc[key] = {}
                elif isinstance(struct[key], list):
                    doc[key] = type(struct[key])()
                elif struct[key] is list:
                    doc[key] = []
                elif isinstance(struct[key], tuple):
                    doc[key] = [None for i in range(len(struct[key]))]
                else:
                    doc[key] = None
            #
            # if the value is a dict, we have a another skeleton to validate
            #
            if isinstance(struct[key], dict) and type(key) is not type:
                self.__generate_skeleton(doc[key], struct[key], new_path)
