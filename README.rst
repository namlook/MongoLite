========
MongoLite
========

MongoLite is a fork of MongoKit_ by the same author. It aims to come back to simplicity by stripping a lot of features.

The goal of MongoLite is to stick as much as possible to the pymongo api.
MongoLite always choose speed over syntaxic sugar, this is why you won't see
validation or dot notation features in this project.

.. _MongoKit : http://namlook.github.com/mongokit/

MongoLite is perfect for who wants have a thing layer on top of pymongo and don't care about validation stuff.

Most of MongoKit's features can be recreated by using plugins and custom types.

A mongolite is a beautiful stone_

.. _stone : http://www.mindat.org/photos/0656330001207867080.jpg

.. _MongoKit : http://namlook.github.com/mongokit/

.. topic:: **Your data is clean**:

    "Tools change, not data". In order to follow this "credo", just like
    MongoKit, MongoLite won't add any information into your data saved into the
    database.  So if you need to use other mongo tools or ODMs in other languages,
    your data won't be polluted by MongoKit's stuff.

Features
========

 * schema less feature
 * inheritance and polymorphisme support
 * skeleton generation (your object is automaticaly filled by the correct fields)
 * nested and complex schema declaration
 * default values features
 * random query support (which returns a random document from the database)
 * json helpers
 * GridFS support

Go to the full documentation_ .

.. _documentation : http://namlook.github.com/mongolite/

A quick example
===============

Document are enhanced python dictionary with a ``validate()`` method.
A Document declaration look like that::

    >>> from mongokit import *
    >>> import datetime

    >>> connection = Connection()
    
    >>> @connection.register
    ... class BlogPost(Document):
    ...     structure = {
    ...             'title':unicode,
    ...             'body':unicode,
    ...             'author':unicode,
    ...             'date_creation':datetime.datetime,
    ...             'rank':int
    ...     }
    ...     required_fields = ['title','author', 'date_creation']
    ...     default_values = {'rank':0, 'date_creation':datetime.datetime.utcnow}
    ... 

We fire a connection and register our objects.

    >>> blogpost = con.test.example.BlogPost() # this use the db "test" and the collection "example"
    >>> blogpost['title'] = u'my title'
    >>> blogpost['body'] = u'a body'
    >>> blogpost['author'] = u'me'
    >>> blogpost
    {'body': u'a body', 'title': u'my title', 'date_creation': datetime.datetime(...), 'rank': 0, 'author': u'me'}
    >>> blogpost.save()
   
Saving the object will call the `validate()` method.

And you can use more complex structure::

    >>>  @connection.register
    ...  class ComplexDoc(Document):
    ...     __database__ = 'test'
    ...     __collection__ = 'example'
    ...     structure = {
    ...         "foo" : {"content":int},
    ...         "bar" : {
    ...             int:{unicode:int}
    ...         }
    ...     }
    ...     required_fields = ['foo.content', 'bar.$int']
     
Please, see the tutorial_ for more examples.

.. _tutorial : http://namlook.github.com/mongokit/tutorial.html

Suggestion and patches are really welcome. If you find mistakes in the documentation
(english is not my primary langage) feel free to contact me. You can find me (namlook) 
on the freenode #mongodb irc channel or on twitter_.

.. _twitter : http://twitter.com/namlook


Recent Change Log
=================

v0.1
----

 * fork from MongoKit, strip all unwanted features
