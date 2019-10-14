

# DLX
DLX is a serialization, storage, and query toolkit for MARC data and associated content files. DLX provies APIs for manipulating, storing, and retrieving MARC data ~~as well as content file submission and retrieval (not yet implemented)~~.

### Installation:
```bash
> pip install git+git://github.com/dag-hammarskjold-library/dlx
```
### Usage:
DLX provides various classes for working with the DLX data.

```python
#/usr/bin/env python

from dlx import DB
from dlx.marc import Bib, Auth, Matcher, OrMatch
from bson.regex import Regex
```
Connect to the database using a [MongoDB connection string](https://docs.mongodb.com/manual/reference/connection-string/).

```python
DB.connect('connection string')
```

 `Bib` and `Auth` have class methods for accessing the
 `db.bibs` and `db.auths` database collections.
 
```python
bib = Bib.match_id(99999) # returns a Bib() object
auth = Auth.match_id(283289) # returns an Auth() object
```

Use the class method `.match()` with a series of `Matcher`
objects to write queries.

```python
bibs = Bib.match(
    Matcher('269',('a','2012-12-31')),
    Matcher('245',('a',Regex('^Report')))
)

auths = Auth.match(
    Matcher('100',('a',Regex('Dag'))),
    Matcher('400',('a',Regex('Carl')))
)
```

Use `OrMatch` to group matcher objects into OR queries.

```python
bibs = Bib.match(
    OrMatch(
        Matcher('191',('b','A/'),('c','72')),
        Matcher('791',('b','A/'),('c','72'))
    )
)
```

`.match()` returns a generator for iterating through
matching records. The generator yeilds instances of `Bib()`
or `Auth()`, which have instance methods for getting values 
from the instance such as `.get_value()`.

```python
for bib in bibs:
    # The `Bib` and `Auth` objects
    print('date: ' + bib.get_value('269','a'))
    print('title: ' + ' '.join(bib.get_values('245','a','b','c')))
    print('-' * 100) 
```
