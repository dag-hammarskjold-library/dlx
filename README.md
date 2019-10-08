
# dlx
Provides a Python API to the DLX database

### Installation:
```bash
> pip install git+git://github.com/dag-hammarskjold-library/dlx
```

### Usage:

Connect to the database using a [MongoDB connection string](https://docs.mongodb.com/manual/reference/connection-string/).

```python
#/usr/bin/env python

from dlx import DB, Bib, Auth
from bson.regex import Regex

# connect to DB
DB.connect('connection string')

# get an instance of of a subclass of `dlx.MARC` from the database (`Bib` or `Auth`)
bib = Bib.match_id(99999)
auth = Auth.match_id(283289)

# use `dlx.marc.record.Bib.match()` and `dlx.marc.record.Auth.match()` with a series of `dlx.marc.record.Matcher` obejcts to write queries.
from dlx.marc.record import Matcher
bibs = Bib.match(
    Matcher('269',('a','2012-12-31')),
    Matcher('245',('a',Regex('^Report')))
)

auths = Auth.match(
    Matcher('100',('a',Regex('Dag'))),
    Matcher('400',('a',Regex('Carl')))
)

# `match()` returns a generator for iterating through matching records.
# the generator yeilds instances of `Bib` or `Auth`.

# iterate through the matching records
for bib in bibs:

    # `bib` is a `Bib` object
    print('title: ' + ' '.join(bib.get_values('245','a','b','c')))
    print('date: ' + bib.get_value('269','a'))
    print('authors: ' + '; '.join(bib.get_values('710','a')))
    print('subjects: ' + '; '.join(bib.get_values('650','a')))
        
    print('-' * 100)
    
```
