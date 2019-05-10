
# dlx
Provides a Python API to the DLX database

### Installation:
```bash
> pip install git+https://github.com/dag-hammarskjold-library/dlx
```

### Usage:

Connect to the database using a [MongoDB connection string](https://docs.mongodb.com/manual/reference/connection-string/).

Note: The class names `dlx.JMARC`, `dlx.JBIB`, and `dlx.JAUTH` have been deprecated. They still work , but have been replaced by `dlx.MARC`, `dlx.Bib`, and `dlx.Auth`. This is to avoid confusion with JMARC, the JSON specification. The old classes are now subclasses of their new counterparts and will be eliminated eventually.

```python
#/usr/bin/env python

import re
from dlx import DB, Bib, Auth

# connect to DB
DB.connect('connection string')

# get an instance of of a subclass of `dlx.MARC` from the database (`Bib` or `Auth`)
bib = Bib.match_id(99999)
auth = Auth.match_id(283289)

# get a generator for iterating through matching records.
# the generator yeilds instnaces of `dlx.Bib` or `dlx.Auth`.

# match one value
bibs = Bib.match_value('269','a','2012-12-31')
auths = Auth.match_value('100','a',re.compile('Dag'))

# match multiple values from different fields
bibs = Bib.match_values(
    ('269','a','2012-12-31'), 
    ('245','a',re.compile('report',re.IGNORECASE))
)

# match multiple values using boolean `or`
bibs = Bib.match_values_or(
    ('269','a','2012-12-31'),
    ('269','a','2013-01-02')
)

# match multiple subfield values within the same field
bibs = Bib.match_field(
    '245', 
    ('a','Copyright law survey /'), 
    ('c','World Intellectual Property Organization.')
)

# match multiple fields using subfield values within the same field 
bibs = Bib.match_fields (
    (
        '245', 
	('a','Copyright law survey /'), 
	('c','World Intellectual Property Organization.')
    ),
    (
        '260', 
	('a', re.compile('Geneva'))
    )
)

# match multiple fields using subfield values within the same field using boolean `or`
bis = Bib.match_fields_or (
    (
        '191', 
	 ('a', re.compile('^A/RES')), 
	 ('c', '73')
    ),
    (
        '650',
	('a', 'HUMAN RIGHTS')
    )
)

# iterate
for bib in bibs:

    # `bib` is a `Bib` object
    print('title: ' + ' '.join(bib.get_values('245','a','b','c')))
    print('date: ' + bib.get_value('269','a'))
    print('authors: ' + '; '.join(bib.get_values('710','a')))
    print('subjects: ' + '; '.join(bib.get_values('650','a')))
		
    print('-' * 100)
    
```
