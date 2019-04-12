
# dlx
Provides a Python API to the DLX database

### Installation:
```bash
pip install git+https://github.com/dag-hammarskjold-library/dlx
```

### Usage:

Connect to the database using a [MongoDB connection string](https://docs.mongodb.com/manual/reference/connection-string/).

Note: The class names `dlx.JMARC`, `dlx.JBIB`, and `dlx.JAUTH` have been deprecated. They still work , but have been replaced by `dlx.MARC`, `dlx.Bib`, and `dlx.Auth`. This is to avoid confusion with JMARC, the JSON specification. The old classes are now subclasses of their new counterparts and will be eliminated eventually.

```python
#/usr/bin/env python

import re
import dlx

# connect to DB
dlx.DB.connect('connection string')

# get record by ID
marc = dlx.JBIB.match_id(6020) # deprecated
marc = dlx.Bib.match_id(6020)

# get cursor for iterating through matching JMARC records

# match one value
cursor = dlx.Bib.match_value('269','a','2012-12-31')

# match multiple values from different fields
cursor = dlx.Bib.match_values(('269','a','2012-12-31'), ('245','a',re.compile('report',re.IGNORECASE)))

# match multiple values using boolean `or`
cursor = dlx.Bib.match_values_or(('269','a','2012-12-31'), ('269','a','2013-01-02'))

# match multiple subfield values from same field
cursor = dlx.Bib.match_field('245', ('a','Copyright law survey /'), ('c','World Intellectual Property Organization.'))

# iterate
for jmarc in cursor:

    # `jmarc` is a `dlx.Bib` object
    print('title: ' + ' '.join(jmarc.get_values('245','a','b','c')))
    print('date: ' + jmarc.get_value('269','a'))
    print('authors: ' + '; '.join(jmarc.get_values('710','a')))
    print('subjects: ' + '; '.join(jmarc.get_values('650','a')))
		
    print('-' * 100)
    
```
