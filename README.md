# dlx
Provides a Python API to the DLX database

### Installation:
```bash
pip install git+https://github.com/dag-hammarskjold-library/dlx
```

### Usage:
```python
#/usr/bin/env python

import dlx

# connect to DB
dlx.DB.connect('valid Mongo connection string')

# get JMARC record by ID
jmarc = dlx.JBIB.match_id(6020)

# get cursor for iterating through matching JMARC records

# match one value
cursor = dlx.JBIB.match_value('269','a','2012-12-31')

# match multiple values from different fields
cursor = dlx.JBIB.match_values(('269','a','2012-12-31'), ('245','a',re.compile('report',re.IGNORECASE)))

# match multiple subfield values from same field
cursor = dlx.JBIB.match_field('245', ('a','Copyright law survey /'), ('c','World Intellectual Property Organization.'))

# iterate
for jmarc in cursor:

    print('title: ' + ' '.join(jmarc.get_values('245','a','b','c')))
    print('date: ' + jmarc.get_value('269',a'))
    print('authors: ' + '; '.join(get_values('710','a')))
    print('subjects: ' + '; '.join(jmarc.get_values('650','a')))
		
    print('-' * 100)
    
```
