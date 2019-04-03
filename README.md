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

# iterate through bibs
for jmarc in dlx.JBIB.match_value(tag,code,val):

    print('symbols: ' + '; '.join(jmarc.symbols()))
    print('title: ' + jmarc.title())
    print('date: ' + jmarc.get_value('269','a'))
    print('subjects: ' + '; '.join(jmarc.get_values('650','a')))
		
    for lang in ('EN','FR'):
        print(lang + ': ' + jmarc.file(lang))
		
    print('-' * 100)
    
```
