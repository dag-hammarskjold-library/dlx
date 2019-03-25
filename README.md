# dlx
Provides a Python API to the DLX database

### Usage:
```python
#/usr/bin/env python

from dlx.db import DB
from dlx.query import match_value
from dlx.jmarc import JMARC

db = DB()
query = match_value('269','a','2005-01-05')
cursor = db.bibs.find(query)

for doc in cursor:
    jmarc = JMARC(doc)
    print('symbol: ' + jmarc.get_value('191','a'))
```
