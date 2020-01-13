

# DLX
DLX is a serialization, storage, and query toolkit for MARC data and associated content files. DLX provies APIs for manipulating, storing, and retrieving MARC data ~~as well as content file submission and retrieval (not yet implemented)~~.

### Installation:
```bash
> pip install git+git://github.com/dag-hammarskjold-library/dlx
```
### Usage:
DLX provides various classes for working with the DLX data.

```python
from bson import Regex
from dlx import DB
from dlx.marc import BibSet, QueryDocument, Condition

DB.connect(<connection_string>)

query = QueryDocument(
    Condition(
        tag='191',
        modifier='exists'
    ),
    Condition(
        tag='269',
        subfields={'a': Regex('^1975')}
    )
)

print(query.to_json())

bibset = BibSet.from_query(query, projection={'191': True}, skip=0, limit=0)
print('There are {} results'.format(bibset.count))

bibset.cache()

for bib in bibset.records:
    print('id: {}, symbol: {}'.format(bib.id, bib.get_value('191','a')))

print(bibset.to_xml())

```
