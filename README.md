# DLX
APIs for performing ETL operations on MARC and file data

### Requirements:
* Python 3.6+
* Valid MongoDB connection string
* AWS S3 credentials (file submission only)

### Installation:
```bash
> pip install git+git://github.com/dag-hammarskjold-library/dlx
```
### Usage:
```python
from dlx import DB

DB.connect('<connection string>')

### MARC

from dlx.marc import Bib, BibSet, Query, Condition

bib = Bib.from_id(100)
print(bib.get_value('245', 'a'))

query = Query(
	Condition('245', {'a': 'Title', 'b': 'subtitle'})
)

for bib in BibSet.from_query(query):
	bib.set('245', 'a', 'New title')
	bib.commit(user='<username>')

### File

from dlx.file import File, Identifier, S3

S3.connect(bucket='<bucket name>')

fileobj = open('file.txt', 'r')

File.import_from_handle(
	fileobj,
	identifiers=[Identifier('isbn', '1')], 
	filename='fn.txt', languages=['EN'],
	mimetype='text/plain',
	source='demo'
)

f = File.latest_by_identifier_language(Identifier('isbn', '1'), 'EN')
print(f.url)
```
