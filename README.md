

# DLX
APIs for performing ETL operations on MARC and file data

### Requirements:
* Python 3.9+
* Valid MongoDB connection string
* AWS S3 credentials (file submission only)

### Installation:
```bash
> pip install git+git://github.com/dag-hammarskjold-library/dlx@<latest release>
```
### Usage:
```python
from dlx import DB

DB.connect('<connection string>')

### MARC

from dlx.marc import Bib, BibSet, Query, Condition

# get a record from the database as a `Bib` object
bib = Bib.from_id(10000)
# get values from the record
print(bib.get_value('245', 'a'))

# contstruct a query using search syntax
query = Query.from_string("245__a:'Title' AND 245__b:'subtitle'")

# construct a more specific query using a `Condition`
query = Query(
	Condition('245', {'a': 'Title', 'b': 'subtitle'})
)

# get the records that match the query as `Bib` objects
for bib in BibSet.from_query(query):
	# edit the records
	bib.set('245', 'a', 'Edited title')
	# save the records back to the database
	bib.commit(user='myusername')

### File

from dlx.file import File, Identifier

# import a file from the local disk
File.import_from_path(
	'c:\\files\file.pdf', 
	identifiers=[Identifier('isbn', '9781234567890')], 
	filename='fn.txt', languages=['EN'], 
	mimetype='text/plain', 
	source='demo'
)

# import a file from a url
File.import_from_path(
	'www.someurl.com/file.pdf', 
	identifiers=[Identifier('isbn', '9781234567890')], 
	filename='fn.txt', languages=['EN'], 
	mimetype='text/plain', 
	source='demo'
)

# retrieve the file record as a `File` object
f = File.latest_by_identifier_language(Identifier('isbn', '9781234567890'), 'EN')
# get the file's new s3 link
print(f.url)
```
