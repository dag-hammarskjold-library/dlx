"""
Parameters:
[0] connection string
[1] body (as it appears in the data, i.e. 'S/')
[2] session

Usage:
> pip install git+https://github.com/dag-hammarskjold-library/dlx
> python itp.py <connection string> S/ 70

Output:
Writes bib and auth records to 'bibs.mrc' and 'auths.mrc' in the current working directory.
"""

import sys, codecs
from dlx import DB, Bib, Auth

DB.connect(sys.argv[1])
body = sys.argv[2]
session = sys.argv[3]

"""
Extract MARC records for ITP session
"""

#find the bibs for the session, print them in marc21, and save any found xrefs.

bibfile = codecs.open('bibs.mrc','w','utf-8')
authfile = codecs.open('auths.mrc','w','utf-8')
auth_ids = {}

cursor = Bib.match_fields_or (
	('191', ('b', body), ('c', session)),
	('791', ('b', body), ('c', session))
)

print("processing bibs...")

for bib in cursor:
	for xref in bib.get_xrefs():
		auth_ids[xref] = True
		
	bibfile.write(bib.to_mrc())
	
### get the auths

print("processing auths...")
		
for auth_id in sorted(auth_ids.keys()):
	auth = Auth.match_id(auth_id)
	
	authfile.write(auth.to_mrc())
	
print("done.")