
import os
import json

class Configs(object):
	
	# schemas
	schema_dir = os.path.dirname(__file__) + '/cdschemas/'
	with open(schema_dir + 'jmarc.schema.json') as x: jmarc_schema = json.loads(x.read())
	#with open(schema_dir + '/jfile.schema.json') as x: jfile_schema = json.loads(x.read())

	# this is used by dlx.query to locate the linked value
	authority_controlled = {
		'191': {
			'b' : '190',
			'c' : '190'
		},
		'600': {
			'a' : '100'
		},
		'610': {
			'a' : '110'
		},
		'650': {
			'a' : '150'
		},
		'651': {
			'a' : '151'
		},
		'700': {
			'a': '100'
		},
		'710': {
			'a' : '110'
		},
		'991': {
			'b' : '191',
			'c' : '191',
			'd' : '191'
		}	
	}
	
	