
class NotConnected(Exception):
	def __init__(self):
		Exception.__init__(self,'Not connected to database')
