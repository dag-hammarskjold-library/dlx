from datetime import datetime
from xlrd import open_workbook, xldate

class Table(object):
    @classmethod
    def from_excel(cls,path,sheet_number=0):
        wkbook = open_workbook(path)
        sheet = wkbook.sheet_by_index(sheet_number)
        
        def clean(cell):
            if cell.ctype == 3:
                parts = xldate.xldate_as_tuple(cell.value,0)[0:3]
                dt = datetime(*parts)
                val = dt.strftime('%Y-%m-%d')
                return val
            elif cell.ctype == 2:
                return(int(cell.value))
            else:
                return cell.value
        
        lol = []        
        for row in sheet.get_rows():
            cells = []
            for cell in row:              
                cells.append(clean(cell))
            lol.append(cells)
            
        return cls(lol)
        
    def __init__(self,list_of_lists=None,**kwargs):
        self.index = {}
        self.header = []
        
        if list_of_lists:
            self.header = list_of_lists[0]
            
            rowx = 0
            for row in list_of_lists[1:len(list_of_lists)]:
                self.index[rowx] = {}
                
                cellx = 0
                for cell in row:
                    field_name = self.header[cellx]
                    self.index[rowx][field_name] = cell
                    cellx += 1
                
                rowx += 1
                
    def set(self,rowx,field_name,value):
        self.index[rowx][field_name] = value
        
        return self 
        
    def get(self,rowx,field_name):
        return self.index[rowx][field_name]
            
    def to_list(self):
        output = []
        output.append(self.header)
        
        for temp_id in self.index.keys():
            row = []

            for field_name in self.index[temp_id].keys():
                row.append(self.index[temp_id][field_name])
            
            output.append(row)
            
        return output
        
    def to_html(self,**kwargs):
        rows = []
        
        for row in self.to_list():
            tds = ['<td>{}</td>'.format(val) for val in row]
            to_str = ''.join(tds)
            rows.append(to_str)
        
        trs = ['<tr>{}</tr>'.format(row) for row in rows]
        to_str = ''.join(trs)
        
        table = '<table>{}</table>'.format(to_str)
        
        return table
        
        