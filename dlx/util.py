from datetime import datetime
from xlrd import open_workbook
from xlrd.xldate import xldate_as_tuple

class Table():
    @classmethod
    def from_excel(cls, path, sheet_number=0, date_format='%Y%m%d'):
        wkbook = open_workbook(path)
        sheet = wkbook.sheet_by_index(sheet_number)

        def clean(cell):
            if cell.ctype == 3:
                # todo: get feedback on date formatting
                parts = xldate_as_tuple(cell.value,0)[0:3]
                dt = datetime(*parts)
                val = dt.strftime(date_format)
                return val
            elif cell.ctype == 2:
                return(int(cell.value))
            else:
                return cell.value
        
        lol = []        
        for row in sheet.get_rows():
            cells = []
            for cell in row:
                cells.append(str(clean(cell)).rstrip())
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
        
class ISO6391():
    codes = {	
    	"aa": "Afar",
    	"ab": "Abkhazian",
    	"ae": "Avestan",
    	"af": "Afrikaans",
    	"ak": "Akan",
    	"am": "Amharic",
    	"an": "Aragonese",
    	"ar": "Arabic",
    	"as": "Assamese",
    	"av": "Avaric",
    	"ay": "Aymara",
    	"az": "Azerbaijani",
    	"ba": "Bashkir",
    	"be": "Belarusian",
    	"bg": "Bulgarian",
    	"bh": "Bihari languages",
    	"bi": "Bislama",
    	"bm": "Bambara",
    	"bn": "Bengali",
    	"bo": "Tibetan",
    	"br": "Breton",
    	"bs": "Bosnian",
    	"ca": "Catalan; Valencian",
    	"ce": "Chechen",
    	"ch": "Chamorro",
    	"co": "Corsican",
    	"cr": "Cree",
    	"cs": "Czech",
    	"cu": "Church Slavic; Old Slavonic; Church Slavonic; Old Bulgarian; Old Church Slavonic",
    	"cv": "Chuvash",
    	"cy": "Welsh",
    	"da": "Danish",
    	"de": "German",
    	"dv": "Divehi; Dhivehi; Maldivian",
    	"dz": "Dzongkha",
    	"ee": "Ewe",
    	"el": "Greek:  Modern (1453-)",
    	"en": "English",
    	"eo": "Esperanto",
    	"es": "Spanish; Castilian",
    	"et": "Estonian",
    	"eu": "Basque",
    	"fa": "Persian",
    	"ff": "Fulah",
    	"fi": "Finnish",
    	"fj": "Fijian",
    	"fo": "Faroese",
    	"fr": "French",
    	"fy": "Western Frisian",
    	"ga": "Irish",
    	"gd": "Gaelic; Scottish Gaelic",
    	"gl": "Galician",
    	"gn": "Guarani",
    	"gu": "Gujarati",
    	"gv": "Manx",
    	"ha": "Hausa",
    	"he": "Hebrew",
    	"hi": "Hindi",
    	"ho": "Hiri Motu",
    	"hr": "Croatian",
    	"ht": "Haitian; Haitian Creole",
    	"hu": "Hungarian",
    	"hy": "Armenian",
    	"hz": "Herero",
    	"ia": "Interlingua (International Auxiliary Language Association)",
    	"id": "Indonesian",
    	"ie": "Interlingue; Occidental",
    	"ig": "Igbo",
    	"ii": "Sichuan Yi; Nuosu",
    	"ik": "Inupiaq",
    	"io": "Ido",
    	"is": "Icelandic",
    	"it": "Italian",
    	"iu": "Inuktitut",
    	"ja": "Japanese",
    	"jv": "Javanese",
    	"ka": "Georgian",
    	"kg": "Kongo",
    	"ki": "Kikuyu; Gikuyu",
    	"kj": "Kuanyama; Kwanyama",
    	"kk": "Kazakh",
    	"kl": "Kalaallisut; Greenlandic",
    	"km": "Central Khmer",
    	"kn": "Kannada",
    	"ko": "Korean",
    	"kr": "Kanuri",
    	"ks": "Kashmiri",
    	"ku": "Kurdish",
    	"kv": "Komi",
    	"kw": "Cornish",
    	"ky": "Kirghiz; Kyrgyz",
    	"la": "Latin",
    	"lb": "Luxembourgish; Letzeburgesch",
    	"lg": "Ganda",
    	"li": "Limburgan; Limburger; Limburgish",
    	"ln": "Lingala",
    	"lo": "Lao",
    	"lt": "Lithuanian",
    	"lu": "Luba-Katanga",
    	"lv": "Latvian",
    	"mg": "Malagasy",
    	"mh": "Marshallese",
    	"mi": "Maori",
    	"mk": "Macedonian",
    	"ml": "Malayalam",
    	"mn": "Mongolian",
    	"mr": "Marathi",
    	"ms": "Malay",
    	"mt": "Maltese",
    	"my": "Burmese",
    	"na": "Nauru",
    	"nb": "Bokmål:  Norwegian; Norwegian Bokmål",
    	"nd": "Ndebele:  North; North Ndebele",
    	"ne": "Nepali",
    	"ng": "Ndonga",
    	"nl": "Dutch; Flemish",
    	"nn": "Norwegian Nynorsk; Nynorsk:  Norwegian",
    	"no": "Norwegian",
    	"nr": "Ndebele:  South; South Ndebele",
    	"nv": "Navajo; Navaho",
    	"ny": "Chichewa; Chewa; Nyanja",
    	"oc": "Occitan (post 1500); Provençal",
    	"oj": "Ojibwa",
    	"om": "Oromo",
    	"or": "Oriya",
    	"os": "Ossetian; Ossetic",
    	"pa": "Panjabi; Punjabi",
    	"pi": "Pali",
    	"pl": "Polish",
    	"ps": "Pushto; Pashto",
    	"pt": "Portuguese",
    	"qu": "Quechua",
    	"rm": "Romansh",
    	"rn": "Rundi",
    	"ro": "Romanian; Moldavian; Moldovan",
    	"ru": "Russian",
    	"rw": "Kinyarwanda",
    	"sa": "Sanskrit",
    	"sc": "Sardinian",
    	"sd": "Sindhi",
    	"se": "Northern Sami",
    	"sg": "Sango",
    	"si": "Sinhala; Sinhalese",
    	"sk": "Slovak",
    	"sl": "Slovenian",
    	"sm": "Samoan",
    	"sn": "Shona",
    	"so": "Somali",
    	"sq": "Albanian",
    	"sr": "Serbian",
    	"ss": "Swati",
    	"st": "Sotho:  Southern",
    	"su": "Sundanese",
    	"sv": "Swedish",
    	"sw": "Swahili",
    	"ta": "Tamil",
    	"te": "Telugu",
    	"tg": "Tajik",
    	"th": "Thai",
    	"ti": "Tigrinya",
    	"tk": "Turkmen",
    	"tl": "Tagalog",
    	"tn": "Tswana",
    	"to": "Tonga (Tonga Islands)",
    	"tr": "Turkish",
    	"ts": "Tsonga",
    	"tt": "Tatar",
    	"tw": "Twi",
    	"ty": "Tahitian",
    	"ug": "Uighur; Uyghur",
    	"uk": "Ukrainian",
    	"ur": "Urdu",
    	"uz": "Uzbek",
    	"ve": "Venda",
    	"vi": "Vietnamese",
    	"vo": "Volapük",
    	"wa": "Walloon",
    	"wo": "Wolof",
    	"xh": "Xhosa",
    	"yi": "Yiddish",
    	"yo": "Yoruba",
    	"za": "Zhuang; Chuang",
    	"zh": "Chinese",
    	"zu": "Zulu",
    }
    
    @classmethod
    def language_by_code(cls, code):
        return cls.codes[code]
        