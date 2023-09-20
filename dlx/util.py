import re
from datetime import datetime
from xlrd import open_workbook
from xlrd.xldate import xldate_as_tuple

def isint(x):
    try:
        assert x == int(x)
    except:
        return False
        
    return True

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
            tds = [f'<td>{val}</td>' for val in row]
            to_str = ''.join(tds)
            rows.append(to_str)
        
        trs = [f'<tr>{row}</tr>' for row in rows]
        to_str = ''.join(trs)
        
        table = f'<table>{to_str}</table>'
        
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

class AsciiMap:
    data = {
        ### A
        'Å': 'A',
        'å': 'A',
        'Ǻ': 'A',
        'ǻ': 'A',
        'Ḁ': 'A',
        'ḁ': 'A',
        'ẚ': 'A',
        'Ă': 'A',
        'ă': 'A',
        'Ặ': 'A',
        'ặ': 'A',
        'Ắ': 'A',
        'ắ': 'A',
        'Ằ': 'A',
        'ằ': 'A',
        'Ẳ': 'A',
        'ẳ': 'A',
        'Ẵ': 'A',
        'ẵ': 'A',
        'Ȃ': 'A',
        'ȃ': 'A',
        'Â': 'A',
        'â': 'A',
        'Ậ': 'A',
        'ậ': 'A',
        'Ấ': 'A',
        'ấ': 'A',
        'Ầ': 'A',
        'ầ': 'A',
        'Ẫ': 'A',
        'ẫ': 'A',
        'Ẩ': 'A',
        'ẩ': 'A',
        'Ả': 'A',
        'ả': 'A',
        'Ǎ': 'A',
        'ǎ': 'A',
        'Ⱥ': 'A',
        'ⱥ': 'A',
        'Ȧ': 'A',
        'ȧ': 'A',
        'Ǡ': 'A',
        'ǡ': 'A',
        'Ạ': 'A',
        'ạ': 'A',
        'Ä': 'A',
        'ä': 'A',
        'Ǟ': 'A',
        'ǟ': 'A',
        'À': 'A',
        'à': 'A',
        'Ȁ': 'A',
        'ȁ': 'A',
        'Á': 'A',
        'á': 'A',
        'Ā': 'A',
        'ā': 'A',
        'Ā̀': 'A', # MULTIBYTE
        'ā̀': 'A', # MULTIBYTE
        'Ã': 'A',
        'ã': 'A',
        'Ą': 'A',
        'ą': 'A',
        'Ą́': 'A', # MULTIBYTE
        'ą́': 'A', # MULTIBYTE
        'Ą̃': 'A', # MULTIBYTE
        'ą̃': 'A', # MULTIBYTE
        'A̲': 'A', # MULTIBYTE
        'a̲': 'A', # MULTIBYTE
        'ᶏ': 'A',
        'Å': 'A', # angstrom
        ### B
        'Ƀ': 'B',
        'ƀ': 'B',
        'Ḃ': 'B',
        'ḃ': 'B',
        'Ḅ': 'B',
        'ḅ': 'B',
        'Ḇ': 'B',
        'ḇ': 'B',
        'Ɓ': 'B',
        'ɓ': 'B',
        'ᵬ': 'B',
        'ᶀ': 'B',
        'Β': 'Β',  # Beta
        ### C
        'Ć': 'C',
        'ć': 'C',
        'Ĉ': 'C',
        'ĉ': 'C',
        'Č': 'C',
        'č': 'C',
        'Ċ': 'C',
        'ċ': 'C',
        'Ḉ': 'C',
        'ḉ': 'C',
        'Ƈ': 'C',
        'ƈ': 'C',
        'C̈': 'C', # MULTIBYTE
        'c̈': 'C', # MULTIBYTE
        'Ȼ': 'C',
        'ȼ': 'C',
        'Ç': 'C',
        'ç': 'C',
        'Ꞓ': 'C',
        'ꞓ': 'C',
        ### D
        'Đ': 'D',
        'đ': 'D',
        'Ɗ': 'D',
        'ɗ': 'D',
        'Ḋ': 'D',
        'ḋ': 'D',
        'Ḍ': 'D',
        'ḍ': 'D',
        'Ḑ': 'D',
        'ḑ': 'D',
        'Ḓ': 'D',
        'ḓ': 'D',
        'Ď': 'D',
        'ď': 'D',
        'Ḏ': 'D',
        'ḏ': 'D',
        'Ɖ': 'D', # African D / Eth
        'ɖ' : 'D', # African D
        'ð' : 'D', # lowercase Eth 
        ### E
        'Ĕ': 'E',
        'ĕ': 'E',
        'Ḝ': 'E',
        'ḝ': 'E',
        'Ȇ': 'E',
        'ȇ': 'E',
        'Ê': 'E',
        'ê': 'E',
        'Ê̄': 'E', # MULTIBYTE
        'ê̄': 'E', # MULTIBYTE
        'Ê̌': 'E', # MULTIBYTE
        'ê̌': 'E', # MULTIBYTE
        'Ề': 'E',
        'ề': 'E',
        'Ế': 'E',
        'ế': 'E',
        'Ể': 'E',
        'ể': 'E',
        'Ễ': 'E',
        'ễ': 'E',
        'Ệ': 'E',
        'ệ': 'E',
        'Ẻ': 'E',
        'ẻ': 'E',
        'Ḙ': 'E',
        'ḙ': 'E',
        'Ě': 'E',
        'ě': 'E',
        'Ɇ': 'E',
        'ɇ': 'E',
        'Ė': 'E',
        'ė': 'E',
        'Ė́': 'E', # MULTIBYTE
        'ė́': 'E', # MULTIBYTE
        'Ė̃': 'E', # MULTIBYTE
        'ė̃': 'E', # MULTIBYTE
        'Ẹ': 'E',
        'ẹ': 'E',
        'Ë': 'E',
        'ë': 'E',
        'È': 'E',
        'è': 'E',
        'È̩': 'E', # MULTIBYTE
        'è̩': 'E', # MULTIBYTE
        'Ȅ': 'E',
        'ȅ': 'E',
        'É': 'E',
        'é': 'E',
        'É̩': 'E', # MULTIBYTE
        'Ē': 'E',
        'ē': 'E',
        'Ḕ': 'E',
        'ḕ': 'E',
        'Ḗ': 'E',
        'ḗ': 'E',
        'Ẽ': 'E',
        'ẽ': 'E',
        'Ḛ': 'E',
        'ḛ': 'E',
        'Ę': 'E',
        'ę': 'E',
        'Ę́': 'E', # MULTIBYTE
        'ę́': 'E', # MULTIBYTE
        'Ę̃': 'E', # MULTIBYTE
        'ę̃': 'E', # MULTIBYTE
        'Ȩ': 'E',
        'ȩ': 'E',
        'E̩': 'E', # MULTIBYTE
        'e̩': 'E', # MULTIBYTE
        'ᶒ': 'E',
        ### F
        'Ƒ': 'F',
        'ƒ': 'F',
        'Ḟ': 'F',
        'ḟ': 'F',
        'ᵮ': 'F',
        'ᶂ': 'F',
        ### G
        'Ǵ': 'G',
        'ǵ': 'G',
        'Ǥ': 'G',
        'ǥ': 'G',
        'Ĝ': 'G',
        'ĝ': 'G',
        'Ǧ': 'G',
        'ǧ': 'G',
        'Ğ': 'G',
        'ğ': 'G',
        'Ģ': 'G',
        'ģ': 'G',
        'Ɠ': 'G',
        'ɠ': 'G',
        'Ġ': 'G',
        'ġ': 'G',
        'Ḡ': 'G',
        'ḡ': 'G',
        'Ꞡ': 'G',
        'ꞡ': 'G',
        'ᶃ': 'G',
        ### H
        'Ĥ': 'H',
        'ĥ': 'H',
        'Ȟ': 'H',
        'ȟ': 'H',
        'Ħ': 'H',
        'ħ': 'H',
        'Ḩ': 'H',
        'ḩ': 'H',
        'Ⱨ': 'H',
        'ⱨ': 'H',
        'ẖ': 'H',
        'ẖ': 'H',
        'Ḥ': 'H',
        'ḥ': 'H',
        'Ḣ': 'H',
        'ḣ': 'H',
        'Ḧ': 'H',
        'ḧ': 'H',
        'Ḫ': 'H',
        'ḫ': 'H',
        'Ꜧ': 'H',
        'ꜧ': 'H',
        ### I
        'Ị': 'I',
        'ị': 'I',
        'Ĭ': 'I',
        'ĭ': 'I',
        'Î': 'I',
        'î': 'I',
        'Ǐ': 'I',
        'ǐ': 'I',
        'Ɨ': 'I',
        'ɨ': 'I',
        'Ï': 'I',
        'ï': 'I',
        'Ḯ': 'I',
        'ḯ': 'I',
        'Í': 'I',
        'í': 'I',
        'Ì': 'I',
        'ì': 'I',
        'Ȉ': 'I',
        'ȉ': 'I',
        'Į': 'I',
        'į': 'I',
        'Į́': 'I', # MULTIBYTE
        'Į̃': 'I', # MULTIBYTE
        'Ī': 'I',
        'ī': 'I',
        'Ī̀': 'I', # MULTIBYTE
        'ī̀': 'I', # MULTIBYTE
        'ᶖ': 'I',
        'Ỉ': 'I',
        'ỉ': 'I',
        'Ȋ': 'I',
        'ȋ': 'I',
        'Ĩ ': 'I', # MULTIBYTE
        'ĩ': 'I',
        'Ḭ': 'I',
        'ḭ': 'I',
        'ᶤ': 'I',
        'İ': 'I', # dotted
        'i̇': 'I', # MULTIBYTE
        ### J
        'Ĵ': 'J',
        'ĵ': 'J',
        'J̌': 'J', # MULTIBYTE
        'ǰ': 'J',
        'Ɉ': 'J',
        'ɉ': 'J',
        'J̃': 'J', # MULTIBYTE
        'j̇̃': 'J', # MULTIBYTE
        ### K
        'Ƙ': 'K',
        'ƙ': 'K',
        'Ꝁ': 'K',
        'ꝁ': 'K',
        'Ḱ': 'K',
        'ḱ': 'K',
        'Ǩ': 'K',
        'ǩ': 'K',
        'Ḳ': 'K',
        'ḳ': 'K',
        'Ķ': 'K',
        'ķ': 'K',
        'ᶄ': 'K',
        'Ⱪ': 'K',
        'ⱪ': 'K',
        'Ḵ': 'K',
        'ḵ': 'K',
        ### L
        'Ĺ': 'L',
        'ĺ': 'L',
        'Ł': 'L',
        'ł': 'L',
        'Ľ': 'L',
        'ľ': 'L',
        'Ḹ': 'L',
        'ḹ': 'L',
        'L̃': 'L', # MULTIBYTE
        'l̃': 'L', # MULTIBYTE
        'Ļ': 'L',
        'ļ': 'L',
        'Ŀ': 'L',
        'ŀ': 'L',
        'Ḷ': 'L',
        'ḷ': 'L',
        'Ḻ': 'L',
        'ḻ': 'L',
        'Ḽ': 'L',
        'ḽ': 'L',
        'Ƚ': 'L',
        'ƚ': 'L',
        'Ⱡ': 'L',
        'ⱡ': 'L',
        ### M
        'Ḿ': 'M',
        'ḿ': 'M',
        'Ṁ': 'M',
        'ṁ': 'M',
        'Ṃ': 'M',
        'ṃ': 'M',
        'M̃': 'M', # MULTIBYTE
        'm̃': 'M', # MULTIBYTE
        'ᵯ': 'M',
        ### N
        'Ń': 'N',
        'ń': 'N',
        'Ñ': 'N',
        'ñ': 'N',
        'Ň': 'N',
        'ň': 'N',
        'Ǹ': 'N',
        'ǹ': 'N',
        'Ṅ': 'N',
        'ṅ': 'N',
        'Ṇ': 'N',
        'ṇ': 'N',
        'Ņ': 'N',
        'ņ': 'N',
        'Ṉ': 'N',
        'ṉ': 'N',
        'Ṋ': 'N',
        'ṋ': 'N',
        'Ꞥ': 'N',
        'ꞥ': 'N',
        'ᵰ': 'N',
        'ᶇ': 'N',
        ### O
        'Ø': 'O',
        'ø': 'O',
        'Ǿ': 'O',
        'ǿ': 'O',
        'ᶱ': 'O',
        'Ö': 'O',
        'ö': 'O',
        'Ȫ': 'O',
        'ȫ': 'O',
        'Ó': 'O',
        'ó': 'O',
        'Ò': 'O',
        'ò': 'O',
        'Ô': 'O',
        'ô': 'O',
        'Ố': 'O',
        'ố': 'O',
        'Ồ': 'O',
        'ồ': 'O',
        'Ổ': 'O',
        'ổ': 'O',
        'Ỗ': 'O',
        'ỗ': 'O',
        'Ộ': 'O',
        'ộ': 'O',
        'Ǒ': 'O',
        'ǒ': 'O',
        'Ő': 'O',
        'ő': 'O',
        'Ŏ': 'O',
        'ŏ': 'O',
        'Ȏ': 'O',
        'ȏ': 'O',
        'Ȯ': 'O',
        'ȯ': 'O',
        'Ȱ': 'O',
        'ȱ': 'O',
        'Ọ': 'O',
        'ọ': 'O',
        'Ɵ': 'O',
        'ɵ': 'O',
        'Ơ': 'O',
        'ơ': 'O',
        'Ớ': 'O',
        'ớ': 'O',
        'Ờ': 'O',
        'ờ': 'O',
        'Ỡ': 'O',
        'ỡ': 'O',
        'Ợ': 'O',
        'ợ': 'O',
        'Ở': 'O',
        'ở': 'O',
        'Ỏ': 'O',
        'ỏ': 'O',
        'Ō': 'O',
        'ō': 'O',
        'Ṓ': 'O',
        'ṓ': 'O',
        'Ṑ': 'O',
        'ṑ': 'O',
        'Õ': 'O',
        'õ': 'O',
        'Ȭ': 'O',
        'ȭ': 'O',
        'Ṍ': 'O',
        'ṍ': 'O',
        'Ṏ': 'O',
        'ṏ': 'O',
        'Ǫ': 'O',
        'ǫ': 'O',
        'Ȍ': 'O',
        'ȍ': 'O',
        'O̩': 'O', # MULTIBYTE
        'o̩': 'O', # MULTIBYTE
        'Ó̩': 'O', # MULTIBYTE
        'ó̩': 'O', # MULTIBYTE
        'Ò̩': 'O', # MULTIBYTE
        'ò̩': 'O', # MULTIBYTE
        'Ǭ': 'O',
        'ǭ': 'O',
        'O͍': 'O', # MULTIBYTE
        'o͍': 'O', # MULTIBYTE
        ### P
        'Ṕ': 'P',
        'ṕ': 'P',
        'Ṗ': 'P',
        'ṗ': 'P',
        'Ᵽ': 'P',
        'ᵽ': 'P',
        'Ƥ': 'P',
        'ƥ': 'P',
        'ᵱ': 'P',
        'ᶈ': 'P',
        ### Q
        'ʠ': 'Q',
        'Ɋ': 'Q',
        'ɋ': 'Q',
        ### R
        'Ŕ': 'R',
        'ŕ': 'R',
        'Ɍ': 'R',
        'ɍ': 'R',
        'Ř': 'R',
        'ř': 'R',
        'Ŗ': 'R',
        'ŗ': 'R',
        'Ȑ': 'R',
        'ȑ': 'R',
        'Ȓ': 'R',
        'ȓ': 'R',
        'ɽ': 'R',
        'R̃': 'R', # MULTIBYTE
        ### S
        'Ś': 'S',
        'ś': 'S',
        'S̩': 'S', # MULTIBYTE
        'Ŝ': 'S',
        'ŝ': 'S',
        'Š': 'S',
        'š': 'S',
        'Ş': 'S',
        'ş': 'S',
        'ș': 'S',
        'S̈': 'S', # MULTIBYTE
        'ȿ': 'S',
        '𐌔': 'S', # Italic
        '𐍃': 'S', # Gothic
        ### T
        'Ť': 'T',
        'ť': 'T',
        'Ţ': 'T',
        'ţ': 'T',
        'Ʈ': 'T',
        'ʈ': 'T',
        'Ț': 'T',
        'ț': 'T',
        'ƫ': 'T',
        'Ŧ': 'T',
        'ŧ': 'T',
        'Ⱦ': 'T',
        'Ƭ': 'T',
        'ƭ': 'T',
        'Ꞇ': 'T',
        'Τ': 'T', # Greek
        'Ⲧ': 'T', # Coptic
        'Т': 'T', # Cyrilic
        '𐌕': 'T', # Italic
        'ᛏ': 'T', # runic
        'ፐ': 'T', # Ge'ez
        ### U
        'Ŭ': 'U',
        'ŭ': 'U',
        'Ʉ': 'U',
        'ʉ': 'U',
        'Ü': 'U',
        'ü': 'U',
        'Ǜ': 'U',
        'ǜ': 'U',
        'Ǘ': 'U',
        'ǘ': 'U',
        'Ǚ': 'U',
        'ǚ': 'U',
        'Ǖ': 'U',
        'ǖ': 'U',
        'Ú': 'U',
        'ú': 'U',
        'Ù': 'U',
        'ù': 'U',
        'Û': 'U',
        'û': 'U',
        'Ǔ': 'U',
        'ǔ': 'U',
        'Ȗ': 'U',
        'ȗ': 'U',
        'Ű': 'U',
        'ű': 'U',
        'Ŭ': 'U',
        'ŭ': 'U',
        'Ư': 'U',
        'ư': 'U',
        'Ū': 'U',
        'ū': 'U',
        'Ū̀': 'U', # MULTIBYTE
        'ū̀': 'U', # MULTIBYTE
        'Ū́': 'U', # MULTIBYTE
        'ū́': 'U', # MULTIBYTE
        'Ū̃': 'U', # MULTIBYTE
        'ū̃': 'U', # MULTIBYTE
        'Ũ': 'U',
        'ũ': 'U',
        'Ų': 'U',
        'ų': 'U',
        'Ų́': 'U', # MULTIBYTE
        'ų́': 'U', # MULTIBYTE
        'Ų̃': 'U', # MULTIBYTE
        'ų̃': 'U', # MULTIBYTE
        'Ȕ': 'U',
        'ȕ': 'U',
        'Ů': 'U',
        'ů': 'U',
        ### V
        'Ṽ': 'V',
        'Ṿ': 'V',
        'Ʋ': 'U',
        'ᶌ': 'v',
        'Ʌ': 'V', # turned
        'ⱴ': 'v', # curl
        '℣': 'v', # versical
        'Ꝟ': 'V', # scribal
        'Ỽ': 'V', # Welsh
        ### W
        "Ẃ": "W",
        "ẃ": "W",
        "Ẁ": "W",
        "ẁ": "W",
        "Ŵ": "W",
        "ŵ": "W",
        "Ẅ": "W",
        "ẅ": "W",
        "Ẇ": "W",
        "ẇ": "W",
        "Ẉ": "W",
        "ẉ": "W",
        "ẘ": "W",
        'Ⱳ': 'W', # hook
        'Ꝡ': 'W', # VY
        ### X
        "Ẍ": "X",
        "ẍ": "X",
        "Ẋ": "X",
        "ẋ": "X",
        "X̂": "X",
        "x̂": "X",
        "ᶍ": "X",
        ### Y
        #'Ý': 'Y',
        "Ý": "Y",
        "ý": "Y",
        "Ỳ": "Y",
        "ỳ": "Y",
        "Ŷ": "Y",
        "ŷ": "Y",
        "Ÿ": "Y",
        "ÿ": "Y",
        "Ỹ": "Y",
        "ỹ": "Y",
        "Ẏ": "Y",
        "ẏ": "Y",
        "Ỵ": "Y",
        "ỵ": "Y",
        "ẙ": "Y",
        "Ỷ": "Y",
        "ỷ": "Y",
        "Ȳ": "Y",
        "ȳ": "Y",
        "Ɏ": "Y",
        "ɏ": "Y",
        "Ƴ": "Y",
        "ƴ": "Y",
        'Ɏ': 'Y', # stroke
        'Ƴ': 'Y', # hook
        'Ỿ': 'Y', # schwa
        ### Z
        "Ź": "Z",
        "ź": "Z",
        "Ẑ": "Z",
        "ẑ": "Z",
        "Ž": "Z",
        "ž": "Z",
        "Ż": "Z",
        "ż": "Z",
        "Ẓ": "Z",
        "ẓ": "Z",
        "Ẕ": "Z",
        "ẕ": "Z",
        "Ƶ": "Z",
        "ƶ": "Z",
        "ᵶ": "Z",
        "ᶎ": "Z",
        "Ⱬ": "Z",
        "ⱬ": "Z",
        # other
        "–": "-", # en dash
        "’": "'",
        "`": "'"
    }

    @classmethod
    def single_byte(cls):
        '''Returns a dict that can be used with `str.makestrans` (`makestrans` 
        only accepts single-byte keys)'''

        return {key: AsciiMap.data[key] for key in filter(lambda x: len(x) == 1, AsciiMap.data.keys())}

    @classmethod
    def multi_byte(cls):
        '''Returns a dict of keys that cannot be used by `str.maketrans` (`maketrans`
        only accepts single-byte keys)'''

        return {key: AsciiMap.data[key] for key in filter(lambda x: len(x) > 1, AsciiMap.data.keys())}

class SynonymMap():
    # todo
    data = [
        ['car', 'automobile']
    ]

class Tokenizer:
    '''For splitting a string of words into asciified stem words'''

    from nltk import PorterStemmer
    STEMMER = PorterStemmer()
    
    @classmethod
    def split_words(cls, string):
        return re.compile(r'\w+').findall(string)
        
    @classmethod
    def asciify(cls, string):
        if all(ord(char) < 128 for char in string):
            # all chars are ascii
            return string

        if all(char not in string for char in AsciiMap.data.keys()):
            # none of the chars in the map are in the string
            return string

        for char, rep in AsciiMap.multi_byte().items():
            # chars that can't be used in the maketrans table
            if char in string:
                string = string.replace(char, rep)

        string = string.translate(str.maketrans(AsciiMap.single_byte())).lower()

        return string

    @classmethod
    def stem(cls, string):
        return Tokenizer.STEMMER.stem(string)

    @classmethod
    def scrub(cls, string):
        #string = re.sub(r"['‘’]", '', string) # apostrophes
        return re.sub(r'\W+', ' ', Tokenizer.asciify(string.upper()).lower()).strip()

    @classmethod
    def tokenize(cls, string):
        return [Tokenizer.stem(x) for x in Tokenizer.split_words(Tokenizer.asciify(string))]

