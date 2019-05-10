

# rendering a page with a remplate engine

from jinja2 import Template

our_template = '''
<html>

    A record:<br>
    {{ data }}

</html>


'''

jmarc = {
    '_id' : 1, 
    '245' : [
        {
            'code' : 'a', 
            'value' : 'title'
        }
    ]
}

template = Template(our_template)
rendered = template.render(data=jmarc)

print('The jinja-rendered page:\n')
print(rendered)

# rendering a page with javascript

our_page = '''
<html>
<body>

<script>

</script>
<body>    
</html>

'''



