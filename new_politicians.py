import bz2
import xml.etree.ElementTree as ET
import wikitextparser as wtp
import sqlite3 as sql
conn = sql.connect('connect.db')
c = conn.cursor()

file_name = "enwiki-20200901-pages-articles-multistream.xml.bz2"

def get_article(page):
    return str(page.find("revision/text").text)

def get_infobox(article):
    parsed = wtp.parse(article)
    for tmpl in parsed.templates:
        if('Infobox' in tmpl.name):
            return tmpl
    return None

def get_shortDes(article):
    parsed = wtp.parse(article)
    for tmpl in parsed.templates:
        if('short description' in tmpl.name):
            return tmpl
    return None

def get_categories(article):
    parsed = wtp.parse(article)
    s = parsed.get_sections()[-1].string
    return s[s.find("[[Category:"):].split("\n")

def is_politician(categories):
    for category in categories:
        if("politician" in category.lower()):
            return True
    return False

def is_living(categories):
    for category in categories:
        if("living" in category.lower()):
            return True
    return False

def is_position(shortDes, position):
    for arg in shortDes.args:
        if(position.lower().strip() in arg.name.lower().strip()):
            return True
    return False

def print1000(num, name):
    if(num % 1000 == 0):
        print(name + ": " + str(num))

def commit1000(commit_count):
    if(commit_count % 1000 == 0):
        conn.commit()

def get_gender(article):
  MALE_PRONOUNS = ['he', 'him', 'his', 'himself']
  FEMALE_PRONOUNS = ['she', 'her', 'hers', 'herself']
  male, female = 0, 0
  for word in article.split():
    word = word.strip(' ~!@#$%^&*()_+`-=[]\\{}|;\':\",./<>?') #remove punctuation, not perfect
    if(word in MALE_PRONOUNS):
      male += 1
    elif(word in FEMALE_PRONOUNS):
        female += 1
  return male > female

def get_spouse(infobox):
    infobox = get_infobox(infobox)
    for arg in infobox.arguments:
        if('spouse' in arg.name.lower().strip() and arg.value.strip() != '\n'):
            return arg.value
    return None

def char_count(infobox, name):
    total = 0.0
    count = 0.0
    infobox = get_infobox(infobox)
    for arg in infobox.arguments:
        total += len(arg.name)
        total += len(arg.value)
        if(name.lower() in arg.name.lower().strip()):
            count += len(arg.name)
            count += len(arg.value)
    return (count, total)

def extract():
    m_total, f_total = 0, 0
    m_total_char, f_total_char = 0, 0
    m_spouse, f_spouse, m_child_char, f_child_char, m_spouse_char, f_spouse_char = 0, 0, 0, 0, 0, 0
    for (pageid, name, gender, infobox) in c.execute("SELECT * FROM politicians"):
        print(infobox)
        if(infobox != None):
            spouse = get_spouse(infobox)
            count_spouse = char_count(infobox, 'spouse')
            count_child = char_count(infobox, 'child')
            if(gender):
                m_total += 1
                m_spouse += 1
                m_child_char += count_child[0]
                m_total_char += count_child[1]
                m_spouse_char += count_spouse[0]
            else:
                f_total += 1
                f_spouse += 1
                f_child_char += count_child[0]
                f_total_char += count_child[1]
                f_spouse_char += count_spouse[0]
        print(m_total, f_total, m_total_char, f_total_char, m_spouse, f_spouse, m_child_char, f_child_char, m_spouse_char, f_spouse_char)
        
def iterate_file(file_name):
    c.execute('''
    CREATE TABLE IF NOT EXISTS politicians(
    pageid integer,
    name text,
    gender integer,
    infobox text)''')
    conn.commit()
    page_count, living_count, politician_count, commit_count = 0, 0, 0, 0
    for event, elem in ET.iterparse(file_name, events = ('end',)):
        if elem.tag == 'page':
            page_count += 1
            id = elem.find('id').text
            name = elem.find('title').text
            article = get_article(page)
            infobox = get_infobox(article)
            shortDes = get_shortDes(article)
            categories = get_categories(article)
            gender = get_gender(article)
            if(is_living(categories)):
                living_count += 1
            if(is_politician(categories)):
                politician_count += 1
                c.execute("INSERT INTO all_pages VALUES (?, ?, ?, ?)", (pageid, name, gender, repr(infobox)))
                commit_count += 1
                commit1000(commit_count)
            elem.clear()
            print1000(page_count, "page_count")
            print1000(living_count, "living_count")
            print1000(politician_count, "politician_count")

    conn.commit()

#xml_file = bz2.open(file_name)
#iterate_file(xml_file)
extract()

