import bz2
import xml.etree.ElementTree as ET
import wikitextparser as wtp
import sqlite3 as sql
import zlib

def get_infobox(text):
  parsed = wtp.parse(text)
  for tmpl in parsed.templates:
    if('Infobox' in tmpl.name):
      return tmpl
  return None

def get_spouse(infobox_args):
  for arg in infobox_args:
    if((arg.name.lower().strip() == 'spouse' or arg.name.lower().strip() == 'spouse(s)') and arg.value.strip() != '\n'):
      return arg.value
  return None
  
def spouse_names(spouse):
  parsed = wtp.parse(spouse)
  spouse_list = []
  for link in parsed.wikilinks:
    spouse_list.append(link.title.strip())
  return spouse_list
  
def clean_name(name):
  if('(' not in name):
    return name
  return name[:name.find('(')]
  
def gender(text):
  MALE_PRONOUNS = ['he', 'him', 'his', 'himself']
  FEMALE_PRONOUNS = ['she', 'her', 'hers', 'herself']
  male, female = 0, 0
  for word in text.split():
    word = word.strip(' ~!@#$%^&*()_+`-=[]\\{}|;\':\",./<>?') #remove punctuation, not perfect
    if(word in MALE_PRONOUNS):
      male += 1
    elif(word in FEMALE_PRONOUNS):
        female += 1
  if(male > female):
    return 1
  return 0
