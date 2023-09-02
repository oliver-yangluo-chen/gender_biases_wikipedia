import xml.etree.ElementTree as ET

file_name = "test.xml"
tree = ET.parse(file_name)
root = tree.getroot()

for i in root.iter():
  print(i.attrib)
