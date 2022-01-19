import zipfile
import pathlib
import tempfile
import xml.etree.ElementTree as ET
import collections
import csv
import time
import concurrent.futures
import sys

def check_dir(dir_name):
    return pathlib.Path(dir_name).glob("*.zip")
    
def unzip_xmls(file_name):
    try:
        with zipfile.ZipFile(file_name, "r") as zf:
            if not zf.testzip():
                return tuple(map(
                    zf.extract,
                    (member for member in zf.infolist()
                    if pathlib.Path(member.filename).suffix=='.xml')
                ))
                
            else:
                print(f"{file_name} is NOT zip!")
                return []
    except:
        print(f"Something is wrong with {file_name}")
        return []

def get_xml_data(xml_file_name):
    result={"objects":[]} 
    for event, elem in ET.iterparse(xml_file_name):
        if elem.tag == "var_id" and elem.attrib.get("varname")=="id":
             result["xml_id"]=elem.attrib.get("value")
        if elem.tag == "var_level" and elem.attrib.get("varname")=="level":
             result["xml_level"]=elem.attrib.get("value")
        if elem.tag == "obj" and "objectname" in elem.attrib:
             result["objects"].append(elem.attrib.get("objectname"))
    return result 
                 
  
def write_to_csvs(xml_contents):
    with open("levels.csv","a") as lcsv, open("objs.csv","a") as objcsv:
        levs_writer=csv.writer(lcsv, lineterminator="\n")
        objs_writer=csv.writer(objcsv, lineterminator="\n")
        for content in xml_contents:
            levs_writer.writerow([content["xml_id"],content["xml_level"]])
            for obj in content["objects"]:
                objs_writer.writerow([content["xml_id"], obj])
                                     

def main():
    start = time.monotonic()
    start_path="" 
    if len(sys.argv) > 1:
        dir_path = pathlib.Path(sys.argv[1]).resolve()
        if not dir_path.exists():
            print(f"The specified path {dir_path} not found!")
            return
        if not dir_path.is_dir():
            print(f"The specified path {dir_path} is not a directory!")
            return
        start_path=dir_path
   
    zip_files = list(check_dir(start_path))
    xml_data=[]
    with concurrent.futures.ProcessPoolExecutor() as unzip_ex:
       for done_future in list(unzip_ex.map(unzip_xmls, zip_files)):
           for data in unzip_ex.map(get_xml_data, done_future):
               xml_data.append(data)
    write_to_csvs(xml_data)            
    stop = time.monotonic()
    print(f"done in {stop-start} seconds")

if __name__=="__main__":
    main()

