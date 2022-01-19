import xml.etree.ElementTree as ET
import uuid
import random
import zipfile
import concurrent.futures
import time
import pathlib

def create_xml_file(file_number):
    root=ET.Element("root")
    varname_level=ET.SubElement(root, "var_id")
    varname_level.set("varname","id")
    varname_level.set("value",str(uuid.uuid4()))

    varname_id=ET.SubElement(root, "var_level")
    varname_id.set("varname","level")
    varname_id.set("value",str(random.randint(1,100)))

    objects=ET.SubElement(root, "objects")
    for i in range(random.randint(1,10)):
        xml_object=ET.SubElement(objects, "obj")
        xml_object.set("objectname",str(uuid.uuid4()))
    tree=ET.ElementTree(root)
    file_name=f"test{file_number}.xml"
    tree.write(file_name, encoding="utf-8", xml_declaration=True)
    return file_name

def zip_files(zip_file_name, file_names):
    with zipfile.ZipFile(zip_file_name, "w") as zf:
        for file_name in file_names:
            zf.write(file_name)


def main():
    start=time.monotonic()
    file_nums=(i for i in range(1,501))
    with concurrent.futures.ThreadPoolExecutor() as tpe:
        xml_futures=[]
        done_xml_futures=[]
        zip_index=1
        for file_num in file_nums:
            xml_futures.append(tpe.submit(create_xml_file, file_num))
        for future_done in concurrent.futures.as_completed(xml_futures):
            xml_created=future_done.result()
            done_xml_futures.append(xml_created)
            if len(done_xml_futures)==100:
                print("sotochka!")
                zip_files(f"test_zip{zip_index}.zip", done_xml_futures)
                zip_index+=1 
                for unpacked_xml in done_xml_futures:
                    pathlib.Path(unpacked_xml).unlink()
                done_xml_futures.clear()
    stop=time.monotonic()
    print(f"execution took {stop-start}")

if __name__=="__main__":
    main()

