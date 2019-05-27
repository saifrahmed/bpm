import sys
import os
import argparse 
import zipfile
import tempfile
import logging
import xml.etree.ElementTree as ET


FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('apple_health')
logger.setLevel(logging.DEBUG)


def find_data_files(infile, indir):
    if infile and indir:
        logger.error("Cannot have both an input file to extract and also a ready-extracted data dir")
        raise Exception("Cannot have both an input file to extract and also a ready-extracted data dir")

    if infile:
        if not (os.path.exists(input_file)):
            logger.error(f"Bad input file received {input_file}")
            raise Exception(f"Bad input file received {input_file}")

        if (os.path.isdir(input_file)):
            # we didnt get an exact file input, lets look for the expected name
            input_file = os.path.join(input_file, "export.zip")

            # is it actually there?
            if not (os.path.exists(input_file)):
                raise Exception(f"Received input directory, but expected input file {input_file} not found")        


        with tempfile.TemporaryDirectory() as tarball_contents:
            logger.info(f"Working in folder {tarball_contents}")
            zip_ref = zipfile.ZipFile(input_file, 'r')
            zip_ref.extractall(tarball_contents)
            zip_ref.close()
            keyfile1 = os.path.join(tarball_contents, "apple_health_export", "export_cda.xml")
            keyfile2 = os.path.join(tarball_contents, "apple_health_export", "export.xml")

            if not (os.path.exists(keyfile1)):
                logger.error(f"Bad input file received, missing key file export_cda.xml")            
                raise Exception(f"Bad input file received, missing key file export_cda.xml")

            if not (os.path.exists(keyfile2)):
                logger.error(f"Bad input file received, missing key file export.xml")
                raise Exception(f"Bad input file received, missing key file export.xml")

            return keyfile2, keyfile1


    if indir:
        if not (os.path.exists(indir)):
            logger.error(f"Bad input file received {indir}")            
            raise Exception(f"Bad input file received {indir}")

        if not (os.path.isdir(indir)):
            logger.error(f"Received non-existent input directory {indir}")            
            raise Exception(f"Received non-existent input directory {indir}")        

        keyfile1 = os.path.join(indir, "export_cda.xml")
        keyfile2 = os.path.join(indir, "export.xml")

        if not (os.path.exists(keyfile1)):
            logger.error(f"Bad input file received, missing key file export_cda.xml")            
            raise Exception(f"Bad input file received, missing key file export_cda.xml")

        if not (os.path.exists(keyfile2)):
            logger.error(f"Bad input file received, missing key file export.xml")
            raise Exception(f"Bad input file received, missing key file export.xml")

        return keyfile2, keyfile1        

def process_data_files(exportfile, cdafile):
    logger.info(f"Processing Export File {exportfile}")
    print()
    print("start_date,end_date,observation_time,hr_bpm")
    tree = ET.parse(exportfile)
    root = tree.getroot()  
    for child in root.iter():
        #print(child.tag)
        if child.tag == 'Record':
            #print(child.tag)
            if 'type' in child.attrib and child.attrib['type']=='HKQuantityTypeIdentifierHeartRateVariabilitySDNN':
                grandchildren = child.iter()
                for gc in grandchildren:
                    if gc.tag == 'InstantaneousBeatsPerMinute':
                        if 'bpm' in gc.attrib:
                            st = child.attrib['startDate']
                            ed = child.attrib['endDate']

                            bpm = gc.attrib['bpm']
                            tm = gc.attrib['time']
                            print(f"{st},{ed},{tm},{bpm}")

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', help='Path to Apple Health extract zipball')
    parser.add_argument('--datadir', help='Path to Apple Health extracted files')

    args = parser.parse_args()

    input_file = None 
    data_dir = None 

    if args.infile:
        input_file = args.infile
        logger.info(f"Path to input: {input_file}")

    if args.datadir:
        data_dir = args.datadir
        logger.info(f"Path to readily available data: {data_dir}")

    exportfile, cdafile = find_data_files(input_file, data_dir)
    process_data_files(exportfile, cdafile)



  
if __name__== "__main__":
    main()