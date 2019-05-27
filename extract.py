import sys
import os
import argparse 
import zipfile
import tempfile
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, date


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

    # Extract 1
    # Extract first order HR observations
    # https://developer.apple.com/documentation/healthkit/hkquantitytypeidentifier/2881127-heartratevariabilitysdnn
    
    '''
     <Record type="HKQuantityTypeIdentifierHeartRate" 
         sourceName="Saif Ahmed" 
         sourceVersion="4.0" 
         device="&lt;&lt;HKDevice: 0x282d50a50&gt;, name:Apple Watch, manufacturer:Apple, model:Watch, hardware:Watch3,4, software:4.0&gt;" 
         unit="count/min" 
         creationDate="2017-11-15 00:15:23 -0400" 
         startDate="2017-11-15 00:13:33 -0400" 
         endDate="2017-11-15 00:13:33 -0400" 
         value="76">
     '''    

    # Extract 2
    # Extract HR observations from HR Variability SD
    # https://developer.apple.com/documentation/healthkit/hkquantitytypeidentifier/2881127-heartratevariabilitysdnn

    '''
     <Record type="HKQuantityTypeIdentifierHeartRateVariabilitySDNN" 
         sourceName="Saif Ahmed" 
         sourceVersion="4.1" 
         device="&lt;&lt;HKDevice: 0x282d8eda0&gt;, name:Apple Watch, manufacturer:Apple, model:Watch, hardware:Watch3,4, software:4.1&gt;" 
         unit="ms" 
         creationDate="2017-11-22 19:15:52 -0400" 
         startDate="2017-11-22 19:14:47 -0400" 
         endDate="2017-11-22 19:15:52 -0400" 
         value="32.1111">
      <HeartRateVariabilityMetadataList>
       <InstantaneousBeatsPerMinute bpm="95" time="6:14:48.94 PM"/>
       <InstantaneousBeatsPerMinute bpm="94" time="6:14:49.58 PM"/>
       <InstantaneousBeatsPerMinute bpm="91" time="6:14:50.24 PM"/>
       <InstantaneousBeatsPerMinute bpm="93" time="6:14:50.88 PM"/>
    '''

    for child in root.iter():
        #print(child.tag)
        if child.tag == 'Record':
            #print(child.tag)
            if 'type' in child.attrib:

                if child.attrib['type']=='HKQuantityTypeIdentifierHeartRate':
                    if 'value' in child.attrib:
                        st = child.attrib['startDate']
                        ed = child.attrib['endDate']
                        bpm = child.attrib['value']
                        #print(f"{st},{ed},,{bpm}")
                        print(f"{st},{bpm}")

                elif child.attrib['type']=='HKQuantityTypeIdentifierHeartRateVariabilitySDNN':

                    seq_st = child.attrib['startDate']
                    seq_ed = child.attrib['endDate']

                    seq_st_dt = datetime.strptime(seq_st, '%Y-%m-%d %H:%M:%S %z')

                    grandchildren = child.iter()
                    obs_first = None
                    for gc in grandchildren:
                        if gc.tag == 'InstantaneousBeatsPerMinute':
                            if not obs_first:
                                obs_first = gc.attrib['time']
                                st_dt = datetime.strptime(obs_first, '%H:%M:%S.%f %p')
                            
                            if 'bpm' in gc.attrib:
                                bpm = gc.attrib['bpm']

                                # Derive elapsed time offset within observation series
                                tm = gc.attrib['time']
                                ed_dt = datetime.strptime(tm, '%H:%M:%S.%f %p')
                                time_offset = datetime.combine(date.min, ed_dt.time()) - datetime.combine(date.min, st_dt.time())

                                # Apply elapsed time offset to sequence start time
                                seq_st_dt_plusdelta = seq_st_dt+time_offset

                                print(f"{seq_st_dt_plusdelta},{bpm}")
                                #print(f"{st},{ed},{time_offset},{bpm}")
                                #print(f"{seq_st},{seq_ed},{tm},{time_offset},{seq_st_dt_plusdelta},{bpm}")
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