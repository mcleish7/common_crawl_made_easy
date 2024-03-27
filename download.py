"""
File to extract a small amount of ENGLISH Common Crawl data into json files with the corresponding URLS
This extracts for both WARC (raw HTML) and WET files (extracted clean text by common crawl)
Developed by Sean McLeish (University of Maryland)

NOTE: I have left some commented code in the file to help others implement more methods quickly, feel free to remove it
"""

import os
import gzip
import warcio
import re
import glob
import json
import argparse

def count_lines(file_path):
    """
    Counts the number of lines in a .gz file
    """
    with gzip.open(file_path, 'rt', encoding='utf-8') as file:
        line_count = sum(1 for _ in file)
    return line_count

def crawl_index(dir_path, paths_file, type='warc', max_files=5, offset=0):
    """
    For each line in the paths file input, the corresponding file is downloaded for the first `max_files lines 
    WARNING: There are alot of documents int the downloaded files so they are very large
    Example line from each file type:
    WARC: 'crawl-data/CC-MAIN-2023-50/segments/1700679099281.67/warc/CC-MAIN-20231128083443-20231128113443-00000.warc.gz'
    WET: 'crawl-data/CC-MAIN-2023-50/segments/1700679099281.67/wet/CC-MAIN-20231128083443-20231128113443-00000.warc.wet.gz'
    WAT: 'crawl-data/CC-MAIN-2023-50/segments/1700679099281.67/wat/CC-MAIN-20231128083443-20231128113443-00000.warc.wat.gz'
    """
    print(f"Running paths for {paths_file}, with dir path {dir_path}, type={type}")
    os.chdir(dir_path)
    os.makedirs(f"{type}_index", exist_ok=True)
    os.chdir(f"{type}_index") # now have a file to download all these paths into
    print(os.getcwd())
    number_of_lines = count_lines(paths_file)
    sample_interval = int(number_of_lines/max_files)
    with gzip.open(paths_file, 'rt', encoding='utf-8') as file:
        for i, line in enumerate(file):
            if i % sample_interval == offset:
                print(f"running: wget https://data.commoncrawl.org/{line.strip()}")
                os.system(f"wget https://data.commoncrawl.org/{line.strip()}")
                print(f"finsihed running: wget https://data.commoncrawl.org/{line.strip()}")
    
def read_warc(dir_path):
    """
    Scan through WARC files e.g: CC-MAIN-20231128083443-20231128113443-00000.warc.gz
    The lines of the file run in cylces of 3 for each webpage: [request, response, metadata]
    Request: information about the request that was went to the webpage from common crawl
    Response: the html returned
    MetaData: metadata on the webpage, e.g. language and encoding type

    Saves a json with he corresponding name in warc_json_data
    Json data has shape [(URL0, record_ID0, HTML0), (URL1, record_ID1, HTML1), ...]
    Note: We only extract websites identified as English by common crawl
    TODO: Implement more robust ways to deal with unidentifed language and encodings
    """
    os.chdir(f"{dir_path}")
    os.makedirs(f"warc_json_data", exist_ok=True)
    os.chdir(f"{dir_path}/warc_index")

    warc_files = glob.glob("*.warc.gz")
    for warc_file in warc_files:
        print(f"Extracting data from: {warc_file}")
        base_name = warc_file.split('.')[0]
        processed = []
        store = None
        with gzip.open(warc_file, 'rb') as file:
            for record in  warcio.ArchiveIterator(file):
                url = record.rec_headers.get_header('WARC-Target-URI')

                # if record.rec_type == 'request':
                #     # stores the meta data on the request, has target url in
                #     meta = record.rec_headers.headers
                #     meta_meta = record.http_headers.headers # CC meta data about request

                if record.rec_type == 'response':
                    # meta = record.rec_headers.headers
                    data = record.content_stream().read()
                    store = data # store the data to decode in the meta stage
                elif record.rec_type == 'metadata':
                    # meta_meta = record.rec_headers.headers
                    meta = record.content_stream().read().decode('utf-8').strip()
                    try: # if we cannot detect the language we skip the file
                        language_code = re.search(r'"code":"([^"]+)"', meta).group(1) # only want english
                    except:
                        language_code = "none"
                    if language_code == 'en':
                        charset = re.search(r'charset-detected:\s+(\S+)', meta).group(1)
                        record_ID = record.rec_headers.get_header('WARC-Concurrent-To')
                        if store is not None:
                            try: # if we hit a decode error we skip the file
                                store = store.decode(charset)
                                processed.append((url, record_ID, store))
                            except:
                                store = None
                    store = None

        # save the data down in json format
        json_data = json.dumps(processed)
        with open(f"../warc_json_data/{base_name}.json", 'w') as json_file:
            json_file.write(json_data)

def read_wet(dir_path):
    """
    This reads the WET files, e.g: CC-MAIN-20231128083443-20231128113443-00000.warc.wet.gz
    WET files have one record type we are interested in `conversion`, this stores the extracted text from the HTML webpages that were scraped
    returns list of tuples where each tuples has form (url, refers_to_ID, data)
    refers_to_ID can be used to march to WARC record
    """
    os.chdir(f"{dir_path}")
    os.makedirs(f"wet_json_data", exist_ok=True)
    os.chdir(f"{dir_path}/wet_index")

    wet_files = glob.glob("*.warc.wet.gz")
    for wet_file in wet_files:
        print(f"Extracting data from: {wet_file}")
        base_name = wet_file.split('.')[0]
        processed = []
        with gzip.open(wet_file, 'rb') as file:
            i=0
            for record in  warcio.ArchiveIterator(file):
                if record.rec_type == 'conversion':
                    url = record.rec_headers.get_header('WARC-Target-URI')
                    language = record.rec_headers.get_header('WARC-Identified-Content-Language')
                    refersID = record.rec_headers.get_header('WARC-Refers-To')
                    if language == "eng":
                        # meta = record.rec_headers.headers
                        data = record.content_stream().read().decode('utf-8')
                        processed.append((url, refersID, data))
        
        # save the data down in json format
        json_data = json.dumps(processed)
        with open(f"../wet_json_data/{base_name}.json", 'w') as json_file:
            json_file.write(json_data)

def merge_lists(warc_list_of_tuples, wet_list_of_tuples):
    """
    Merges the warc and wet lists of tuples together 
    Wet list of tuples has tuples like: (url, ID, wet_data)
    Warc list of tuples has tuples like: (url, ID, warc_data)

    Returns a list of tuples like: (url, ID, warc_data, wet_data)
    """
    tuple_dict = {(url, ID): (url, ID, wet_data) for url, ID, wet_data in wet_list_of_tuples}
    merged_list = [(url, ID, warc_data, tuple_dict.get((url, ID))[2]) for url, ID, warc_data in warc_list_of_tuples if (url, ID) in tuple_dict]
    # tuple_dict.get((url, ID))[2] = corresponding wet data
    return merged_list

def common_elements(dir_path):
    """
    Matches the URLS from the WARC and WET files, then puts this in another json which is a list of tuples (URL, WARC DATA, WET DATA)
    NOTE: this has bad time complexity
    """
    os.chdir(f"{dir_path}")
    os.makedirs(f"combined_json_data", exist_ok=True)
    wet_files = glob.glob("wet_json_data/*.json")
    wet_files = [item.replace('wet_json_data/', '') for item in wet_files]
    warc_files = glob.glob("warc_json_data/*.json")
    warc_files = [item.replace('warc_json_data/', '') for item in warc_files]
    common_files = set(warc_files).intersection(set(wet_files))
    for common_file in common_files:
        print(f"Merging data from WARC and WET files for: {common_file}")
        common_output = []
        with open(f"warc_json_data/{common_file}", 'r') as json_file:
            warc = json.load(json_file)
        with open(f"wet_json_data/{common_file}", 'r') as json_file:
            wet = json.load(json_file)
        
        merged_list_of_tuples = merge_lists(warc, wet) # List of tuples like: (url, ID, warc_data, wet_data)

        json_data = json.dumps(merged_list_of_tuples)
        with open(f"combined_json_data/{common_file}", 'w') as json_file:
            json_file.write(json_data)

def main():
    parser = argparse.ArgumentParser(description="Download parts of Common Crawl")
    parser.add_argument('--warc', action='store_true', help="only process the warc files")
    parser.add_argument('--wet', action='store_true', help="only process the wet files")
    parser.add_argument('--delete_after', action='store_true', help="automatically deletes the temporary files after")
    parser.add_argument("--path", type=str, required=True, help="path to the directory of the `warc.paths.gz` files")
    parser.add_argument("--max_files", type=int, required=True, help="maximum number of files to download")
    parser.add_argument("--offset", type=int, required=True, help="sample files with this offset from the interval")
    FLAGS = parser.parse_args()

    max_files = FLAGS.max_files
    dir_path = FLAGS.path
    delete_after = FLAGS.delete_after

    # Extract the parts of the WARC and WET files we need into jsons
    if FLAGS.warc:
        print("WARC ONLY")
        crawl_index(dir_path, f"{dir_path}/warc.paths.gz", type="warc", max_files=max_files, offset=FLAGS.offset)
        read_warc(dir_path)
        exit()
    elif FLAGS.wet:
        print("WET ONLY")
        crawl_index(dir_path, f"{dir_path}/wet.paths.gz", type="wet", max_files=max_files, offset=FLAGS.offset)
        read_wet(dir_path)
        exit()
    else:
        ## Download all WARC, WET and WAT files
        crawl_index(dir_path, f"{dir_path}/warc.paths.gz", type="warc", max_files=max_files, offset=FLAGS.offset) # WARC = the raw crawl data
        crawl_index(dir_path, f"{dir_path}/wet.paths.gz", type="wet", max_files=max_files, offset=FLAGS.offset) # WET = computed metadata for the data stored in the WARC
        # crawl_index(dir_path, f"{dir_path}/wat.paths.gz", type="wat", max_files=max_files) # WAT = extracted plaintext from the data stored in the WARC
        read_warc(dir_path)
        read_wet(dir_path)

    # Iterate through the extracted WARC and WET jsons to match the records
    common_elements(dir_path) # this is inefficient but okay for a small chunks

    if delete_after: # leave only the paths files and the data after processing
        os.chdir(f"{dir_path}")
        os.system(f"rm -r warc_index")
        os.system(f"rm -r wet_index")
        os.system(f"rm -r wat_index")

if __name__ == "__main__":
    main()