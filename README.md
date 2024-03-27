# How To Download a Small English Slice From the Common Crawl
Code to extract a small amount of ENGLISH Common Crawl data into json files with the corresponding URLS and IDs.

Developed by Sean McLeish (University of Maryland)

NOTE: This code is designed for extracting a small amount of data to play with, not downloading and processing entire crawls.

## Step 1:
Go the the [Common Crawl Blog](https://commoncrawl.org/blog) and pick your favourite common crawl, the examples give are for [November/December 2023](https://commoncrawl.org/blog/november-december-2023-crawl-archive-now-available).

Click on the associated links for the `warc.paths.gz`, `wat.paths.gz` and `wet.paths.gz`. This will download them for you, then you need to place them in the folder you plan on developing in.

## Step 2:
Install warcio for reading the files: `$ pip install warcio==1.7.4`. I developed in Python 3.10.4

## Step 3:
Run `$ python download.py --path <YOUR PATH> --max_files <NUMBER OF FILES>`

### Options:
1. `--warc`: only process the warc files
2. `--wet`: only process the wet files
3. `--delete_after`: automatically deletes the temporary files after
4. `--path` (REQUIRED): path to the directory of the `warc.paths.gz` files
5. `--max_files` (REQUIRED): maximum number of files to download
6. `--offset`: sample files with this offset from the interval, e.g. if `offset=1` instead of sampling file 0, 100, 200, ... we sample 1, 101, 201, ...

## Step 4:
You now have two files `warc_json_data` and `wet_json_data` containing a small sample of common crawl data to play with.

## Step 5:
`combined_json_data` merges `warc_json_data` and `wet_json_data`, taking only common files from both to create a combined set of json files.

# Contributing
Please open pull requests and issues to add features or ask questions.

### References
Alot of this work is based off of this [blog post](https://skeptric.com/notebooks/WAT%20WET%20WARC%20-%20Common%20Crawl%20Archives.html).
<!-- https://skeptric.com/notebooks/WAT%20WET%20WARC%20-%20Common%20Crawl%20Archives.html -->