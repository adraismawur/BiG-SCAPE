"""Class to support file processing for MIBIG files

Author: Arjan Draisma
"""

import io
import os
import sys
import zipfile
import gzip
import tarfile
import urllib.request
from glob import glob


import src.gbk as gbk
import src.big_scape as big_scape

def scan_mibig_files(run: big_scape.Run):
    """Checks if the mibig files currently exist in the directory specified in the command line
    arugments

    inputs:
        run - The run details object relevant to this run
    """
    base_url = "https://dl.secondarymetabolites.org/mibig/mibig_gbk_"
    if run.options.mibig21:
        file_name = "MIBiG_2.1_final"
    elif run.options.mibig14:
        file_name = "MIBiG_1.4_final"
    else:
        file_name = "MIBiG_1.3_final"

    mibig_path = run.options.mibig_path

    full_path = os.path.join(mibig_path, file_name)

    # TODO: check if all files actually exist
    return os.path.exists(full_path)

        


def download_mibig(run: big_scape.Run):
    """Downloads the mibig files specified in command line parameters to the mibig folder specified
    in the command line parameters
    
    inputs:
        run - The run details object relevant to this run
    """
    # return if we're not using mibig, just so we don't waste time on downloading in strange cases
    if not run.mibig.use_mibig:
        return
    
    # double check if the files are already there
    # return early if so
    if scan_mibig_files(run):
        return

    base_url = "https://dl.secondarymetabolites.org/mibig/mibig_gbk_"
    if run.options.mibig21:
        # TODO: download snapshot of mibig
        # TODO: download gbks from snapshot
        # TODO: rename this option to snapshot
        print("\n\n\nCannot download mibig 2.1 files. these should have been included in the \
        BiG-SCAPE repository")
    elif run.options.mibig14:
        file_name = "MIBiG_1.4_final"
        mibig_url = base_url + "1.4.tar.gz"
    else:
        file_name = "MIBiG_1.3_final"
        mibig_url = base_url + "1.3.tar.gz"

    mibig_path = run.options.mibig_path

    full_path = os.path.join(mibig_path, file_name)

    # mibig can be downloaded as gzip, so let's see if we can extract while downloading
    with urllib.request.urlopen(mibig_url) as response:
        with gzip.GzipFile(fileobj=response) as uncompressed:
            tar_file = uncompressed.read()
    
            tar = tarfile.open(fileobj=io.BytesIO(tar_file))

            # some files should be excluded, we do this here
            files = tar.getmembers()
            extract_files = []
            for file in files:
                # skip anything with a dot at the start
                if file.name.startswith("."):
                    continue

                extract_files.append(file)

            tar.extractall(full_path, extract_files)


def extract_mibig(run: big_scape.Run):
    """Extracts MIBiG zips

    Inputs:
    - run: parameters relevant to the current run
    - verbose: whether to report code execution verbosely
    """
    # Read included MIBiG
    # TODO: automatically download new versions

    print("\n Trying to read bundled MIBiG BGCs as reference")
    print("Assuming mibig path: {}".format(run.options.mibig_path))

    # try to see if the zip file has already been decompressed
    numbgcs = len(glob(os.path.join(run.mibig.gbk_path, "*.gbk")))
    if numbgcs == 0:
        if not zipfile.is_zipfile(run.mibig.zip_path):
            sys.exit("Did not find file {}. \
                Please re-download it from the official repository".format(run.mibig.zip_path))

        with zipfile.ZipFile(run.mibig.zip_path, 'r') as mibig_zip:
            for fname in mibig_zip.namelist():
                if fname[-3:] != "gbk":
                    continue

                extractedbgc = mibig_zip.extract(fname, path=run.options.mibig_path)
                if run.options.verbose:
                    print("  Extracted {}".format(extractedbgc))

    elif run.mibig.expected_num_bgc == numbgcs:
        print("  MIBiG BGCs seem to have been extracted already")
    else:
        sys.exit("Did not find the correct number of MIBiG BGCs ({}). \
            Please clean the 'Annotated MIBiG reference' folder from any \
            .gbk files first".format(run.mibig.expected_num_bgc))
