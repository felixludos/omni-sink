

from pathlib import Path
import shutil, sys, os
from tqdm import tqdm
from tabulate import tabulate
import omnifig as fig
from multiprocessing import Pool, cpu_count

from . import misc
from .checking import compute_file_checksum, compute_directory_checksum




def worker(files):
	checksums = {}
	for file_path in files:
		checksum = compute_file_checksum(file_path)
		if checksum in checksums:
			checksums[checksum].append(file_path)
		else:
			checksums[checksum] = [file_path]
	return checksums


# @fig.script('kount')
# def kount(cfg: fig.Configuration):
# 	pass


# # Testing the top-level script
# processor = FileProcessor("/mnt/data/files.db")
# directory_info = processor.process_directory("/mnt/data/sample_dir")
# get_all_file_info("/mnt/data/files.db")  # Should include info of all processed files






