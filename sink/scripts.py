

from pathlib import Path
import shutil, sys, os, time
from tqdm import tqdm
# from tabulate import tabulate
import omnifig as fig

from .database import FileDatabase
from . import misc
from .processing import recursive_marker, process_path



@fig.script('add', description='Process a given path (add hashes and meta info to database)')
def collect_files(cfg: fig.Configuration):

	db_path = cfg.pull('db-path', misc.data_root()/'files.db')
	chunksize = cfg.pull('chunksize', 1024*1024)

	db = FileDatabase(db_path, chunksize=chunksize)

	path = Path(cfg.pulls('path', 'p')).absolute()

	print('Marking files for processing')
	marked_paths = []
	recursive_marker(db, marked_paths, str(path))

	total = len(marked_paths)

	print(f'Found {total} files to process')

	report_id = db.get_report_id(cfg.pull('description', None))

	print(f'Starting processing {path} ({total} files)')

	pbar = cfg.pull('pbar', True)

	start = time.time()

	itr = tqdm(marked_paths) if pbar else marked_paths
	for mark in itr:
		if pbar:
			itr.set_description(str(Path(mark).relative_to(path)))
		process_path(db, mark)

	end = time.time()

	print(f'Processing took {end-start:.2f} seconds')

	print(f'Done processing {path}')








