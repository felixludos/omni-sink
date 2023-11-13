from pathlib import Path
import shutil, sys, os, time
from tqdm import tqdm
from tabulate import tabulate
import omnifig as fig
from functools import lru_cache
import humanize

from .database import FileDatabase
from . import misc
from .processing import recursive_mark_crawl, process_path, identify_duplicates, recursive_leaves_crawl



@fig.script('add', description='Process a given path (add hashes and meta info to database)')
def add_path_to_db(cfg: fig.Configuration):

	db_path : Path = Path(cfg.pull('db-path', misc.data_root()/'files.db'))
	chunksize : int = cfg.pull('chunksize', 1024*1024)
	db = FileDatabase(db_path, chunksize=chunksize)

	path: Path = Path(cfg.pulls('path', 'p')).absolute()

	print('Marking files for processing')
	marked_paths = []
	recursive_mark_crawl(db, marked_paths, path)

	total = len(marked_paths)

	print(f'Found {total} files to process')

	report_id = db.get_report_id(cfg.pull('description', None))

	print(f'Starting processing {path} ({total} files)')

	pbar: bool = cfg.pull('pbar', True)

	start = time.time()

	itr = tqdm(marked_paths) if pbar else marked_paths
	for mark in itr:
		if pbar:
			itr.set_description(str(mark.relative_to(path)))
		process_path(db, mark)

	end = time.time()

	print(f'Processing took {end-start:.2f} seconds')

	print(f'Done processing {path}')



@fig.script('dedupe', description='Finds and records duplicate files')
def find_path_duplicates(cfg: fig.Configuration):

	db_path : Path = Path(cfg.pull('db-path', misc.data_root()/'files.db'))
	db = FileDatabase(db_path)

	@lru_cache(maxsize=None)
	def get_size(p: Path):
		return db.find_path(p)[1][0]

	base: Path = cfg.pulls('path', 'p', default=None)
	if base is not None:
		base = Path(base).absolute()
	if base is None:
		raise NotImplementedError

	print('Finding duplicates')

	info = db.find_path(base)
	if info is None:
		raise ValueError(f'No info found in database for {base} (run `add` first)')

	base_code, (base_size, base_modtime) = info

	report_id = db.get_report_id(cfg.pull('description', None))

	pbar: bool = cfg.pull('pbar', True)

	print(f'Building duplicates')

	codes = {}
	for path, (hash_code, (size, modtime)) in db.find_all_duplicates():
		codes.setdefault(hash_code, []).append({'path': path, 'size': size, 'modtime': modtime})

	print(f'{len(codes)} hashes with more than one entry')

	duplicates, rejects = identify_duplicates(codes, pbar=tqdm if pbar else None)

	print(f'Found {len(duplicates)} duplicates ({len(rejects)} rejects)')

	hits = set()
	leaves = []

	terminals = {item['path'] for group in duplicates.values() for item in group}

	print(f'Finding leaves with {len(terminals)} distinct terminals.')

	itr = tqdm(total=base_size, unit='B', unit_scale=True, unit_divisor=1024) if pbar else None
	recursive_leaves_crawl(leaves, hits, base, terminals=terminals, pbar=itr, get_size=get_size)

	if pbar:
		itr.close()

	print(f'Found {len(hits)} hits ({len(leaves)} leaves)')

	# new_size = sum(get_size(p) for p in leaves)















