from pathlib import Path
import shutil, sys, os, time
from tqdm import tqdm
from omnibelt import save_json
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

	killpath = Path(cfg.pulls('killpath', 'out', default=misc.data_root() / 'kill.json'))

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

	pbar: bool = cfg.pull('pbar', True)

	print(f'Building duplicates')

	codes = {}
	for path, (hash_code, (size, modtime)) in db.find_all_duplicates(base):
		codes.setdefault(hash_code, []).append({'path': path, 'size': size, 'modtime': modtime})

	print(f'{len(codes)} hashes with more than one entry')

	duplicates, possible, rejects = identify_duplicates(codes, pbar=tqdm if pbar else None)

	print(f'Found {len(duplicates)} duplicates ({len(possible)} possible, {len(rejects)} rejects)')

	leaves = []

	terminals = {item['path']: code for code, group in duplicates.items() for item in group}
	terminals.update({item['path']: code for code, group in possible.items() for item in group})

	print(f'Finding leaves with {len(terminals)} distinct terminals.')

	itr = tqdm(total=base_size, unit='B', unit_scale=True, unit_divisor=1024) if pbar else None
	recursive_leaves_crawl(leaves, base, terminals=terminals, pbar=itr, get_size=get_size)

	if pbar:
		itr.close()

	print(f'Found {len(leaves)} leaves')

	new_size = 0
	cands = {}
	for leaf in leaves:
		code = terminals.get(leaf, None)
		if code not in cands:
			new_size += get_size(leaf)
		if code is not None:
			cands.setdefault(code, []).append(leaf)

	print(f'Original Size: {humanize.naturalsize(base_size)}')
	print(f'New Size: {humanize.naturalsize(new_size)}')
	print(f'Reduction: {humanize.naturalsize(base_size-new_size)} ({(base_size-new_size)/base_size*100:.2f}%)')

	# relevant = {terminals[leaf] for paths in cands.values() for leaf in paths}
	reds = [cs[0] for cs in cands.values() if len(cs) == 1]
	relevant = {terminals[path] for path in reds}
	# ambiguous = [cs for cs in sorted(cands.values(), key=lambda ps: get_size(ps[0]), reverse=True) if len(cs) > 1]
	ambiguous = [cs for cs in cands.values() if len(cs) > 1]

	print(f'Found {len(reds)} redundancies ({len(ambiguous)} ambiguous)')

	# filter out corner cases
	bad_key = '00000000000000000000000000000000'

	save_json([[str(path) for path in paths] for paths in ambiguous], killpath)
	# save_json({
	# 	'targets': [{str(path): terminals[path] for path in paths} for paths in ambiguous],
	# 	'reds': [str(path) for path in reds],
	# 	'clusters': {code: [{'path': str(info['path']), 'size': info['size'], 'modtime': info['modtime']} for info in codes[code]] for code in relevant},
	# }, killpath)

	print(f'Finds saved to {killpath}')

	return cands














