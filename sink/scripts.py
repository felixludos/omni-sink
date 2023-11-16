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
from .processing import recursive_mark_crawl, identify_duplicates, recursive_leaves_crawl



@fig.script('add', description='Process a given path (recursively add info to database)')
def add_path_to_db(cfg: fig.Configuration):

	db_path : Path = Path(cfg.pull('db-path', misc.data_root()/'files.db'))
	chunksize : int = cfg.pull('chunksize', 1024*1024)
	db = FileDatabase(db_path, chunksize=chunksize)

	path: Path = Path(cfg.pulls('path', 'p')).absolute()

	print('Marking items for processing')
	marked_paths = []
	recursive_mark_crawl(db, marked_paths, path)

	total = len(marked_paths)
	print(f'Found {total} items to process')

	report_id = db.get_report_id(cfg.pull('description', None))
	print(f'Starting processing {path} ({total} items) with report-id {report_id}')

	pbar: bool = cfg.pull('pbar', True)

	start = time.time()

	itr = tqdm(marked_paths) if pbar else marked_paths
	for mark in itr:
		if pbar:
			itr.set_description(str(mark.relative_to(path)))

		# info = db.find_path(path)

		if path.is_file():
			savepath, info = db.process_file(path)

		elif path.is_dir():
			savepath, info = db.process_dir(path)

		else:
			raise ValueError(f"Unknown path type: {path}")

		db.save_file_info(savepath, info)

	end = time.time()

	print(f'Processing took {end-start:.2f} seconds')

	print(f'Done processing {path}')



@fig.script('dedupe', description='Finds and record duplicates items')
def find_path_duplicates(cfg: fig.Configuration):

	candidates_path = Path(cfg.pulls('candidate-path', 'out', default=misc.data_root() / 'candidates.json'))

	db_path : Path = Path(cfg.pull('db-path', misc.data_root()/'files.db'))
	db = FileDatabase(db_path)

	@lru_cache(maxsize=None)
	def get_size(p: Path):
		return db.find_path(p)[1][2]

	base: Path = cfg.pulls('path', 'p', default=None)
	if base is not None:
		base = Path(base).absolute()
	if base is None:
		raise NotImplementedError

	print('Finding duplicates')

	info = db.find_path(base)
	if info is None:
		raise ValueError(f'No info found in database for {base} (run `add` first)')

	base_code, (base_isdir, base_count, base_size, base_modtime) = info

	print(f'Will find duplicates of {base}')
	print(tabulate([['Size', humanize.naturalsize(base_size)],
					['Count', humanize.intword(base_count)]]))

	pbar: bool = cfg.pull('pbar', True)
	use_bytes: bool = cfg.pull('use-bytes', False)
	@lru_cache(maxsize=None)
	def get_increment(p: Path):
		return db.find_path(p)[1][2 if use_bytes else 1]

	print(f'Identifying duplicates')

	codes = {}
	for path, (hash_code, (isdir, count, size, modtime)) in db.find_all_duplicates(base):
		codes.setdefault(hash_code, []).append({'path': path, 'size': size, 'modtime': modtime,
												'isdir': isdir, 'count': count})

	print(f'{len(codes)} hashes with more than one entry')

	duplicates, possible, rejects = identify_duplicates(codes, pbar=tqdm if pbar else None)

	print(f'Found {len(duplicates)} duplicates ({len(possible)} possible, {len(rejects)} rejects)')

	leaves = []

	terminals = {item['path']: code for code, group in duplicates.items() for item in group}
	terminals.update({item['path']: code for code, group in possible.items() for item in group})

	print(f'Finding leaves with {len(terminals)} distinct terminals.')

	pbar_args = {'total': base_size, 'unit': 'B', 'unit_scale': True, 'unit_divisor': 1024} if use_bytes \
		else {'total': len(terminals), 'unit': 'item'}
	itr = tqdm(total=base_size, **pbar_args) if pbar else None
	recursive_leaves_crawl(leaves, base, terminals=terminals, pbar=itr, get_increment=get_increment)

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
	# used_dupes = [cs[0] for cs in cands.values() if len(cs) == 1]
	# relevant = {terminals[path] for path in reds}
	# ambiguous = [cs for cs in sorted(cands.values(), key=lambda ps: get_size(ps[0]), reverse=True) if len(cs) > 1]
	groups = [group for group in cands.values() if len(group) > 1]

	print(f'Found {len(groups)} candidate groups of duplicates.')

	# filter out corner cases
	# bad_key = '00000000000000000000000000000000'

	if candidates_path is not None:
		save_json([[str(path) for path in group] for group in groups], candidates_path)
		# save_json({
		# 	'targets': [{str(path): terminals[path] for path in paths} for paths in ambiguous],
		# 	'reds': [str(path) for path in reds],
		# 	'clusters': {code: [{'path': str(info['path']), 'size': info['size'], 'modtime': info['modtime']}
		# 	for info in codes[code]] for code in relevant},
		# }, killpath)

		print(f'Candidate duplicates saved to {candidates_path}')

	return groups



# @fig.script('quarantine')
# def quarantine_targets(cfg: fig.Configuration):
# 	pass










