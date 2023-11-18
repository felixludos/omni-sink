from pathlib import Path
import shutil, sys, os, time
from tqdm import tqdm
import textwrap
from omnibelt import save_json, load_json
from tabulate import tabulate
from datetime import datetime, timedelta
import omnifig as fig
from functools import lru_cache
import humanize

from .database import FileDatabase
from . import misc
from .processing import recursive_mark_crawl, identify_duplicates, recursive_leaves_crawl, PathOrdering



@fig.script('add', description='Process a given path (recursively add info to database)')
def add_path_to_db(cfg: fig.Configuration):

	db_path : Path = Path(cfg.pull('db-path', misc.data_root()/'files.db'))
	chunksize : int = cfg.pull('chunksize', 1024*1024)
	db = FileDatabase(db_path, chunksize=chunksize)

	ignore_path_names = cfg.pull('ignore-path-names', ['omni-sink-quarantine', '$RECYCLE.BIN'])
	ignore_path_names = set(ignore_path_names)
	report_description = cfg.pull('description', None)

	pbar: bool = cfg.pull('pbar', True)

	base_path: Path = Path(cfg.pulls('path', 'p')).absolute()

	marked_paths = []
	skipped_paths = []
	itr = tqdm(desc='Marking items') if pbar else None
	recursive_mark_crawl(db, marked_paths, skipped_paths, base_path, ignore_names=ignore_path_names, pbar=itr)
	if pbar:
		itr.close()
		time.sleep(0.05) # to allow the previous tqdm to close

	print(f'Skipped {len(skipped_paths)} items due to permission errors.')
	if len(skipped_paths):
		print(tabulate([[str(path)] for path in skipped_paths], headers=['Skipped Paths']))
		print()

	total = len(marked_paths)
	print(f'Found {total} items to process')

	report_id = db.get_report_id(report_description)
	print(f'Starting processing {base_path} ({total} items) with report-id {report_id}')

	start = time.time()

	itr = marked_paths
	if pbar:
		itr = tqdm(marked_paths)
		itr.reset()
	for mark in itr:
		if pbar:
			itr.set_description(str(mark.relative_to(base_path)))

		if mark.is_file():
			savepath, info = db.process_file(mark)

		elif mark.is_dir():
			savepath, info = db.process_dir(mark)

		else:
			raise ValueError(f"Unknown path type: {mark}")

		db.save_file_info(savepath, info)

	end = time.time()

	print(f'Processing took {humanize.precisedelta(timedelta(seconds=end-start))}')

	print(f'Done processing {base_path}')



@fig.script('dedupe', description='Finds and record duplicates items')
def find_path_duplicates(cfg: fig.Configuration):

	candidates_path = Path(cfg.pulls('candidate-path', 'out', default=misc.data_root() / 'candidates.json'))

	db_path : Path = Path(cfg.pull('db-path', misc.data_root()/'files.db'))
	db = FileDatabase(db_path)

	# @lru_cache(maxsize=None)
	# def get_size(p: Path):
	# 	return db.find_path(p)[1][2]

	base_path: Path = cfg.pulls('path', 'p', default=None)
	if base_path is not None:
		base_path = Path(base_path).absolute()
	if base_path is None:
		raise NotImplementedError

	pbar: bool = cfg.pull('pbar', True)
	use_bytes: bool = cfg.pull('use-bytes', True)

	# print('Finding duplicates')

	base = db.find_path(base_path)
	if base is None:
		raise ValueError(f'No info found in database for {base_path} (run `add` first)')

	# base_code, (base_isdir, base_count, base_size, base_modtime) = info

	print(f'Will find duplicates of {base.path}')
	print(tabulate([['Size', humanize.naturalsize(base.size)],
					['Count', humanize.intcomma(base.count)]]))

	@lru_cache(maxsize=None)
	def get_increment(path: Path):
		item = db.find_path(path)
		return item.size if use_bytes else item.count

	print(f'Collecting all groups of items with identical hashes within {base.path} (this may take a while)')

	start = time.time()

	codes = {}
	for item in db.find_all_duplicates(base.path):
		codes.setdefault(item.code, []).append(item)

	print(f'{len(codes)} hashes with more than one entry')

	duplicates, possible, rejects = identify_duplicates(codes, pbar=tqdm if pbar else None)

	print(f'Found {humanize.intcomma(len(duplicates))} duplicate items '
		  f'({humanize.intcomma(len(possible))} possible, {humanize.intcomma(len(rejects))} rejects)')

	leaves = []

	terminals = {item.code: code for code, group in duplicates.items() for item in group}
	terminals.update({item.path: code for code, group in possible.items() for item in group})

	print(f'Finding leaves with {humanize.intcomma(len(terminals))} distinct terminals.')

	pbar_args = {'total': base.size, 'unit': 'B', 'unit_scale': True, 'unit_divisor': 1024} \
		if use_bytes else {'total': base.count, 'unit': 'item'}
	itr = tqdm(**pbar_args) if pbar else None
	recursive_leaves_crawl(leaves, base.path, terminals=terminals, pbar=itr, get_increment=get_increment)
	if pbar: itr.close()

	print(f'Found {len(leaves)} leaves')

	new_size = 0
	cands = {}
	for path in leaves:
		code = terminals.get(path, None)
		if code not in cands:
			new_size += db.find_path(path).size
		if code is not None:
			cands.setdefault(code, []).append(path)

	end = time.time()
	print(f'Processing took {humanize.precisedelta(timedelta(seconds=end-start))}')

	print(f'Original Size: {humanize.naturalsize(base.size)}')
	print(f'New Size: {humanize.naturalsize(new_size)}')
	print(f'Reduction: {humanize.naturalsize(base.size-new_size)} ({(base.size-new_size)/base.size*100:.2f}%)')

	# relevant = {terminals[leaf] for paths in cands.values() for leaf in paths}
	# used_dupes = [cs[0] for cs in cands.values() if len(cs) == 1]
	# relevant = {terminals[path] for path in reds}
	# ambiguous = [cs for cs in sorted(cands.values(), key=lambda ps: get_size(ps[0]), reverse=True) if len(cs) > 1]
	groups = [group for group in cands.values() if len(group) > 1]

	print(f'Found {len(groups)} candidate groups of duplicates.')

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



@fig.script('quarantine')
def quarantine_targets(cfg: fig.Configuration):

	candidates_path = Path(cfg.pulls('candidate-path', 'in', default=misc.data_root() / 'candidates.json'))
	quarantine_root = cfg.pulls('quarantine-root', 'out', default=None)
	if quarantine_root is None:
		base_path = cfg.pulls('path', 'p', default=None, silent=True)
		if base_path is not None:
			quarantine_root = Path(base_path).absolute() / 'omni-sink-quarantine'
		else:
			raise ValueError('Must provide either `quarantine-root` or `path`')

	db_path : Path = Path(cfg.pull('db-path', misc.data_root()/'files.db'))
	db = FileDatabase(db_path)

	pbar: bool = cfg.pull('pbar', True)
	show_top = cfg.pull('show-top', 10)
	auto_confirm = cfg.pull('auto-confirm', False)

	groups = load_json(candidates_path)
	groups = [[Path(path) for path in group] for group in groups]

	print(f'Preparing {len(groups)} candidate groups of duplicates.')

	cfg.push('sorter._type', 'default-ordering', overwrite=False, silent=True)
	sorter: PathOrdering = cfg.pull('sorter')

	for group in tqdm(groups, 'Identifying Targets') if pbar else groups:
		sorter.inplace(group, get_info=db.find_path)
	groups.sort(key=lambda group: db.find_path(group[0]).size, reverse=True)
	kill_list = [target for group in groups for target in group[1:]]
	kill_size = sum(db.find_path(path).size for path in kill_list)
	base_path = Path(os.path.commonpath([str(path) for path in kill_list]))

	print()
	print(f'Found {humanize.intcomma(len(kill_list))} items to quarantine. '
		  f'Total size: {humanize.naturalsize(kill_size)}')

	fixed = {}
	reverse_fixed = {}
	for path in kill_list:
		name = path.name
		i = 1
		while name in fixed:
			name = f'{path.stem} ({i}){path.suffix}'
			i += 1
		fixed[name] = path
		reverse_fixed[path] = name

	if show_top is not None:
		print()
		print(f'{"Largest" if len(groups) > show_top else "All"} {min(show_top, len(groups))} items')
		print(tabulate([[humanize.naturalsize(db.find_path(group[0]).size),
						 len(group),
						 '\n'.join(reverse_fixed.get(path, '-') for path in group),
						 '\n'.join(str(path) for path in group)
						 ] for group in groups[:show_top]],
					   headers=['Size', 'Occ.', 'Quarantined Name', 'Original Path']))
		if len(groups) > show_top:
			print(f'--- and {len(groups) - show_top} more ---')

	quarantine_dir = quarantine_root / 'content'

	if not auto_confirm:
		print()
		print(f'Quarantining from {base_path} to {quarantine_dir}')
		while True:
			confirm = input('Confirm? [y/n] ').lower()
			if confirm in ['y', 'yes']:
				break
			elif confirm in ['n', 'no']:
				print('Quarantine aborted.')
				return
			else:
				print('Invalid input.')

	quarantine_dir.mkdir(parents=True, exist_ok=True)
	save_json({
		'base-path': str(base_path),
		'timestamp': datetime.now().isoformat(),
		'quarantine': {str(fixed): str(path) for fixed, path in fixed.items()},
		'groups': [[str(path) for path in group] for group in groups],
	}, quarantine_root / 'info.json')

	print(f'Quarantining {len(kill_list)} items to {quarantine_dir}')
	for path in tqdm(kill_list, 'Quarantining') if pbar else kill_list:
		dest = quarantine_dir / reverse_fixed[path]
		shutil.move(str(path), str(dest))

	return fixed










