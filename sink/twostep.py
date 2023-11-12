import os.path
from pathlib import Path
import time
from tqdm import tqdm
from contextlib import nullcontext
from multiprocessing import Manager, Queue, Lock
import sys
import sqlite3
from multiprocessing import cpu_count, Pipe, Process
# from pathos.multiprocessing import ProcessingPool as Pool
import multiprocessing
import dill
import multiprocessing as mp
import time

import time
import omnifig as fig
import asyncio

from . import misc
from .database import FileDatabase



def recursive_marker(db: FileDatabase, marked_paths, path, pbar=None):
	if not db.exists(path):
		if os.path.isdir(path):
			for sub in os.listdir(path):
				subpath = os.path.join(path, sub)
				recursive_marker(db, marked_paths, subpath)

		marked_paths.append(path)

		if pbar is not None:
			pbar.update(1)


def recursive_collect_dupes(db: FileDatabase, duplicates, path, pbar=None):
	if pbar is not None:
		pbar.set_description(path)

	base_hash, (base_size, base_modified) = db.find_path(path)

	dupes = {other: {'size': size, 'modified': modified}
			 for other, (size, modified) in db.find_duplicates(path, base_hash)}

	if len(dupes):
		duplicates[path] = {'dupes': dupes, 'size': base_size, 'modified': base_modified}

	elif os.path.isdir(path):
		for sub in os.listdir(path):
			subpath = os.path.join(path, sub)
			recursive_collect_dupes(db, duplicates, subpath, pbar=pbar)



def worker_process_fn(todo_queue, lock, db_path, report_id, pbar=None):
	db = FileDatabase(db_path)
	db.set_report_id(report_id)

	while True:
		path = todo_queue.get()
		if path is None:
			break

		info = db.find_path(path)

		if info is None:
			if os.path.isfile(path):
				savepath, info = db.process_file(path)

			elif os.path.isdir(path):
				savepath, info = db.process_dir(path)

			else:
				raise ValueError(f"Unknown path type: {path}")

			with lock or nullcontext():
				db.save_file_info(savepath, info)

		if pbar is not None:
			pbar.update(1)

	print("Worker process exiting normally")



def simple_worker_fn(path, lock, db_path, report_id):
	db = FileDatabase(db_path)
	db.set_report_id(report_id)

	info = db.find_path(path)

	if info is None:
		if os.path.isfile(path):
			savepath, info = db.process_file(path)

		elif os.path.isdir(path):
			savepath, info = db.process_dir(path)

		else:
			raise ValueError(f"Unknown path type: {path}")

		# with lock or nullcontext():
		db.save_file_info(savepath, info)



@fig.script('process', description='Process a given path (add hashes and meta info to database)')
def process(cfg: fig.Configuration):

	db_path = cfg.pull('db-path', misc.data_root()/'files.db')
	chunksize = cfg.pull('chunksize', 1024*1024)

	db = FileDatabase(db_path, chunksize=chunksize)

	# num_workers = cfg.pulls('num-workers', 'w', default=cpu_count())
	path = Path(cfg.pulls('path', 'p')).absolute()

	print('Marking files for processing')
	marked_paths = []
	recursive_marker(db, marked_paths, str(path))

	total = len(marked_paths)
	# print(f'Found {total} files to process using {num_workers} workers')
	print(f'Found {total} files to process')

	report_id = db.get_report_id(cfg.pull('description', None))

	lock = None
	print(f'Starting processing {path} ({total} files)')

	pbar = cfg.pull('pbar', True)

	start = time.time()

	# if num_workers == 0:
	itr = tqdm(marked_paths) if pbar else marked_paths
	for mark in itr:
		if pbar:
			itr.set_description(str(Path(mark).relative_to(path)))
		simple_worker_fn(mark, lock, db_path, report_id)
	# else:
	# 	with mp.Manager() as manager:
	# 		todo_queue = manager.Queue()
	# 		lock = manager.Lock()
	#
	# 		workers = [Process(target=worker_process_fn, args=(todo_queue, lock, db_path, report_id, pbar))
	# 				   for _ in range(num_workers)]
	#
	# 		for worker in workers:
	# 			worker.start()
	#
	# 		for mark in marked_paths:
	# 			todo_queue.put(mark)
	#
	# 		for _ in range(num_workers):
	# 			todo_queue.put(None)
	#
	# 		for worker in workers:
	# 			worker.join()
	#
	# 		# pool = manager.Pool(num_workers)
	# 		# pool.starmap(simple_worker_fn, [(mark, lock, db_path, report_id, pbar) for mark in marked_paths])
	# 		# pool.close()

	end = time.time()

	print(f'Processing took {end-start:.2f} seconds')

	print(f'Done processing {path}')




