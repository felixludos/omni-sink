import os.path
from pathlib import Path
from tqdm import tqdm
from multiprocessing import Manager, Queue, Lock
import sys
import sqlite3
from multiprocessing import Pool, cpu_count, Pipe, Process
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



# def worker_process_fn(todo_queue, lock, db_path, report_id, pbar=None):
# 	db = FileDatabase(db_path)
# 	db.set_report_id(report_id)
#
# 	while True:
# 		path = todo_queue.get()
# 		if path is None:
# 			break
#
# 		info = db.find_path(path)
#
# 		if info is None:
# 			if os.path.isfile(path):
# 				savepath, info = db.process_file(path)
#
# 			elif os.path.isdir(path):
# 				savepath, info = db.process_dir(path)
#
# 			else:
# 				print(f"Unknown path type: {path}")
# 				continue
#
# 			with lock:
# 				db.save_file_info(savepath, info)
#
# 		if pbar is not None:
# 			pbar.update(1)
#
# 	print("Worker process exiting normally")



def simple_worker_fn(path, lock, db_path, report_id, pbar=None):
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

		with lock:
			db.save_file_info(savepath, info)

	if pbar is not None:
		pbar.update(1)

	# print("Worker process exiting normally")



@fig.script('process', description='Process a given path (add hashes and meta info to database)')
def process(cfg: fig.Configuration):

	db_path = cfg.pull('db-path', misc.data_root()/'files.db')
	chunksize = cfg.pull('chunksize', 1024*1024)

	db = FileDatabase(db_path, chunksize=chunksize)

	num_workers = cfg.pulls('num-workers', 'w', default=cpu_count())
	path = Path(cfg.pulls('path', 'p')).absolute()

	print('Marking files for processing')
	marked_paths = []
	recursive_marker(db, marked_paths, str(path))

	total = len(marked_paths)
	print(f'Found {total} files to process using {num_workers} workers')

	report_id = db.get_report_id(cfg.pull('description', None))

	# task_queue = Queue()
	lock = Lock()
	print(f'Starting processing {path} ({total} files)')

	pbar = tqdm(total=total) if cfg.pull('pbar', True) else None

	if num_workers == 0:
		for mark in marked_paths:
			simple_worker_fn(mark, lock, db_path, report_id, pbar)
	else:
		with Pool(num_workers) as p:
			p.starmap(simple_worker_fn, [(mark, lock, db_path, report_id, pbar) for mark in marked_paths])

	print(f'Done processing {path}')




