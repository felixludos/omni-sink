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



def worker_process_fn(todo_queue, lock, db_path, report_id, pbar=None, final_path=None):
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
				collector = Queue()

				contents = os.listdir(path)
				for sub in contents:
					subpath = os.path.join(path, sub)
					todo_queue.put((subpath, collector))

				infos = []
				while len(infos) != len(contents):
					subpath, info = collector.get()
					infos.append(info)

				savepath, info = db.compute_directory_info(path, infos)

			else:
				print(f"Unknown path type: {path}")
				continue

			with lock:
				db.save_file_info(savepath, info)

		else:
			savepath = path

		if pbar is not None:
			pbar.update(1)

		if final_path is not None and savepath == final_path:
			break

	print("Worker process exiting normally")



@fig.script('process', description='Process a given path (add hashes and meta info to database)')
def process(cfg: fig.Configuration):

	db_path = cfg.pull('db-path', misc.data_root()/'files.db')
	chunksize = cfg.pull('chunksize', 1024*1024)

	db = FileDatabase(db_path, chunksize=chunksize)

	num_workers = cfg.pulls('num-workers', 'w', default=cpu_count())

	use_pbar = cfg.pull('pbar', True)

	path = Path(cfg.pulls('path', 'p')).absolute()

	print('Marking files for processing')
	marked_paths = []
	recursive_marker(db, marked_paths, str(path))

	print(f'Found {len(marked_paths)} files to process.')

	report_id = db.get_report_id(cfg.pull('description', None))

	task_queue = Queue()
	lock = Lock()

	total = len(marked_paths)
	print(f'Starting processing {path} ({total} files)')

	pbar = tqdm(total=total) if use_pbar else None

	workers = [Process(target=worker_process_fn, args=(task_queue, lock, db_path, report_id, pbar))] \
		if num_workers is not None and num_workers > 0 else None

	for path in marked_paths:
		task_queue.put(path)

	if workers is None:
		worker_process_fn(task_queue, lock, db_path, report_id, pbar, final_path=path)
	else:
		for _ in range(num_workers):
			task_queue.put(None)
		for w in workers:
			w.start()

		print(f'Killing workers')

		for w in workers:
			w.join()

	print(f'Done processing {path}')




