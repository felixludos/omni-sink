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



def worker_process_fn(todo_queue, lock, db_path, report_id, pbar=None, final_path=None):
	db = FileDatabase(db_path)
	db.set_report_id(report_id)

	while True:
		path, aggregator = todo_queue.get()
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
		if aggregator is not None:
			aggregator.put((savepath, info))

		if final_path is not None and savepath == final_path:
			break

	print("Worker process exiting normally")



# @fig.script('process', description='Process a given path (add hashes and meta info to database)')
def process(cfg: fig.Configuration):

	# cfg.push('db._type', 'file-db', overwrite=False, silent=True)
	# db = cfg.pull('db')
	# db_path = db.db_path
	db_path = cfg.pull('db-path', misc.data_root()/'files.db')
	chunksize = cfg.pull('chunksize', 1024*1024)

	db = FileDatabase(db_path, chunksize=chunksize)

	num_workers = cfg.pulls('num-workers', 'w', default=cpu_count())

	use_pbar = cfg.pull('pbar', True)
	count_total = use_pbar and cfg.pull('count-total', True)

	path = Path(cfg.pulls('path', 'p'))

	total = sum(1 for _ in path.glob('**/*')) if count_total else None

	report_id = db.get_report_id(cfg.pull('description', None))

	task_queue = Queue()
	lock = Lock()

	print(f'Starting input {path}' if total is None else f'Starting input {path} ({total} files)')

	pbar = tqdm(total=total) if use_pbar else None

	workers = [Process(target=worker_process_fn, args=(task_queue, lock, db_path, report_id, pbar))] \
		if num_workers is not None and num_workers > 0 else None

	done_queue = Queue()
	task_queue.put((path, done_queue))

	if workers is None:
		worker_process_fn(task_queue, lock, db_path, report_id, pbar, final_path=path)
	else:
		for w in workers:
			w.start()
		out = done_queue.get()

		print(f'Completed work {path}, now killing workers')

		for _ in range(num_workers):
			task_queue.put((None, None))

		for w in workers:
			w.join()

	print(f'Completed input {path}')




