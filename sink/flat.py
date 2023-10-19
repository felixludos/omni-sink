import os.path
from multiprocessing import Manager
import sys
import sqlite3
from multiprocessing import Pool, cpu_count
import time
import omnifig as fig
import asyncio

from .database import FileDatabase


async def worker_process_fn(path, lock, db: FileDatabase, pbar=None):
	if os.path.isfile(path):
		savepath, info = db.process_file(path)
		with lock:
			db.save_file_info(savepath, info)
			if pbar is not None:
				pbar.update(1)
		return

	for subpath in os.listdir(path):
		subpath = os.path.join(path, subpath)
		if os.path.isdir(subpath):
			await worker_process_fn(subpath, lock, db, pbar=pbar)
		else:
			savepath, info = db.process_file(subpath)
			with lock:
				db.save_file_info(savepath, info)
				if pbar is not None:
					pbar.update(1)

	infos = db.process_directory(path)
	with lock:
		for savepath, info in infos.items():
			db.save_file_info(savepath, info)
			if pbar is not None:
				pbar.update(1)

	pass
