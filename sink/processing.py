
import os
from tqdm import tqdm
import hashlib
from multiprocessing import Manager
import sys
import sqlite3
from multiprocessing import Pool, cpu_count
import time
import omnifig as fig

from .database import FileDatabase




def process_and_save_file(db: FileDatabase, file_path, lock, pbar):
	file_path, metadata, file_hash = db.process_file(file_path)
	with lock:
		db.save_file_info(file_path, metadata, file_hash)
		if pbar is not None:
			pbar.update(1)  # Update the progress bar
	return 1



def process_directory(directory_path, db, use_pbar=True, num_workers=cpu_count()):
	file_paths = [os.path.join(root, file) for root, _, files in os.walk(directory_path) for file in files]
	manager = Manager()
	lock = manager.Lock()

	pbar = tqdm(total=len(file_paths)) if use_pbar else None
	if num_workers is not None and num_workers > 0:
		with Pool(processes=num_workers) as pool:
			args = [(db, file_path, lock, pbar) for file_path in file_paths]  # Pass pbar to the worker functions
			results = pool.starmap(process_and_save_file, args)
	else:
		results = [process_and_save_file(db, file_path, lock, pbar) for file_path in file_paths]

	return results



def find_duplicates(self, directory_root=None):
	conn = sqlite3.connect(self.db_path)
	cursor = conn.cursor()

	# Modify the query based on whether a directory root is provided
	if directory_root:
		query = 'SELECT hash, path FROM files WHERE status="completed" AND path LIKE ?'
		cursor.execute(query, (f"{directory_root}%",))
	else:
		query = 'SELECT hash, path FROM files WHERE status="completed"'
		cursor.execute(query)

	files = cursor.fetchall()

	hashes = [file[0] for file in files]
	duplicate_hashes = set([h for h in hashes if hashes.count(h) > 1])

	duplicate_files = [file[1] for file in files if file[0] in duplicate_hashes]

	conn.close()
	return duplicate_files



####################################################################################################

# def process_and_save_file(self, args):
# 	file_path, lock, pbar = args  # Modify this line to receive pbar
# 	info = self.process_file(file_path)
# 	file_path, file_info = info
#
# 	with lock:
# 		self.save_file_info(file_path, file_info['metadata'], file_info['hash'], "completed")
# 		pbar.update(1)  # Update the progress bar
#
# 	return info
#
#
# def process_directory(worker, directory_path):
# 	file_paths = [os.path.join(root, file) for root, _, files in os.walk(directory_path) for file in files]
# 	manager = Manager()
# 	lock = manager.Lock()
#
# 	with Pool(processes=cpu_count()) as pool, tqdm(total=len(file_paths)) as pbar:  # Add tqdm progress bar here
# 		args = [(file_path, lock, pbar) for file_path in file_paths]  # Pass pbar to the worker functions
# 		results = pool.map(worker.process_and_save_file, args)
#
# 	print(f"Completed processing {len(file_paths)} files.", flush=True)
# 	return {file_path: info for file_path, info in results}
#
#
#
#
# class FileProcessor:
# 	def __init__(self, db_path):
# 		self.db_path = db_path
# 		self.init_database()
#
# 	def init_database(self):
# 		conn = sqlite3.connect(self.db_path)
# 		cursor = conn.cursor()
# 		cursor.execute('''
# 			CREATE TABLE IF NOT EXISTS files (
# 				path TEXT PRIMARY KEY,
# 				size INTEGER,
# 				modification_time REAL,
# 				hash TEXT,
# 				status TEXT
# 			)
# 		''')
# 		conn.commit()
# 		conn.close()
#
# 	def get_metadata(self, file_path):
# 		return {
# 			"size": os.path.getsize(file_path),
# 			"modification_time": os.path.getmtime(file_path)
# 		}
#
# 	def compute_hash(self, file_path, chunk_size=1024*1024):
# 		hasher = hashlib.sha256()
# 		with open(file_path, 'rb') as f:
# 			while True:
# 				data = f.read(chunk_size)
# 				if not data:
# 					break
# 				hasher.update(data)
# 		return hasher.hexdigest()
#
# 	def process_file(self, file_path):
# 		metadata = self.get_metadata(file_path)
# 		file_hash = self.compute_hash(file_path)
# 		return file_path, {"metadata": metadata, "hash": file_hash}
#
# 	def save_file_info(self, file_path, metadata, file_hash, status):
# 		conn = sqlite3.connect(self.db_path)
# 		cursor = conn.cursor()
# 		cursor.execute('''
# 			INSERT OR REPLACE INTO files (path, size, modification_time, hash, status)
# 			VALUES (?, ?, ?, ?, ?)
# 		''', (file_path, metadata['size'], metadata['modification_time'], file_hash, status))
# 		conn.commit()
# 		conn.close()
#
# 	def process_and_save_file(self, args):
# 		file_path, lock, pbar = args  # Modify this line to receive pbar
# 		info = self.process_file(file_path)
# 		file_path, file_info = info
#
# 		with lock:
# 			self.save_file_info(file_path, file_info['metadata'], file_info['hash'], "completed")
# 			pbar.update(1)  # Update the progress bar
#
# 		return info
#
# 	def process_directory(self, directory_path):
# 		file_paths = [os.path.join(root, file) for root, _, files in os.walk(directory_path) for file in files]
# 		manager = Manager()
# 		lock = manager.Lock()
#
# 		with Pool(processes=cpu_count()) as pool, tqdm(total=len(file_paths)) as pbar:  # Add tqdm progress bar here
# 			args = [(file_path, lock, pbar) for file_path in file_paths]  # Pass pbar to the worker functions
# 			results = pool.map(self.process_and_save_file, args)
#
# 		print(f"Completed processing {len(file_paths)} files.", flush=True)
# 		return {file_path: info for file_path, info in results}


	# def find_duplicates(self):
	# 	conn = sqlite3.connect(self.db_path)
	# 	cursor = conn.cursor()
	#
	# 	cursor.execute('SELECT hash, path FROM files WHERE status="completed"')
	# 	files = cursor.fetchall()
	#
	# 	hashes = [file[0] for file in files]
	# 	duplicate_hashes = set([h for h in hashes if hashes.count(h) > 1])
	#
	# 	duplicate_files = [file[1] for file in files if file[0] in duplicate_hashes]
	#
	# 	conn.close()
	# 	return duplicate_files




