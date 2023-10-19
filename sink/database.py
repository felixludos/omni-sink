import os
import hashlib
from multiprocessing import Manager, Pool, cpu_count
import sys
import sqlite3
import omnifig as fig

from . import misc



@fig.component('file-db')
class FileDatabase(fig.Configurable):
	def __init__(self, db_path=misc.data_root()/'files.db', chunksize=1024*1024):
		self.db_path = db_path
		self.chunksize = chunksize
		self.init_database()
		# self.conn = sqlite3.connect(self.db_path)


	def init_database(self):
		conn = sqlite3.connect(self.db_path)
		cursor = conn.cursor()
		cursor.execute('''
			CREATE TABLE IF NOT EXISTS files (
				path TEXT PRIMARY KEY,
				status TEXT
				hash TEXT,
				filesize INTEGER,
				modification_time REAL,
			)
		''')
		conn.commit()
		conn.close()


	def compute_hash(self, file_path):
		hasher = hashlib.sha256()
		chunk_size = self.chunksize
		with open(file_path, 'rb') as f:
			while True:
				data = f.read(chunk_size)
				if not data:
					break
				hasher.update(data)
		return hasher.hexdigest()


	def compute_directory_hash(self, content_hashes: list[str]):
		combined_checksums = ''.join(content_hashes).encode()
		return hashlib.sha256(combined_checksums).hexdigest()


	def compute_directory_info(self, dir_path, content_info):
		metadatas, hashes = zip(*content_info)
		filesizes, modification_times = zip(*metadatas)
		directory_hash = self.compute_directory_hash(hashes)
		dirsize = sum(filesizes)
		modification_time = os.path.getmtime(dir_path)
		metadata = dirsize, modification_time
		return dir_path, (directory_hash, metadata)


	def process_file(self, file_path):
		size, modification_time = os.path.getsize(file_path), os.path.getmtime(file_path)
		metadata = size, modification_time
		file_hash = self.compute_hash(file_path)
		return file_path, (file_hash, metadata)


	def save_file_info(self, file_path, file_info, status='completed'):
		conn = sqlite3.connect(self.db_path)
		cursor = conn.cursor()

		file_hash, metadata = file_info

		cursor.execute('''
			INSERT OR REPLACE INTO files (path, status, hash, filesize, modification_time)
			VALUES (?, ?, ?, ?, ?)
		''', (file_path, status, file_hash, *metadata))
		conn.commit()
		conn.close()


	def find_path(self, path, status='completed'):
		conn = sqlite3.connect(self.db_path)
		cursor = conn.cursor()
		# find matching records
		query = 'SELECT hash, filesize, modification_time FROM files WHERE path=? AND status=?'
		cursor.execute(query, (path, status))
		if cursor.rowcount == 0:
			return None
		info = cursor.fetchone()
		conn.close()
		file_hash, *metadata = info
		return file_hash, metadata


	def find_all(self, root=None, status='completed'):
		conn = sqlite3.connect(self.db_path)
		cursor = conn.cursor()

		if root:
			query = 'SELECT path, hash, filesize, modification_time FROM files WHERE status=? AND path LIKE ?'
			cursor.execute(query, (status, f'{root}%'))
		else:
			query = 'SELECT path, hash, filesize, modification_time FROM files WHERE status=?'
			cursor.execute(query, (status,))

		for row in cursor.fetchall():
			path, file_hash, *metadata = row
			yield path, file_hash, metadata

		conn.close()



