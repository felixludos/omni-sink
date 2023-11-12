import os
from pathlib import Path
import sqlite3
import omnifig as fig

from . import misc



@fig.component('file-db')
class FileDatabase(fig.Configurable):
	def __init__(self, db_path: str =str(misc.data_root()/'files.db'), chunksize: int = 1024*1024):
		self.db_path = str(Path(db_path).absolute())
		self.chunksize = chunksize
		self.conn = sqlite3.connect(self.db_path)
		self.init_database()
		self._report_id = None


	def init_database(self):
		conn = self.conn
		cursor = conn.cursor()
		cursor.execute('''
		    CREATE TABLE IF NOT EXISTS reports (
		        id INTEGER PRIMARY KEY AUTOINCREMENT,
		        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		        description TEXT
		    )''')
		cursor.execute('''
			CREATE TABLE IF NOT EXISTS files (
				path TEXT PRIMARY KEY,
				report INTEGER NOT NULL,
				status TEXT,
				hash TEXT,
				filesize INTEGER,
				modification_time REAL,
				FOREIGN KEY (report) REFERENCES reports(id)
			)''')
		conn.commit()


	def create_report_id(self, description: str | None = None):
		conn = self.conn
		cursor = conn.cursor()
		cursor.execute('INSERT INTO reports (description) VALUES (?)', (description,))
		conn.commit()
		report_id = cursor.lastrowid
		return report_id


	def set_report_id(self, report_id: int):
		self._report_id = report_id
	def get_report_id(self, description: str = None) -> int:
		if self._report_id is None:
			self._report_id = self.create_report_id(description)
		return self._report_id


	def compute_hash(self, file_path: str) -> str:
		return misc.md5_file_hash(file_path, chunksize=self.chunksize)


	@staticmethod
	def compute_data_hash(data: bytes | str) -> str:
		if isinstance(data, str):
			data = data.encode()
		return misc.md5_hash(data)


	@staticmethod
	def compute_directory_hash(content_hashes: list[str]) -> str:
		if not len(content_hashes):
			return ''
		code = content_hashes[0]
		for h in content_hashes[1:]:
			if code is None or len(code) == 0:
				code = h
			elif h is not None and len(h):
				code = misc.xor_hexdigests(code, h)
		return code


	def compute_directory_info(self, dir_path, content_info):
		if len(content_info) == 0:
			# hashes, metadatas = [], []
			# filesizes, modification_times = [], []
			modification_time = os.path.getmtime(dir_path)
			metadata = 0, modification_time
			directory_hash = self.compute_data_hash(dir_path)
		else:
			hashes, metadatas = zip(*content_info)
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


	def process_dir(self, dir_path):
		contents = []
		for name in os.listdir(dir_path):
			content_path = os.path.join(dir_path, name)
			info = self.find_path(content_path)
			if info is None:
				raise ValueError(f'Unknown path: {content_path}')
			contents.append(info)
		return self.compute_directory_info(dir_path, contents)


	def save_file_info(self, file_path, file_info, status='completed'):
		conn = self.conn
		cursor = conn.cursor()

		file_hash, metadata = file_info
		hash_val = file_hash

		cursor.execute('''
			INSERT OR REPLACE INTO files (path, report, status, hash, filesize, modification_time)
			VALUES (?, ?, ?, ?, ?, ?)
		''', (file_path, self.get_report_id(), status, hash_val, *metadata))
		conn.commit()


	def find_path(self, path, status='completed'):
		conn = self.conn
		cursor = conn.cursor()

		query = 'SELECT hash, filesize, modification_time FROM files WHERE path=? AND status=?'
		cursor.execute(query, (path, status))
		if cursor.rowcount == 0:
			return None
		info = cursor.fetchone()

		if info is not None:
			hash_val, *metadata = info
			file_hash = hash_val
			return file_hash, metadata


	def find_duplicates(self, path, hash_code):
		conn = self.conn
		cursor = conn.cursor()

		query = 'SELECT path, filesize, modification_time FROM files WHERE hash=? AND path!=?'
		cursor.execute(query, (hash_code, path))

		for row in cursor.fetchall():
			path, *metadata = row
			yield path, metadata


	def exists(self, path, status='completed'):
		conn = self.conn
		cursor = conn.cursor()

		if status is None:
			query = 'SELECT COUNT(*) FROM files WHERE path=?'
			cursor.execute(query, (path,))
		else:
			query = 'SELECT COUNT(*) FROM files WHERE path=? AND status=?'
			cursor.execute(query, (path, status))

		count = cursor.fetchone()[0]
		return count > 0


	def find_all(self, root=None, status='completed'):
		conn = self.conn
		cursor = conn.cursor()

		if root:
			query = 'SELECT path, hash, filesize, modification_time FROM files WHERE status=? AND path LIKE ?'
			cursor.execute(query, (status, f'{root}%'))
		else:
			query = 'SELECT path, hash, filesize, modification_time FROM files WHERE status=?'
			cursor.execute(query, (status,))

		for row in cursor.fetchall():
			path, hash_val, *metadata = row
			file_hash = hash_val
			yield path, (file_hash, metadata)



