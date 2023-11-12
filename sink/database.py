import os
from pathlib import Path
import sqlite3
import omnifig as fig

from . import misc



@fig.component('file-db')
class FileDatabase(fig.Configurable):
	def __init__(self, db_path: Path | str = misc.data_root()/'files.db', chunksize: int = 1024*1024):
		self.db_path = Path(db_path).absolute()
		self.chunksize = chunksize
		self.conn = sqlite3.connect(str(self.db_path))
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


	def compute_hash(self, file_path: Path) -> str:
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


	def compute_directory_info(self, dir_path: Path, content_info) -> tuple[Path, tuple[str, tuple]]:
		if len(content_info) == 0:
			directory_hash = self.compute_data_hash(str(dir_path))
			dirsize = 0
		else:
			hashes, metadatas = zip(*content_info)
			filesizes, modification_times = zip(*metadatas)
			directory_hash = self.compute_directory_hash(hashes)
			dirsize = sum(filesizes)

		modification_time = dir_path.stat().st_mtime
		# modification_time = os.path.getmtime(dir_path)

		metadata = dirsize, modification_time
		return dir_path, (directory_hash, metadata)


	def process_file(self, file_path: Path) -> tuple[Path, tuple[str, tuple]]:
		file_hash = self.compute_hash(file_path)

		size, modification_time = file_path.stat().st_size, file_path.stat().st_mtime
		# size, modification_time = os.path.getsize(file_path), os.path.getmtime(file_path)
		metadata = size, modification_time
		return file_path, (file_hash, metadata)


	def process_dir(self, dir_path: Path) -> tuple[Path, tuple[str, tuple]]:
		contents = []
		for content_path in dir_path.iterdir():
			info = self.find_path(content_path)
			if info is None:
				raise ValueError(f'Missing path: {content_path}')
			contents.append(info)
		return self.compute_directory_info(dir_path, contents)


	def save_file_info(self, file_path: Path | str, file_info: tuple[str, tuple], status: str = 'completed'):
		conn = self.conn
		cursor = conn.cursor()

		file_hash, metadata = file_info
		hash_val = file_hash

		cursor.execute('''
			INSERT OR REPLACE INTO files (path, report, status, hash, filesize, modification_time)
			VALUES (?, ?, ?, ?, ?, ?)
		''', (str(file_path), self.get_report_id(), status, hash_val, *metadata))
		conn.commit()


	def find_path(self, path: Path | str, status: str = 'completed'):
		conn = self.conn
		cursor = conn.cursor()

		query = 'SELECT hash, filesize, modification_time FROM files WHERE path=? AND status=?'
		cursor.execute(query, (str(path), status))
		if cursor.rowcount == 0:
			return None
		info = cursor.fetchone()

		if info is not None:
			hash_val, *metadata = info
			file_hash = hash_val
			return file_hash, metadata


	def find_duplicates(self, path: Path | str, hash_code: str) -> tuple[Path, tuple[str, tuple]]:
		conn = self.conn
		cursor = conn.cursor()

		query = 'SELECT path, filesize, modification_time FROM files WHERE hash=? AND path!=?'
		cursor.execute(query, (hash_code, str(path)))

		for row in cursor.fetchall():
			path, *metadata = row
			yield Path(path), metadata


	def exists(self, path: Path | str, status: str = 'completed') -> bool:
		conn = self.conn
		cursor = conn.cursor()

		if status is None:
			query = 'SELECT COUNT(*) FROM files WHERE path=?'
			cursor.execute(query, (str(path),))
		else:
			query = 'SELECT COUNT(*) FROM files WHERE path=? AND status=?'
			cursor.execute(query, (str(path), status))

		count = cursor.fetchone()[0]
		return count > 0


	def find_all(self, root: Path | str = None, status: str = 'completed') -> tuple[Path, tuple[str, tuple]]:
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
			yield Path(path), (file_hash, metadata)



