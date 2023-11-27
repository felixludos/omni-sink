import os
from pathlib import Path
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
import omnifig as fig

from . import misc


@dataclass
class RowInfo:
	path: Path
	code: str = None
	count: int = None
	size: int = None
	modtime: float = None

	@property
	def modified(self):
		return datetime.fromtimestamp(self.modtime)

	def __post_init__(self):
		if isinstance(self.path, str):
			self.path = Path(self.path)


@fig.component('file-db')
class FileDatabase(fig.Configurable):
	def __init__(self, db_path: Path | str = misc.data_root()/'files.db', chunksize: int = 1024*1024):
		self.db_path = Path(db_path).absolute()
		self.chunksize = chunksize
		self.conn = sqlite3.connect(str(self.db_path))
		self.init_database()
		self._report_id = None


	_RowInfo = RowInfo
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
				filecount INTEGER,
				filesize INTEGER,
				modification_time REAL,
				FOREIGN KEY (report) REFERENCES reports(id)
			)''')
		cursor.execute('''
			CREATE INDEX IF NOT EXISTS idx_hash ON files(hash);
			''')
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
	def compute_directory_hash(content_hashes: list[str]) -> str:
		data = b''.join(bytes.fromhex(code) for code in sorted(content_hashes))
		return misc.md5_hash(data)


	def compute_directory_info(self, dir_path: Path, content_info) -> tuple[Path, tuple[str, tuple]]:
		if len(content_info) == 0:
			hashes = []
			dirsize = 0
			dircount = 0
		else:
			hashes, metadatas = zip(*content_info)
			counts, sizes, _ = zip(*metadatas)
			dircount = sum(cnt or 1 for cnt in counts)
			dirsize = sum(sizes)

		directory_hash = self.compute_directory_hash(hashes)
		modification_time = dir_path.stat().st_mtime
		# modification_time = os.path.getmtime(dir_path)

		metadata = dircount, dirsize, modification_time
		return dir_path, (directory_hash, metadata)


	def process_file(self, file_path: Path) -> tuple[Path, tuple[str, tuple]]:
		file_hash = self.compute_hash(file_path)

		size, modification_time = file_path.stat().st_size, file_path.stat().st_mtime
		# size, modification_time = os.path.getsize(file_path), os.path.getmtime(file_path)
		metadata = None, size, modification_time
		return file_path, (file_hash, metadata)


	def process_dir(self, dir_path: Path, ignore_names) -> tuple[Path, tuple[str, tuple]]:
		contents = []
		for content_path in dir_path.iterdir():
			if content_path == self.db_path or content_path.name in ignore_names or not content_path.exists():
				continue
			rawinfo = self._find_path_raw(content_path)
			if rawinfo is None:
				raise ValueError(f'Missing path: {content_path}')
			contents.append(rawinfo)
		return self.compute_directory_info(dir_path, contents)


	def save_file_info(self, file_path: Path | str, raw_info: tuple[str, tuple], status: str = 'completed'):
		conn = self.conn
		cursor = conn.cursor()

		hash_code, metadata = raw_info

		cursor.execute('''
			INSERT OR REPLACE INTO files (path, report, status, hash, filecount, filesize, modification_time)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		''', (str(file_path), self.get_report_id(), status, hash_code, *metadata))
		conn.commit()


	def _find_path_raw(self, path, status: str = 'completed') -> tuple[str, tuple] | None:
		conn = self.conn
		cursor = conn.cursor()

		query = ('SELECT hash, filecount, filesize, modification_time '
				 'FROM files WHERE path=? AND status=?')
		cursor.execute(query, (str(path), status))
		if cursor.rowcount == 0:
			return None
		rawinfo = cursor.fetchone()

		if rawinfo is not None:
			hash_code, *metadata = rawinfo
			return hash_code, metadata


	@lru_cache(maxsize=None)
	def find_path(self, path: Path | str) -> RowInfo | None:
		rawinfo = self._find_path_raw(path)
		if rawinfo is not None:
			hash_code, metadata = rawinfo
			return self._RowInfo(path, hash_code, *metadata)


	def find_duplicates(self, hash_code: str, path_prefix: Path | str = None) -> RowInfo:
		conn = self.conn
		cursor = conn.cursor()

		if path_prefix is None:
			query = ('SELECT path, filecount, filesize, modification_time '
					 'FROM files WHERE hash=?')
			cursor.execute(query, (hash_code,))

		else:
			query = ('SELECT path, filecount, filesize, modification_time '
					 'FROM files WHERE hash=? AND path LIKE ?')
			cursor.execute(query, (hash_code, f'{path_prefix}%'))

		for row in cursor.fetchall():
			path, *metadata = row
			yield self._RowInfo(path, hash_code, *metadata)


	def find_all_duplicates(self, path_prefix: Path | str = None):
		conn = self.conn
		cursor = conn.cursor()

		if path_prefix is None:
			query = ('SELECT path, hash, filecount, filesize, modification_time '
					 'FROM files '
					 'WHERE hash IN (SELECT hash FROM files GROUP BY hash HAVING COUNT(*) > 1) AND filesize > 0')
			cursor.execute(query)

		else:
			query = ('SELECT path, hash, filecount, filesize, modification_time '
					 'FROM files '
					 'WHERE hash IN (SELECT hash FROM files GROUP BY hash HAVING COUNT(*) > 1) '
					 'AND filesize > 0 AND path LIKE ?')
			cursor.execute(query, (f'{path_prefix}%',))

		for row in cursor.fetchall():
			path, hash_code, *metadata = row
			yield self._RowInfo(path, hash_code, *metadata)


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


	def find_all(self, root: Path | str = None, status: str = 'completed'):
		conn = self.conn
		cursor = conn.cursor()

		if root:
			query = ('SELECT path, hash, filecount, filesize, modification_time '
					 'FROM files WHERE status=? AND path LIKE ?')
			cursor.execute(query, (status, f'{root}%'))

		else:
			query = ('SELECT path, hash, filecount, filesize, modification_time '
					 'FROM files WHERE status=?')
			cursor.execute(query, (status,))

		for row in cursor.fetchall():
			path, hash_code, *metadata = row
			yield self._RowInfo(path, hash_code, *metadata)



