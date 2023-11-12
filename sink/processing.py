import os

from . import misc
from .database import FileDatabase



def recursive_marker(db: FileDatabase, marked_paths: list[str], path: str, pbar=None):
	if path != db.db_path and not db.exists(path):
		if os.path.isdir(path):
			for sub in os.listdir(path):
				subpath = os.path.join(path, sub)
				recursive_marker(db, marked_paths, subpath)

		marked_paths.append(path)

		if pbar is not None:
			pbar.update(1)



def recursive_collect_dupes(db: FileDatabase, duplicates: dict[str, dict], path: str, pbar=None):
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



def process_path(db: FileDatabase, path: str):
	info = db.find_path(path)

	if info is None:
		if os.path.isfile(path):
			savepath, info = db.process_file(path)

		elif os.path.isdir(path):
			savepath, info = db.process_dir(path)

		else:
			raise ValueError(f"Unknown path type: {path}")

		db.save_file_info(savepath, info)



