from pathlib import Path

from . import misc
from .database import FileDatabase



def recursive_marker(db: FileDatabase, marked_paths: list[Path], path: Path, pbar=None):
	if path != db.db_path and not db.exists(path):
		if path.is_dir():
			for sub in path.iterdir():
				recursive_marker(db, marked_paths, sub)

		marked_paths.append(path)

		if pbar is not None:
			pbar.update(1)



def recursive_collect_dupes(db: FileDatabase, duplicates: dict[Path, dict], path: Path, pbar=None):
	if pbar is not None:
		pbar.set_description(path)

	info = db.find_path(path)

	if info is None:
		raise ValueError(f'Missing path: {path} (did you forget to run `add`?)')

	base_hash, (base_size, base_modified) = info

	dupes = {other: {'size': size, 'modified': modified}
			 for other, (size, modified) in db.find_duplicates(path, base_hash)}

	if len(dupes):
		duplicates[path] = {'dupes': dupes, 'size': base_size, 'modified': base_modified}

	elif path.is_dir():
		for sub in path.iterdir():
			recursive_collect_dupes(db, duplicates, sub, pbar=pbar)



def process_path(db: FileDatabase, path: Path):
	info = db.find_path(path)

	if info is None:
		if path.is_file():
			savepath, info = db.process_file(path)

		elif path.is_dir():
			savepath, info = db.process_dir(path)

		else:
			raise ValueError(f"Unknown path type: {path}")

		db.save_file_info(savepath, info)



