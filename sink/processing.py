from pathlib import Path

from . import misc
from .database import FileDatabase



def recursive_mark_crawl(db: FileDatabase, marked_paths: list[Path], path: Path):
	'''post order traversal of the file tree, marking all files for processing'''
	if path != db.db_path and not db.exists(path):
		if path.is_dir():
			for sub in path.iterdir():
				recursive_mark_crawl(db, marked_paths, sub)

		marked_paths.append(path)



def recursive_leaves_crawl(leaves: list[Path], path: Path, terminals: dict[Path, str],
						   pbar=None, get_size=None):
	if path in terminals or path.is_file():
		if pbar is not None:
			pbar.set_description(f'{" "*(10-len(leaves))}{len(leaves)} leaves')
			pbar.update(get_size(path))
		leaves.append(path)
	else:
		for sub in path.iterdir():
			recursive_leaves_crawl(leaves, sub, terminals=terminals, pbar=pbar, get_size=get_size)



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



def identify_duplicates(clusters: dict[str, list[dict]], *, pbar=None):
	accepts = {}
	maybe = {}
	rejects = {}

	itr = clusters.items() if pbar is None else pbar(clusters.items())
	for code, items in itr:
		names = [item['path'].name for item in items]
		sizes = [item['size'] for item in items]
		modtimes = [item['modtime'] for item in items]

		if not any(s != sizes[0] for s in sizes):
			if not any(n != names[0] for n in names) and not any(m != modtimes[0] for m in modtimes):
				accepts[code] = items
			else:
				maybe[code] = items
		else:
			rejects[code] = items

	return accepts, maybe, rejects



