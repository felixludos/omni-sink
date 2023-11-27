from pathlib import Path
import omnifig as fig

from . import misc
from .database import FileDatabase, RowInfo



def recursive_mark_crawl(db: FileDatabase, marked_paths: list[Path], skipped: list[Path],
						 path: Path, ignore_names: set[str], *, pbar=None):
	'''post order traversal of the file tree, marking all files for processing'''
	if path != db.db_path and path.name not in ignore_names and path.exists() and not db.exists(path):
		try:
			if path.is_dir():
				for sub in path.iterdir():
					recursive_mark_crawl(db, marked_paths, skipped, sub, ignore_names, pbar=pbar)

		except PermissionError:
			skipped.append(path)
			if pbar is not None:
				pbar.update(1)

		else:
			marked_paths.append(path)
			if pbar is not None:
				pbar.update(1)



def recursive_leaves_crawl(leaves: list[Path], path: Path, terminals: dict[Path, str],
						   pbar=None, get_increment=None):
	if path in terminals or path.is_file():
		if pbar is not None:
			pbar.set_description(f'{" "*(10-len(leaves))}{len(leaves)} leaves')
			pbar.update(get_increment(path))
		leaves.append(path)
	else:
		for sub in path.iterdir():
			recursive_leaves_crawl(leaves, sub, terminals=terminals, pbar=pbar, get_increment=get_increment)



def identify_duplicates(clusters: dict[str, list[RowInfo]], *, pbar=None):
	accepts = {}
	maybe = {}
	rejects = {}

	itr = clusters.items() if pbar is None else pbar(clusters.items())
	for code, items in itr:
		names = [item.path.name for item in items]
		sizes = [item.size for item in items]
		modtimes = [item.modtime for item in items]

		if not any(s != sizes[0] for s in sizes):
			if not any(n != names[0] for n in names) and not any(m != modtimes[0] for m in modtimes):
				accepts[code] = items
			else:
				maybe[code] = items
		else:
			rejects[code] = items

	return accepts, maybe, rejects



@fig.component('default-ordering')
class PathOrdering(fig.Configurable):
	@staticmethod
	def _path_score(path):
		return 'old' in str(path).lower(), len(path.parents), len(path.name), len(str(path)), path.name

	def inplace(self, paths: list[Path], get_info=None):
		paths.sort(key=self._path_score)


