from pathlib import Path



def repo_root():
	"""Returns the root directory of the repository."""
	return Path(__file__).parent.parent



def data_root():
	"""Returns the root directory of the data."""
	return repo_root() / 'data'



