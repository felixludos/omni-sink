import hashlib
from pathlib import Path



def repo_root() -> Path:
	"""Returns the root directory of the repository."""
	return Path(__file__).parent.parent



def data_root() -> Path:
	"""Returns the root directory of the data."""
	return repo_root() / 'data'



def md5_file_hash(path: Path, chunksize: int = 1024*1024) -> str:
	hasher = hashlib.md5()
	with path.open('rb') as f:
		while True:
			data = f.read(chunksize)
			if not data:
				break
			hasher.update(data)
	return hasher.hexdigest()



def md5_hash(data: bytes) -> str:
	hasher = hashlib.md5()
	hasher.update(data)
	return hasher.hexdigest()



def xor_hexdigests(hex1: str, hex2: str) -> str:
	# Ensure both hexdigests are of the same length
	if len(hex1) != len(hex2):
		raise ValueError("Hex strings must be of the same length")

	# Convert each hex string to an integer
	int1 = int(hex1, 16)
	int2 = int(hex2, 16)

	# XOR the two integers
	xor_result = int1 ^ int2

	# Convert the result back to hex and remove the '0x' prefix
	return format(xor_result, 'x').zfill(len(hex1))


def hex2int(code: str) -> int:
	return int(code, 16)

def int2hex(val: int) -> str:
	return format(val, 'x')




