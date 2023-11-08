import hashlib
from pathlib import Path



def repo_root():
	"""Returns the root directory of the repository."""
	return Path(__file__).parent.parent



def data_root():
	"""Returns the root directory of the data."""
	return repo_root() / 'data'


# def xor_hash(hash1, hash2):
# 	"""Returns the xor of two hashes."""
# 	return ''.join(chr(ord(a) ^ ord(b)) for a, b in zip(hash1, hash2))


def md5_hash(path, chunksize=1024*1024):
	hasher = hashlib.md5()
	with open(path, 'rb') as f:
		while True:
			data = f.read(chunksize)
			if not data:
				break
			hasher.update(data)
	return hasher.hexdigest()


def xor_hexdigests(hex1, hex2):
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


def hex2int(code: str):
	return int(code, 16)

def int2hex(val: int):
	return format(val, 'x')




