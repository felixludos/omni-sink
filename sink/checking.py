import os
import hashlib



# def compute_file_checksum(filename):
#     sha256 = hashlib.sha256()
#     with open(filename, 'rb') as f:
#         for block in iter(lambda: f.read(4096), b''):
#             sha256.update(block)
#     return sha256.hexdigest()
#
#
#
def compute_directory_checksum(dir_path):
    all_checksums = []
    # Walk the directory and get checksums of all files
    for subdir, _, files in os.walk(dir_path):
        for file in sorted(files):  # Sorting ensures consistent order
            file_path = os.path.join(subdir, file)
            if os.path.isfile(file_path):  # Avoid directories or symlinks
                all_checksums.append(compute_file_checksum(file_path))

    # Combine all individual checksums to produce a final checksum
    combined_checksums = ''.join(all_checksums).encode()
    return hashlib.sha256(combined_checksums).hexdigest()





