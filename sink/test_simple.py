
from .checking import compute_checksum


# def test_checking():
#
# 	filename = "path_to_your_file.txt"
# 	print(f"SHA256 Checksum of {filename}: {compute_checksum(filename)}")
#
# 	pass



import os
import tempfile
from FileProcessor import FileProcessor  # Assume FileProcessor class is saved in a file named FileProcessor.py
import pytest

# Creating a temporary directory and files for testing
@pytest.fixture
def setup_temp_files():
    with tempfile.TemporaryDirectory() as tmpdirname:
        file1 = os.path.join(tmpdirname, "file1.txt")
        file2 = os.path.join(tmpdirname, "file2.txt")
        with open(file1, "w") as f:
            f.write("Hello, world!")
        with open(file2, "w") as f:
            f.write("Hello, world!")  # Duplicate content for testing duplicate finding
        yield tmpdirname, file1, file2

# Test file processing
def test_file_processing(setup_temp_files):
    tmpdirname, file1, _ = setup_temp_files
    processor = FileProcessor(":memory:")  # Using in-memory database for testing
    file_info = processor.process_file(file1)
    assert file_info is not None
    assert file_info[1]['metadata']['size'] == 13  # Checking the size of the file as it contains 13 characters

# Test directory processing
def test_directory_processing(setup_temp_files):
    tmpdirname, _, _ = setup_temp_files
    processor = FileProcessor(":memory:")
    directory_info = processor.process_directory(tmpdirname)
    assert len(directory_info) == 2  # As we have created two files in the directory

# Test saving to the database and finding duplicates
def test_database_saving_and_finding_duplicates(setup_temp_files):
    tmpdirname, file1, file2 = setup_temp_files
    processor = FileProcessor(":memory:")
    processor.process_and_save_file((file1, None))  # Passing None for lock as it's not needed in single processing mode
    processor.process_and_save_file((file2, None))

    # Since both files have the same content, they are duplicates
    duplicate_files = processor.find_duplicates()
    assert len(duplicate_files) == 2
    assert file1 in duplicate_files
    assert file2 in duplicate_files





