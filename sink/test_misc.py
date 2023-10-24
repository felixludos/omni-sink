

import pytest
from . import misc
from .misc import xor_hexdigests  # assuming your function is in 'your_module.py'
import os
import tempfile

def test_basic_functionality():
    hex1 = "0a74f7b7ba22fb27d6ad04f218644f98"
    hex2 = "5f3adfe45b2acdf7c0f1d9a1e8466f91"
    result = xor_hexdigests(hex1, hex2)
    assert result == "554e2853e10836d0165cdd53f0222009"

def test_unequal_lengths():
    with pytest.raises(ValueError):
        xor_hexdigests("abcd", "abcdef")

def test_case_insensitivity():
    hex1 = "0a74F7B7BA22FB27D6AD04F218644F98"
    hex2 = "5F3ADFE45B2ACDF7C0F1D9A1E8466F91"
    result = xor_hexdigests(hex1, hex2)
    assert result == "554e2853e10836d0165cdd53f0222009"

def test_all_zeros():
    hex1 = "00000000000000000000000000000000"
    hex2 = "00000000000000000000000000000000"
    result = xor_hexdigests(hex1, hex2)
    assert result == "00000000000000000000000000000000"

def test_all_ones():
    hex1 = "ffffffffffffffffffffffffffffffff"
    hex2 = "ffffffffffffffffffffffffffffffff"
    result = xor_hexdigests(hex1, hex2)
    assert result == "00000000000000000000000000000000"


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


def test_hex_conversion():
    x = '0a'
    y = misc.hex2int(x)
    assert y == 10
    z = misc.int2hex(y)
    assert z == x


def test_small_file(setup_temp_files):
    tmpdirname, file1, file2 = setup_temp_files

    hsh = misc.md5_hash(file1)
    print(hsh)
    hsh2 = misc.md5_hash(file2)
    print(hsh2)

    assert hsh == hsh2
    assert hsh == misc.int2hex(misc.hex2int(hsh))








