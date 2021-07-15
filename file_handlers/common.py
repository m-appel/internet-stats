import logging
import os


def get_file_name(file: str, suffix: str) -> str:
    """Get the basename of the file and return the basename of the
    destination if file is a symlink."""
    if os.path.islink(file):
        file = os.readlink(file)
    basename = os.path.basename(file)
    if not basename.endswith(suffix):
        logging.warning(f'Can not determine name for input {file}: '
                        f'Unexpected suffix (Expected: {suffix})')
        return basename
    return basename[:-len(suffix)]


def make_symlink(src: str, dst: str) -> None:
    """Create a symlink from src to dst. Remove dst first if it
    exists."""
    if os.path.exists(dst):
        os.remove(dst)
    os.symlink(src, dst)
