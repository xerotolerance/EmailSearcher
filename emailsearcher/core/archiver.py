#  Copyright (c) 2020. Christopher J Maxwell <contact@christopherjmaxwell.com>
"""
**Core functionality**: Provides classes that facilitate the creation of Tape-ARchive ('.tar') files
"""

from shutil import copy2
from . import Union, os, Path, Stopwatch, asyncio, partial, Optional, tar, MultiProcess, Threaded, abstractmethod


class PatriciaArchiver:
    """Abstract class to provide archiving capabilities on directory-based PATRICIA Tries"""

    def __init__(self, inddir: Union[os.PathLike, str] = '.ind', name: Union[os.PathLike, str] = None,
                 archdir: Union[os.PathLike, str] = '.compressed', tmpdir='.tmp'):
        self.inddir = Path(inddir)
        self.archdir = Path(archdir)
        self.tmpdir = Path(tmpdir)
        self.name = Path(name).stem if name else None

    @staticmethod
    def _archive(root: Union[str, os.PathLike[str]], compression: str = 'gz') -> Optional[str]:
        """
        Replaces *root* and its subfolders with a nested archive structure from the bottom up.

        :param root: Path of root dir to construct nested archive from
        :param compression: Compression algorithm to use when creating archives (ie: '.gz', '.bz2', '.xz', or '' for no compression)
        :return: The path of the newly created '.tar.*' archive as a string.
        """

        def shorten_name(tarinfo_obj: tar.TarInfo):
            """
            Shortens the path of a TarInfo object within a TarFile to just the filename + extension

            :param tarinfo_obj: Entry whose path info will be edited
            :return: The update TarInfo object
            """
            tarinfo_obj.name = Path(tarinfo_obj.name).name
            return tarinfo_obj

        compression = compression.strip(".")
        suffix = '.tar' + f'.{compression}' if compression else ''
        mode = 'w' + f':{compression}' if compression else ''
        archname = None

        for curr, dirs, files in os.walk(root, topdown=False):
            entries = []
            with tar.open(archname := curr + suffix, mode) as archive:  # create tarball of this folder in parent folder
                for filename in files:  # stage this dir's preexisting files (mainly the .ind file) to be added to archive
                    filepath = Path(curr) / filename
                    entries.append(filepath)

                for new_arch in dirs:  # stage this dir's newly tarred subfolders to be added to archive
                    filepath = Path(curr) / (new_arch + suffix)
                    entries.append(filepath)

                for entry in sorted(entries):  # add entries to archive in lexicographical order
                    archive.add(entry, filter=shorten_name)
                    os.remove(entry)
            try:
                os.rmdir(curr)  # prune the now empty current directory
            except OSError:
                print(f" - Couldn't delete {curr}")
                pass
        return archname

    def archive(self, root=None, compression='gz', fast=True, keep_originals=False) -> Optional[str]:
        """
        Replaces *root* and its subfolders with a nested archive structure from the bottom up.

        :param root: Path of root dir to construct nested archive from
        :param compression: Compression algorithm to use when creating archives (ie: '.gz', '.bz2', '.xz').
                            Use '' for no compression.
        :param fast: Uses a pool to parallelize archive creation if True; use single thread if False
        :param keep_originals: If True, a *copy* of the filetree at *root* will be converted to archive,
                                leaving the original in-tact; If False, folders under & including *root* will be replaced
        :return: The path of the newly created '.tar.*' archive as a string.
        """
        root = Path(root) if root else self.inddir
        if keep_originals:
            self.tmpdir.mkdir(parents=True, exist_ok=True)
            shadowroot = self.tmpdir / root.name
            copy2(root, shadowroot)
            root = shadowroot

        if fast:
            self._default_pool().map(partial(PatriciaArchiver._archive, compression=compression),
                                     (item for item in Path(root).iterdir() if item.is_dir()))
        if archname := self._archive(root):
            archive_path = Path(self.archdir) / (
                self.name + "".join(Path(archname).suffixes) if self.name else archname)
            Path(self.archdir).mkdir(parents=True, exist_ok=True)
            os.renames(archname, archive_path)
            return archive_path

    @abstractmethod
    def _default_pool(self, *args, **kwargs):
        """Returns this class' *Pool instance"""
        raise NotImplementedError(
            f'{type(self)}._default_pool(self, *args, **kwargs) is a virtual method.\n'
        )


class MultiProcessArchiver(PatriciaArchiver, MultiProcess):
    """Concrete class to facilitate Multi-Process archiving of directory-based PATRICIA Tries"""

    def __init__(self, inddir: Union[os.PathLike, str] = '.ind', name: Union[os.PathLike, str] = None,
                 archdir: Union[os.PathLike, str] = '.compressed'):
        super().__init__(inddir, name, archdir)   # super() refers to the first Superclass in the set
        MultiProcess.__init__(self)

    def __enter__(self):
        """
        Provides context manager support for MultiProcessArchiver.

        :return: This instance of MultiProcessArchiver
        """
        MultiProcess.__enter__(self)
        return self

    def __del__(self):
        super().__del__()
        MultiProcess.__del__(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        MultiProcess.__exit__(self, exc_type, exc_val, exc_tb)

    def _default_pool(self, *args, **kwargs):
        """Returns this class' multiprocessing.Pool instance"""
        return self._processes


class ThreadedArchiver(PatriciaArchiver, Threaded):
    """Concrete class to facilitate Multi-Threaded archiving of directory-based PATRICIA Tries"""

    def __init__(self, inddir: Union[os.PathLike, str] = '.ind', name: Union[os.PathLike, str] = None,
                 archdir: Union[os.PathLike, str] = '.compressed'):
        super().__init__(inddir, name, archdir)  # super() refers to first Superclass in set
        Threaded.__init__(self)

    def __enter__(self):
        """
        Provides context manager support for ThreadedArchiver.

        :return: This instance of ThreadedArchiver
        """
        Threaded.__enter__(self)
        return self

    def __del__(self):
        super().__del__()
        Threaded.__del__(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        Threaded.__exit__(self, exc_type, exc_val, exc_tb)

    def _default_pool(self, *args, **kwargs):
        """Returns this class' multiprocessing.pool.ThreadPool instance"""
        return self._threads


async def archiver_main(searchdir: Union[os.PathLike, str], inddir: Union[os.PathLike, str] = '.ind', archdir: Union[os.PathLike, str] = '.compressed'):
    """
    Tester coroutine for archiver.py

    :param searchdir: Path to the dataset
    :param inddir:  Path to the directory containing indexes structure for the dataset
    :param archdir: Path to place the newly created archive file into
    :return: Path to the newly created '.tar.*' file as a string.
    """
    archive_path = None
    async with Stopwatch('Archiving index...'):
        with MultiProcessArchiver(inddir, Path(searchdir).stem, archdir) as archiver:
            archive_path = archiver.archive()
    print()
    return archive_path


if __name__ == '__main__':
    asyncio.run(archiver_main('../dataset'))

