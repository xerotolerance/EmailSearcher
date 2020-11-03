#  Copyright (c) 2020. Christopher J Maxwell <contact@christopherjmaxwell.com>
"""
Main entrypoint for the *emailsearcher* package.

*emailsearcher* provides resources to search through a large dataset quickly and efficiently.
"""

from emailsearcher import *
import argparse


async def analyze(searchdir: Union[os.PathLike, str], query: str, inddir: Union[os.PathLike, str] = '.ind', tmpdir: Union[os.PathLike, str] = '.tmp', archdir: Union[os.PathLike, str] = '.compressed') -> None:
    """
    Runner coroutine for PATRICIA Trie-based Indexing & Search Utils.
    
    :param searchdir: Location of (un-packed) directory to index
    :param query: String to search for
    :param inddir: Directory in which to build PATRICIA Trie
    :param tmpdir: Directory in which to put index files while indexing occurs
            (index files will be moved to their correct spots in the Trie & tmpdir will be auto-deleted at end)
    :param archdir: Directory to place newly constructed archive file into
    :return: None (changes reflected on disk)
    """
    async with Stopwatch('Removing existing dirs...'):
        mp_rmdir(inddir)
        mp_rmdir(tmpdir)
        mp_rmdir(archdir)
    print()

    async with Stopwatch('Indexing files...'):
        index_directory(searchdir, inddir, tmpdir)
    print()

    res = set()
    async with Stopwatch('Searching index...'):
        with Searcher(inddir, searchdir=searchdir) as searcher:
            res = searcher.search(query)
    print(f'Found {len(res)} result(s).')
    for r in sorted(res):
        print('\t- ', r)
    print()

    archive_path = None
    async with Stopwatch('Archiving index...'):
        with MultiProcessArchiver(inddir, Path(searchdir).stem, archdir) as archiver:
            archive_path = archiver.archive()
    print()

    res = set()
    async with Stopwatch('Searching archive...'):
        with Searcher(archive_path, searchdir=searchdir) as searcher:
            res = searcher.search(query)
    print(f'Found {len(res)} result(s).')
    for r in sorted(res):
        print('\t- ', r)


def main(searchdir: Union[os.PathLike, str], query: str, inddir: Union[os.PathLike, str] = '.ind', tmpdir: Union[os.PathLike, str] = '.tmp', archdir: Union[os.PathLike, str] = '.compressed'):
    """
    Launcher for emailsearcher module.

    :param searchdir: Location of (un-packed) directory to index
    :param query: String to search for
    :param inddir: Directory in which to build PATRICIA Trie
    :param tmpdir: Directory in which to put index files while indexing occurs
            (index files will be moved to their correct spots in the Trie & tmpdir will be auto-deleted at end)
    :param archdir: Directory to place newly constructed archive file into
    :return: None (changes reflected on disk)
    """
    asyncio.run(analyze(searchdir, query, inddir, tmpdir, archdir))


if __name__ == '__main__':
    """Facilitates commandline access to the emailsearcher package."""
    parser = argparse.ArgumentParser('emailsearcher')
    parser.add_argument('-e', '--entrypoint')
    parser.add_argument('-s', '--search_for', nargs='+', metavar='TERM', default=[])
    args = parser.parse_args()
    main(args.entrypoint, ' '.join(args.search_for))
