#  Copyright (c) 2020. Christopher J Maxwell <contact@christopherjmaxwell.com>
"""
**Core functionality**: Provides classes that facilitate the searching of a dataset with PATRICIA-Trie_ structured indexes.

.. _PATRICIA-Trie: https://dl.acm.org/doi/10.1145/321479.321481
"""
from typing import List

from . import ABC, abstractmethod, Threaded, MultiProcess, os, Path, as_tokens, tokenize, partial, patricia_path, \
    safeguard_path, tar, Set, Stopwatch, asyncio, Union, Any, Optional


class RealTimeSearcher(ABC):
    """Interface to provide effective Search capabilities"""

    @abstractmethod
    def fuzzy_search(self, *args, **kwargs) -> Any:
        """Searches a dataset for entries containing words that start with the provided tokens"""
        pass

    @abstractmethod
    def match_words(self, *args, **kwargs) -> Any:
        """Searches a dataset for entries containing all the provided tokens"""
        pass

    @abstractmethod
    def match_phrase(self, *args, **kwargs) -> Any:
        """Searches a dataset for a provided phrase"""
        pass

    @abstractmethod
    def search(self, *args, **kwargs) -> Any:
        """Provides general searching of a dataset"""
        pass


class PatriciaTrieSearcher(RealTimeSearcher, Threaded, MultiProcess):
    """Abstract Class to facilitate searching a PATRICIA Trie in parallel"""

    def __init__(self, idroot: Union[os.PathLike[str], str] = '.ind', id_ext: str = '.ind', searchdir: Union[os.PathLike[str], str] = None):
        Threaded.__init__(self)
        MultiProcess.__init__(self)
        self.idroot = idroot
        self.id_ext = id_ext
        self.searchdir = Path(searchdir)

    def __enter__(self):
        """
        Provides context manager support for PatriciaTreeSearchers.

        :return: This instance of PatriciaTrieSearcher
        :rtype: PatriciaTrieSearcher
        """
        Threaded.__enter__(self)
        MultiProcess.__enter__(self)
        return self

    def __del__(self):
        Threaded.__del__(self)
        MultiProcess.__del__(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        Threaded.__exit__(self, exc_type, exc_val, exc_tb)
        MultiProcess.__exit__(self, exc_type, exc_val, exc_tb)

    @abstractmethod
    def _fuzzy_policy(self, token: str, *args, **kwargs):
        """
        Policy Function that takes a token and returns some data when a 'fuzzy' match is found.

        :param args:
            0) a token: str, ...
        :param kwargs: ...
        :return: Some search data
        """
        raise NotImplementedError(
            f'{type(self)}._fuzzy_policy(self, *args, **kwargs) is a virtual method.\n'
        )

    @abstractmethod
    def _exact_policy(self, token: str, *args, **kwargs):
        """
        Policy Function that takes a token and returns some data when an 'exact' match is found.

        :param args:
            0) a token: str, ...
        :param kwargs: ...
        :return: Some search data
        """
        raise NotImplementedError(
            f'{type(self)}._exact_policy(self, *args, **kwargs) is a virtual method.\n'
        )

    @abstractmethod
    def _search(self, query: str, idroot: Union[os.PathLike[str], str] = None, id_ext: str = None, inclusive: bool = False, policy_function=None):
        """
        Provides **core search capabilities** for a PatriciaTrieSearcher.

        :param query: String of tokens to search for
        :param idroot: Path of index dir/archive file to search within
        :param id_ext: Desired extension of files to match
        :param inclusive: Includes results matching *any* token if True; matching *all* tokens if False
        :param policy_function: Function that takes a token and returns some search data
        :return: Search results.
        """
        raise NotImplementedError(
            f'{type(self)}._search(self, *args, **kwargs) is a virtual method.\n'
        )

    @staticmethod
    def _examiner(filepath: Union[str, os.PathLike[str]], state: dict) -> Union[str, os.PathLike[str], None]:
        """
        Checks if a file contains a phrase given a dictionary of attributes describing the search criteria.

        :param filepath: Path of file to search in
        :param state: A dictionary with pairs:
                    {
                        'searchdir': Union[str, os.Pathlike[str]],
                        'phrase_len': int,
                        'fuzzy': bool,
                        'phrase_tokens': List[str],
                        & 'end_tok': str
                    }
        :return: Path to the file if phrase was found; else None
        """

        searchdir = state['searchdir']
        phrase_len = state['phrase_len']
        fuzzy = state['fuzzy']
        phrase_tokens = state['phrase_tokens']
        endtok = state['endtok']

        tokens = list(
            as_tokens(Path(searchdir / filepath).read_text(encoding='utf8', errors='surrogateescape'),
                      unique=False))
        lim = max(len(tokens) - phrase_len - bool(fuzzy), 0)
        found = any(
            phrase_tokens == tokens[x:x + phrase_len]  # exact match first n-1 tokens in phrase
            and tokens[x + phrase_len - (not fuzzy)].startswith(endtok)
            # fuzzy match last token in phrase (if applicable)
            for x in range(lim)
        )
        return filepath if found else None

    def fuzzy_search(self, query: str, idroot: Union[str, os.PathLike[str]] = None, id_ext: str = None, inclusive=False):
        """
        Searches a dataset for entries containing words that start with the provided tokens.

        :param query: Search string
        :param idroot: Path to the index file dir/ archive file to search in
        :param id_ext: File extension of desired files in idroot (eg '.ind' for index files)
        :param inclusive: Include results matching *any* token in query if True; matching *all* tokens in False.
        :return: A set of search results
        """
        return self._search(query, idroot, id_ext, inclusive, policy_function=self._fuzzy_policy)

    def match_words(self, query: str, idroot: Union[str, os.PathLike[str]] = None, id_ext: str = None, inclusive=False):
        """
        Searches a dataset for entries containing all the provided tokens

        :param query: Search string
        :param idroot: Path to the index file dir/ archive file to search in
        :param id_ext: File extension of desired files in idroot (eg '.ind' for index files)
        :param inclusive: Include results matching *any* token in query if True; matching *all* tokens in False.
        :return: A set of search results
        """
        return self._search(query, idroot, id_ext, inclusive, policy_function=self._exact_policy)

    def match_phrase(self, query: str, idroot: Union[str, os.PathLike[str]] = None, id_ext: str = None, searchdir=None, fuzzy=True):
        """
        Searches a dataset for a provided phrase.

        :param query: Search string
        :param idroot: Path to the index file dir/ archive file to search in
        :param id_ext: File extension of desired files in idroot (eg '.ind' for index files)
        :param searchdir: Path to the dataset that idroot holds indexes for
        :param fuzzy: Include results containing phrases starting w/ the given query if True; exact match query if False
        :return: A set of search results
        """
        results = set()
        if query:
            searchdir = Path(searchdir) if searchdir else self.searchdir
            phrase_tokens = list(as_tokens(query, unique=False))
            candidates, endmatches = set(), set()
            if len(phrase_tokens) > 1:
                endtok = phrase_tokens[-1]
                if fuzzy:  # find set of srcs containing words that start w/ endtok
                    endmatches = self.fuzzy_search(phrase_tokens.pop(), idroot, id_ext, inclusive=False)
                    if not endmatches:
                        return results  # endtok not found means phrase won't be found either
                phrase_len = len(phrase_tokens)

                # find set of srcs containing all words in the phrase (excluding endtok if applicable)
                candidates = self.match_words(" ".join(phrase_tokens), idroot, id_ext, inclusive=False)

                if endmatches:  # only keep srcs found in both sets
                    candidates &= endmatches
                    endmatches.clear()

                state = {
                    'phrase_tokens': phrase_tokens,
                    'searchdir': searchdir,
                    'phrase_len': phrase_len,
                    'fuzzy': fuzzy,
                    'endtok': endtok
                }

                # find set of srcs containing the phrase as a whole
                results = {src for src in self._processes.map(
                    partial(PatriciaTrieSearcher._examiner, state=state),
                    (candidates.pop() for _ in range(len(candidates)))
                ) if src}
            elif fuzzy:
                results = self.fuzzy_search(query, idroot, id_ext)
            else:
                results = self.match_words(query, idroot, id_ext)
        return results

    def _default_search(self, *args, **kwargs):
        """Defines the search method used by self.search"""
        return self.match_phrase(*args, **kwargs)

    def search(self, *args, **kwargs):
        return self._default_search(*args, **kwargs)


class FileTreeSearcher(PatriciaTrieSearcher):
    """Concrete Class For Searching a Directory-based PATRICIA Trie"""

    # Required Overloads
    def __init__(self, idroot: Union[os.PathLike[str], str] = '.ind', id_ext: str = '.ind', searchdir: Union[os.PathLike[str], str] = None):
        super().__init__(idroot, id_ext, searchdir)

    def __del__(self):
        super().__del__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)

    def _fuzzy_policy(self, token, *args, **kwargs):
        return self.fuzzyfindtoken(token, *args, **kwargs)

    def _exact_policy(self, token, *args, **kwargs):
        return self.findtoken(token, *args, **kwargs)

    def _search(self, query: str, idroot: Union[str, os.PathLike[str]] = None, id_ext: str = None, inclusive: bool = False, policy_function=None) -> Set[str]:
        """
        Provides **core search capabilities** for a FileTreeSearcher.

        :param query: String of tokens to search for
        :param idroot: Path of index dir/archive file to search within
        :param id_ext: Desired extension of files to match
        :param inclusive: Includes results matching *any* token if True; matching *all* tokens if False
        :param policy_function: Function that takes a token and returns a set of filenames within idroot
        :return: A set of unique filepaths within the dataset indexed by idroot
        """
        idroot = idroot if idroot else self.idroot
        id_ext = id_ext if id_ext else self.id_ext
        policy_function = policy_function if policy_function else self._fuzzy_policy

        results, srcs = set(), set()

        indsets = self._threads.map(partial(policy_function, idroot=idroot, id_ext=id_ext), as_tokens(query))
        srcsets = [{src for srcset in self._threads.map(FileTreeSearcher.get_entries, indsets.pop()) for src in srcset}
                   for _ in range(len(indsets))]

        if inclusive:
            results = {src for _ in range(len(srcsets)) for src in srcsets.pop()}
        elif srcsets:
            results = srcsets.pop()
            for _ in range(len(srcsets)):
                results &= srcsets.pop()
                if not results:
                    break
        return results

    # IO Functions
    @staticmethod
    def get_entries(path: Union[str, os.PathLike[str]], unique: bool = True) -> Union[Set[str], List[str]]:
        """
        Pulls lines from the file at *path* into memory.

        :param path: Path to text file
        :param unique: Returns a **set** of *unique* lines in the file if True; else returns a **list** of *all* words
        :return: Tokens from the provided file.
        """
        lines = Path(path).read_text(encoding='utf-8', errors='surrogateescape').splitlines()
        return set(lines) if unique else lines

    # Policy Functions
    def fuzzyfindtoken(self, token, idroot: Union[str, os.PathLike[str]] = None, id_ext=None):
        """
        Policy Function that takes a token and returns a set of one (1) result when an 'exact' match is found.

        :param token: String to search for
        :param idroot: Path to index root directory
        :param id_ext: File extension of desired files (eg '.ind' for index files)
        :return: A set containing just the *index file* that corresponds to token
        """
        idroot = idroot if idroot else self.idroot
        id_ext = id_ext if id_ext else self.id_ext
        path, closest, _ = patricia_path(token, idroot if idroot else self.idroot)
        return set(
            closest.glob(f'**/*{id_ext if id_ext else self.id_ext}')
        ) if closest and len(path.parts) <= len(closest.parts) else set()

    def findtoken(self, token, idroot: Union[str, os.PathLike[str]] = None, id_ext=None):
        """
        Policy Function that takes a token and returns a set of unique filenames when a 'fuzzy' match is found.

        :param token: String to search for
        :param idroot: Path to index root directory
        :param id_ext: File extension of desired files (eg '.ind' for index files)
        :return: A set of filenames of *index files* that start with token
        """
        idroot = idroot if idroot else self.idroot
        id_ext = id_ext if id_ext else self.id_ext
        path, closest, _ = patricia_path(token, idroot if idroot else self.idroot)
        return set(closest.glob(f'*{id_ext if id_ext else self.id_ext}')) if path == closest else set()


class TarFileSearcher(PatriciaTrieSearcher):
    """Concrete Class For Searching a tape-archive-based PATRICIA Trie"""

    # Required Overloads
    def __init__(self, idroot, id_ext='.ind', searchdir=None):
        super().__init__(idroot, id_ext, searchdir)
        arc_ext = Path(self.idroot).suffix
        self.archive = tar.open(self.idroot, f"r{'|' + arc_ext[1:] if arc_ext else ''}", encoding='utf8')

    def __del__(self):
        super().__del__()
        self.archive.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        self.archive.close()

    def _fuzzy_policy(self, *args, **kwargs):
        return self.fuzzyfindsrcs(*args, **kwargs)

    def _exact_policy(self, *args, **kwargs):
        return self.findsrcs(*args, **kwargs)

    def _search(self, query: str, idroot: Union[str, os.PathLike[str]] = None, id_ext: str = None, inclusive: bool = False, policy_function=None):
        """
        Provides **core search functionality** for TarFileSearcher

        :param query: String to search for
        :param idroot: Path to archive file to search in
        :param id_ext: File extension of desired files (eg. '.ind' for index files)
        :param inclusive: Return results matching *any* token in the string when True; matching *all* tokens if False
        :param policy_function: Function that takes a token & returns the srcs from index files matching that token
        :return: A set of unique filepaths
        """
        idroot = idroot if idroot else self.idroot
        id_ext = id_ext if id_ext else self.id_ext
        policy_function = policy_function if policy_function else self._fuzzy_policy

        results, srcs = set(), set()

        srcsets = self._threads.map(partial(policy_function, idroot=idroot, id_ext=id_ext), as_tokens(query))

        if inclusive:
            results = {src for _ in range(len(srcsets)) for src in srcsets.pop()}
        elif srcsets:
            results = srcsets.pop()
            for _ in range(len(srcsets)):
                results &= srcsets.pop()
                if not results:
                    break
        return results

    # IO Functions
    def get_archive(self, idroot: Union[str, os.PathLike[str]] = None):
        """
        Return an open TarFile handle for reading. Note: It is the user's responsibility to close it when no longer in use.

        :param idroot: Path to archive file to open
        :return:
        """
        if idroot:
            arc_ext = Path(self.idroot).suffix
            return tar.open(self.idroot, f"r{':' + arc_ext[1:] if arc_ext else ''}", encoding='utf8')
        return self.archive

    # Policy Functions
    def _retrievesrcs(self, token: str, idroot:  os.PathLike = None, id_ext: str = None, fuzzy: bool = True, display: bool = False) -> Set[str]:
        """
         Retrieve relevant data from an archived PATRICIA Trie.

        :param token: Token to search trie for
        :param idroot: Path to directory or archive file containing root of PATRICIA Trie
        :param id_ext:  File Extension of relevant entries (eg. '.ind' for index files)
        :param fuzzy:   Match entries *starting with* token if *True*; Exact match otherwise.
        :param display: Display PATRICIA Trie structure to stdout as it's traversed
        :return: A set of unique lines pulled from files within the trie 
        """

        def get_records(archive_file, _token, level=0) -> None:
            """
            Recursively explores archive while incrementally searching for token.
            
            :param archive_file: File descriptor of archive file to search within
            :param _token: Remaining block of *token* yet to be matched
            :param level: Visual indent level for displaying trie structure
            :return: None (Results accumulated to _retrievesrcs.res by side-effect)
            """

            # Fuzzy match next set of entries most closely related to token.
            #   NOTE: _token == "" indicates token already found and now
            #         we're fuzzy matching words that start with token
            matches = sorted({
                                 member.name: member for member in archive_file.getmembers()
                                 if not _token or member.name[0] == _token[0]
                             }.items())

            if not fuzzy:  # Prioritize finding exact match
                for name, member in matches:
                    if Path(name).suffix == id_ext and Path(name).stem.strip('_') == token:
                        if display:
                            print(("\t" * level + '|__* ' if level else '') + name)
                        with archive_file.extractfile(member) as indfile:
                            res.update(indfile.read().decode('utf8').splitlines())
                        return  # Find one and done

            # Check other matching entries
            for name, member in matches:
                if display:
                    print(("\t" * level + '|__* ' if level else '') + name)
                if name.startswith(token) and name.endswith(id_ext):
                    # Found a Fuzzy Match
                    with archive_file.extractfile(member) as indfile:
                        res.update(indfile.read().decode('utf8').splitlines())
                else:
                    # Recurse through newly discovered archive
                    stem = name[:name.find('.')]
                    if (suffixes := Path(name).suffixes) and suffixes.pop(0) == '.tar':
                        _mode = 'r' + (f':{suffixes[0].strip(".")}' if suffixes else '')
                        with tar.open(fileobj=archive_file.extractfile(member), mode=_mode) as outer:
                            get_records(outer, _token[len(stem):] if stem != _token else '', level + 1)
            return

        res = set()
        idroot = idroot if idroot else self.idroot
        id_ext = id_ext if id_ext else self.id_ext
        archive = self.get_archive(idroot)
        get_records(archive, token)
        if archive != self.archive:
            archive.close()
        return res

    def fuzzyfindsrcs(self, token, idroot: Union[str, os.PathLike[str]] = None, id_ext=None):
        """
        Retrieves data from entries in the archive that start with token.

        :param token: Token to search archive for
        :param idroot: Archive file to search
        :param id_ext: Desired File extension of files to match (eg. '.ind' for index files)
        :return: Set of unique filepaths
        """
        return self._retrievesrcs(token, idroot, id_ext, fuzzy=True)

    def findsrcs(self, token, idroot: Union[str, os.PathLike[str]] = None, id_ext=None):
        """
        Retrieves data from entry in the archive that matches token.

        :param token: Token to search archive for
        :param idroot: Archive file to search
        :param id_ext: Desired File extension of files to match (eg. '.ind' for index files)
        :return: Set of unique filepaths
        """
        return self._retrievesrcs(token, idroot, id_ext, fuzzy=False)


def Searcher(idroot: Union[os.PathLike[str], str], id_ext: str = '.ind', searchdir: Union[os.PathLike[str], str] = None) -> RealTimeSearcher:
    """
    Factory Function for creating objects that implement the *RealTimeSearcher* interface.
    
    :param idroot: Path to directory/archive file to search within
    :param id_ext: File extension of relevant entries (eg. '.ind' for index files)
    :param searchdir: Path to directory of dataset (assuming idroot refers to a database of indexes within searchdir)
    :return: An instance of the appropriate concrete *Searcher class for searching idroot
    """
    if Path(idroot).is_dir():
        searcher = FileTreeSearcher(idroot, id_ext, searchdir)
    elif tar.is_tarfile(idroot):
        searcher = TarFileSearcher(idroot, id_ext, searchdir)
    else:
        searcher = None
    return searcher


async def searches_main(indroot: Union[str, os.PathLike[str]], query: str, searchdir: Union[str, os.PathLike[str]]) -> None:
    """
    Tester coroutine for searches.py

    :param indroot: Path to archive file to search within
    :param query: Phrase to search for
    :param searchdir: Path of dataset (assuming indroot is a database of index files for searchdir)
    :return: None
    """
    res = set()
    async with Stopwatch('Searching archive...', display=True):
        with Searcher(indroot, searchdir=searchdir) as searcher:
            res = searcher.searches_main(query)
    print(f'Found {len(res)} result(s).')
    for r in sorted(res):
        print('\t- ', r)


if __name__ == '__main__':
    query = 'good morning'
    searchdir = r'C:\Users\thecm\PycharmProjects\CasperCodingTest\enron_mail_20150507.tar\maildir'
    entrypoint = '.compressed/maildir.tar.gz'
    # entrypoint = searchdir
    asyncio.run(searches_main(entrypoint, query, searchdir))
