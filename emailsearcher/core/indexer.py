#  Copyright (c) 2020. Christopher J Maxwell <contact@christopherjmaxwell.com>
"""
**Core functionalities**:
    **- Generate index files** for tokens found in text files under a given directory & its subfolders.

    **- Create/Organize index file PATRICIA-Trie**-based directory structure **on disk**.
    `(Practical Algorithm To Retrieve Information Coded in Alphanumeric)`__
__ https://dl.acm.org/doi/10.1145/321479.321481
"""

from . import Union, os, tokenize, Path, Set, safeguard_path, ThreadingLock, RLock, Event, ThreadPool, Pool, Stopwatch, \
    mp_rmdir, asyncio, transfer, partial, cpu_count, Manager, sleep


def broadcast_tokens(filepath: Union[os.PathLike, str], writeQ, searchdir: Union[os.PathLike, str] = None) -> Set[str]:
    """
    Tokenizes a file putting the filepath (relative to searchdir) & the set of tokens onto the provided queue as a Tuple.

    :param filepath: Path of file to tokenize
    :param writeQ:  Queue to put the tokenset onto
    :param searchdir: Path to compare filepath against
    :return: A set of unique tokens found in filepath
    """
    tokens = tokenize(filepath := Path(filepath))
    filepath = filepath.relative_to(Path(searchdir)) if searchdir else filepath
    writeQ.put((filepath, tokens))
    return tokens


def index_tokenset(tokenset: Set[str], root: Union[os.PathLike, str], npatriciarized_token_q=None) -> None:
    """
    Recursively generates a PATRICIA Trie from a given tokenset in the form of a Directory tree rooted at the provided directory.

    :param tokenset: A set of tokens to organize into a PATRICIA Trie
    :param root: A directory to create the Trie in
    :param npatriciarized_token_q: [Optional] A progress queue that will be sent a 1 when a token is added to the trie
    :return: None
    """
    root = Path(root)
    if cp := os.path.commonprefix(list(tokenset)):
        root = safeguard_path(root / cp)
        os.makedirs(root, exist_ok=True)
        if npatriciarized_token_q and cp in tokenset:
            npatriciarized_token_q.put(1)
        index_tokenset({token[len(cp):] for token in tokenset if token != cp}, root, npatriciarized_token_q)
    else:
        while tokenset:
            nextTok = tokenset.pop()
            nextGen = {token for token in tokenset if token[0] == nextTok[0]}
            tokenset -= nextGen
            nextGen.add(nextTok)
            index_tokenset(nextGen, root, npatriciarized_token_q)


def writer(writeQ, tmpdir: Union[os.PathLike, str], threshold: int = 10 ** 3,
           writer_lock: ThreadingLock = ThreadingLock(), sentinel=None) -> None:
    """
    Organizes, compiles, & writes entries from the queue to their respective index files.

    :param writeQ: A queue to pull requests from
    :param tmpdir: Directory to store index files in before the trie is complete.
    :param threshold: # of entries this process is allowed to hold in memory before writing to disk.
    :param writer_lock: A threading.Lock (or multiprocessing.Manager.Lock)
            to prevent multiple writer processes from writing to the same sets of files at once.
    :param sentinel: Process will stop pulling from the queue when this value is received.
    :return: None (Changes reflected on disk)
    """

    class IndFile:
        """Wrapper class around a set of entries and a multiprocessing.RLock"""

        def __init__(self):
            self.access = RLock()
            self.srcs = set()

        def __enter__(self):
            self.access.acquire()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.access.release()

    def poll(_) -> None:
        """
        Pulls requests off the queue and organizes them in a cache for this process until writer.sentinel is received.

        :param _: Not used. (Only exists to simplify using this function with a *map method)
        :return: None (results added to writer.pendingwrites by side effect)
        """
        while (request := writeQ.get()) != sentinel:
            for _ in range(nentries := len(request[1])):
                greenlight.wait()  # waits until greenlight.is_set() == True

                # Add entry in pendingwrites for the token (if needed)
                token = request[1].pop()
                pendingwrites[token] = pendingwrites.setdefault(token, IndFile())

                # Add filepath to corresponding entry's srcs set
                with pendingwrites[token]:
                    pendingwrites[token].srcs.add(str(request[0]))
            with ecaccess:
                nwaiting[0] += nentries
        writeQ.put(sentinel)

    def flush(token: str, pending_write: IndFile) -> None:
        """
        Writes elements of *pending_write.srcs* to the index file associated with *token*.

        :param token: A token to be indexed.
        :param pending_write: An IndFile obj containing the set of srcs to write to file
                + the lock that provides this thread with exclusive access to that set of srcs.
        :return: None (Changes reflected on disk.)
        """
        with pending_write, (tmpdir / (token + '_.ind')).open('a', encoding='utf8', errors='surrogateescape') as fl:
            for _ in range(len(pending_write.srcs)):
                print(str(pending_write.srcs.pop()), file=fl)

    tmpdir = Path(tmpdir)
    tmpdir.mkdir(parents=True, exist_ok=True)
    ecaccess = RLock()
    pendingwrites = {}
    greenlight = Event()
    greenlight.set()
    nwaiting = [0]  # Containers/Objects are the only mutable external variables when implicitly captured by subroutines

    with ThreadPool() as threads:
        polls = threads.map_async(poll, range(cpu_count() // 2))
        while not polls.ready():
            with ecaccess:
                if nwaiting[0] > threshold and writer_lock.acquire(blocking=False):
                    greenlight.clear()
                    sleep(1 / 10 ** 6)  # Allow time for polls to hit the wait

                    threads.starmap(flush, (pendingwrites.popitem() for _ in range(len(pendingwrites))))
                    writer_lock.release()
                    nwaiting[0] = 0

                    greenlight.set()
                sleep(1 / 10 ** 6)  # Allow time for scheduler to switch threads (helps balance load)
        with writer_lock:
            threads.starmap(flush, (pendingwrites.popitem() for _ in range(len(pendingwrites))))


def index_directory(searchdir: Union[os.PathLike, str], inddir: Union[os.PathLike, str] = '.ind',
                    tmpdir: Union[os.PathLike, str] = '.tmp') -> None:
    """
    Uses a process pool to create a PATRICIA Trie of index files for *searchdir* under *inddir*.

    :param searchdir: Path to Directory to be indexed.
    :param inddir: Path to create PATRICIA Trie under.
    :param tmpdir: Path to store index files under while trie is being constructed.
    :return: None (changes reflected on disk)
    """
    with Pool() as processes, Manager() as shared:
        writeQ = shared.Queue()  # Process-Safe Queue
        writer_lock = shared.Lock()  # Process-Safe ThreadingLock
        npatriciarized_token_q = shared.Queue()  # Process-Safe Queue

        # Start writer processes
        writerres = processes.starmap_async(partial(writer, writer_lock=writer_lock),
                                            (((writeQ, tmpdir) for _ in range(cpu_count() // 2))))

        # Lazily uses processes to get sets of tokens from all files under searchdir (and its subfolders)*
        #  *Token sets will be broadcast to the writer processes as they're created.
        #   (Lazy eval used to conserve memory)
        tokensets = processes.imap_unordered(partial(broadcast_tokens, searchdir=searchdir, writeQ=writeQ),
                                             (Path(curr) / filename for curr, dirs, files in os.walk(searchdir) for
                                              filename in files))

        # Only keep unique tokens for Trie building step
        tokens = {token for tokenset in tokensets for token in tokenset}

        # Group the tokens by first letter (For parallel processing)
        by_first_letter = {}
        ntokens = len(tokens)
        for _ in range(len(tokens)):
            token = tokens.pop()
            by_first_letter[token[0]] = by_first_letter.setdefault(token[0], set())
            by_first_letter[token[0]].add(token)

        # Generator produces tuples of args for index_tokenset, destructing by_first_letter as it goes
        args_gen = (
            (by_first_letter.popitem()[1], inddir, npatriciarized_token_q)
            for _ in range(len(by_first_letter))
        )

        # Use processes to build PATRICIA Trie branches in parallel*
        # *(a caveat of PATRICIA Tries is folders w/ diff 1st letters can be built concurrently)
        smrf = processes.starmap_async(index_tokenset, args_gen)

        # [translator's note: smrf stands for StarMap Result Future] (...but also the little blue things
        # because I like animation and my IDE displays comments in light blue...)

        # Progress indicator for Trie Building (could be replaced w/ a single wait but
        #   this is prettier & keeps you engaged while you waste your youth
        #   staring at a console...)
        nexamined = 0
        while not smrf.ready():
            while not npatriciarized_token_q.empty():
                nexamined += npatriciarized_token_q.get()
                print(f'\rBuilding directory structure: {nexamined / ntokens:.4%}...', end='')
        print(f'\rFinished structure build. {nexamined / ntokens:.4%} of tokens patricia-rized')

        # Send the sentinel to the writer processess
        # (each writer echos the sentinel to the queue as it shuts down
        #   so the rest of the writers can know its time to wrap it up)
        writeQ.put(None)
        writerres.get()  # waits for all writers to shutdown
        while not writeQ.empty():
            writeQ.get()  # clear the last copy of the sentinel floating in the queue after writers shutdown

        # Use (upto) the # of cores on your machine to
        # transfer the index files into their correct spots in the Trie.
        processes.map(
            partial(transfer, destroot=inddir), Path(tmpdir).iterdir(),
            chunksize=max(1, ntokens//cpu_count())
        )


async def indexer_main(searchdir: Union[os.PathLike, str], inddir: Union[os.PathLike, str] = '.ind',
                       tmpdir: Union[os.PathLike, str] = '.tmp'):
    """
    Tester coroutine for indexer.py

    :param searchdir: Directory to index
    :param inddir: Directory to create PATRICIA Trie under
    :param tmpdir: Directory to keep index files in until Trie is built.
    :return: None
    """
    async with Stopwatch('Removing existing dirs...'):
        mp_rmdir(inddir)
        mp_rmdir(tmpdir)
    print()

    async with Stopwatch('Indexing files...'):
        index_directory(searchdir, inddir, tmpdir)
    print()


if __name__ == '__main__':
    asyncio.run(indexer_main('../../../MPLearning/dataset'))
