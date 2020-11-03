#  Copyright (c) 2020. Christopher J Maxwell <contact@christopherjmaxwell.com>
"""
Provides filesystem-related utility functions integral to emailsearcher.core modules
"""

from . import Union, os, Path, Pool, Optional, Tuple


def mp_rmdir(dirname: Union[os.PathLike, str]) -> None:
    """
    Faster version of shutil.rmtree using parallel processing.

    :param dirname: Path to filetree to delete
    :return: None (changes reflected on disk)
    """

    try:
        if Path(dirname).is_dir():
            os.rmdir(dirname)
        return
    except OSError:
        with Pool() as pp:
            try:
                # remove all files
                pp.map(os.remove,
                       (os.path.join(curr, filename) for curr, dirnames, filenames in os.walk(dirname, topdown=False)
                        for
                        filename in filenames))
                # remove empty directories
                for curr, dirnames, filenames in os.walk(dirname, topdown=False):
                    os.rmdir(curr)
            except OSError as e:
                print(e)
                pass
            pp.close()
            pp.join()


def transfer(src: Union[os.PathLike, str], destroot: Union[os.PathLike, str]) -> None:
    """
    Transfers a file located at *src* into its corrects spot within a PATRICIA Trie located at *destroot*

    :param src: Path of file to move
    :param destroot: Path to root of PATRICIA Trie
    :return: None (changes reflected on disk)
    """

    src = Path(src)
    token = str(src.stem).strip('_')
    target, closest, correction = patricia_path(token, destroot)
    src.replace(target / src.name)
    if correction:
        try:
            closest.replace(correction)
            closest.parent.rmdir()
        except OSError:
            pass
    try:
        src.parent.rmdir()
    except OSError:
        pass


def safeguard_path(path: Union[os.PathLike, str]) -> Optional[Path]:
    """
    Returns a corrected PATRICIA Path for given *path* accounting for Windows/DOS reserved paths.

    :param path: Path to correct as needed
    :return: A valid PATRICIA Path on Windows
    """

    safepath = Path(path)
    try:
        while safepath.is_reserved():
            safepath = safepath.parent / safepath.stem[0] / safepath.stem[1:]
    except IndexError as e:
        print(f'Could not produce valid Windows path for "{str(path)}"')
        safepath = None
    return safepath


def patricia_path(token: str, root: Union[os.PathLike, str]) -> Tuple[Optional[Path], Optional[Path], Optional[Path]]:
    """
    Finds the hypothetical path of *token* within the PATRICIA Trie at *root*.

    :param token: Token to find path for within the PATRICIA Trie
    :param root: Directory containing root of a PATRICIA Trie
    :return: 0) The hypothetical location of token within the Trie.
             1) The entry most similar to token currently present in the Trie.
             2) The hypothetical path of closest if token were to be added to the Trie.
    """

    token, root = str(token), Path(root)
    target, closest, correction = None, None, None
    for curr, dirs, files in os.walk(root):
        if token:
            dirs[:] = [d for d in dirs if d[0] == token[0]]
            if dirs:
                cp = os.path.commonprefix((token, dirs[0]))
                token = token[len(cp):] if cp != token else ''
                cp_path = safeguard_path(cp)
                target = safeguard_path(Path(curr) / cp_path / token)
                closest = Path(curr) / dirs[0]
                if cp != dirs[0]:
                    correction = safeguard_path(Path(curr) / cp_path / dirs[0][len(cp):])
                    break
            else:
                target = safeguard_path(Path(curr) / token)
                closest = Path(curr)
                break
        else:
            break
    return target, closest, correction
