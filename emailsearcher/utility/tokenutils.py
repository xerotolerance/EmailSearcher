#  Copyright (c) 2020. Christopher J Maxwell <contact@christopherjmaxwell.com>
"""
Provides text-processing utilities crucial to emailsearcher.core modules
"""

from . import Generator, Any, Set, Path, os, punctuation


def as_tokens(text: str, unique: bool = True) -> Generator[str, Any, None]:
    """
    Yields tokens from *text* after normalizing & case-folding string. Treats punctuation as whitespace.

    :param text: String to tokenize
    :param unique: Only yield unique tokens (in random order) if True; else yield all tokens in order of appearance
    :return: A generator object which yields tokens in *text*
    """

    tokens = ''.join(c for c in text if c.isspace() or c.isprintable()).translate(
        str.maketrans(punctuation, ' ' * len(punctuation))).casefold().split()
    tokens = set(tokens) if unique else tokens
    return (token for token in tokens)


def tokenize(filepath: os.PathLike) -> Set[str]:
    """
    Tokenizes a file, normalizing & case-folding the text. Treats punctuation as whitespace.

    :param filepath: Path of file to tokenize
    :return: A set of unique tokens found in the file.
    """

    filepath = Path(filepath)
    return set(as_tokens(filepath.read_text(encoding='utf8', errors='surrogateescape')))
