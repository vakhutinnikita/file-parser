import gzip
import re
import pathlib
from typing import List, Dict, Tuple, Optional, Union, Iterator, TextIO


def create_document(doc_lines: List[Tuple[Optional[str], str]]) -> Dict[str, str]:
    """ Creates document object from the list of lines

    :param doc_lines: List of document lines
    :type doc_lines: List[Tuple[Optional[str], str]]
    :return: Document object
    :rtype: Dict[str, str]
    """

    doc: Dict[str, str] = {}
    key: Optional[str] = None
    for line in doc_lines:
        key = line[0] or key
        if key not in doc:
            doc[key] = ''
        else:
            doc[key] += '\n'
        doc[key] += line[1]
    return doc


def process_file(file: TextIO) -> Iterator[Dict[str, str]]:
    """ Iterates opened file and yields documents

    :param file: Opened file
    :type file: TextIO
    :return: File documents
    :rtype: Iterator[Dict[str, str]]
    """

    document_lines: List[Tuple[Optional[str], str]] = []

    while True:
        line: str = file.readline()
        if not line:
            break

        # skip comments
        if line.startswith('#'):
            continue

        # end of document?
        if not line.strip():
            if document_lines:
                yield create_document(document_lines)
                document_lines = []
            continue

        # check if key on line
        if re.match(r'^[\S]+:.*', line):
            key, value = line.split(':')
            document_lines.append((key.strip(), value.strip()))
        else:
            document_lines.append((None, line.strip()))


def parse_file(path: Union[str, pathlib.Path]) -> Iterator[Dict[str, str]]:
    """ Yields processed documents from the file

    :param path: Path to input file
    :type path: Union[str, pathlib.Path]
    :return: File documents
    :rtype: Iterator[Dict[str, str]]
    """

    try:
        with gzip.open(path, 'rt') as file:
            for doc in process_file(file):
                yield doc

    except OSError:
        with open(path) as file:
            for doc in process_file(file):
                yield doc
