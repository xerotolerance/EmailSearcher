#  Copyright (c) 2020. Christopher J Maxwell <contact@christopherjmaxwell.com>
"""
Initializes the emailsearcher.core package.

- Provides import sources for modules in emailsearcher.core
- Pulls resources from modules in emailsearcher.core into a single namespace for exporting
"""

# Imports
from ..common import ABC, abstractmethod, os, Path, partial, tar, ThreadingLock, RLock, Event, cpu_count, Manager, Optional
from ..concurrency import *
from ..utility import *

# Exports
from .archiver import *
from .indexer import *
from .searches import *

