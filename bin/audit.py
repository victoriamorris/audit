#!/usr/bin/env python
# -*- coding: utf8 -*-

"""A tool to perform an audit of the FULL catalogue in Catalogue Bridge."""

import audit
import sys

__author__ = 'Victoria Morris'
__license__ = 'MIT License'
__version__ = '1.0.0'
__status__ = '4 - Beta Development'

audit.main(sys.argv[1:])
