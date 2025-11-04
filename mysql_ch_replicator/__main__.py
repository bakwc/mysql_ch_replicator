#!/usr/bin/env python3
"""
Entry point for running mysql_ch_replicator as a module.
This file enables: python -m mysql_ch_replicator
"""

from .main import main

if __name__ == '__main__':
    main()
