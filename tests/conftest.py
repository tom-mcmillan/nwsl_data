"""Test fixtures & path setup for nwsl_data."""

import os
import sys

# Add project roots to sys.path BEFORE importing project/third-party modules
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
for rel in ("src", "scripts", os.path.join("scripts", "testing")):
    sys.path.insert(0, os.path.join(ROOT, rel))


# (fixtures can go below)
