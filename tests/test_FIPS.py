# test_FIPS.py (tests)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""Add docstring in public module."""  # TODO add docstring.

import unittest
from flowsa.common import getFIPS


class TestFIPS(unittest.TestCase):
    """Add docstring in public class."""  # TODO add docstring.

    def test_bad_state(self):
        """Add docstring in public method."""  # TODO add docstring.
        state = "gibberish"
        self.assertIs(getFIPS(state=state), None)

    def test_bad_county(self):
        """Add docstring in public method."""  # TODO add docstring.
        state = "Wyoming"
        county = "Absaroka"
        self.assertIs(getFIPS(state=state, county=county), None)

    def test_good_state(self):
        """Add docstring in public method."""  # TODO add docstring.
        state = "Georgia"
        self.assertEqual(getFIPS(state=state), "13000")

    def test_good_county(self):
        """Add docstring in public method."""  # TODO add docstring.
        state = "IOWA"
        county = "Dubuque"
        self.assertEqual(getFIPS(state=state, county=county), "19061")
