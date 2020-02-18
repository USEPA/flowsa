import unittest
from flowsa.common import getFIPS

"""Add docstring in public module."""

class TestFIPS(unittest.TestCase):
    """Add docstring in public class."""

    def test_bad_state(self):
        """Add docstring in public method."""
        state = "gibberish"
        self.assertIs(getFIPS(state=state), None)

    def test_bad_county(self):
        """Add docstring in public method."""
        state = "Wyoming"
        county = "Absaroka"
        self.assertIs(getFIPS(state=state, county=county), None)

    def test_good_state(self):
        """Add docstring in public method."""
        state = "Georgia"
        self.assertEqual(getFIPS(state=state), "13000")

    def test_good_county(self):
        """Add docstring in public method."""
        state = "IOWA"
        county = "Dubuque"
        self.assertEqual(getFIPS(state=state, county=county), "19061")
