import unittest
from flowsa.common import getFIPS

class TestFIPS(unittest.TestCase):

    def test_bad_state(self):
        state = "gibberish"
        self.assertIs(getFIPS(state=state),None)

    def test_bad_county(self):
        state = "Wyoming"
        county = "Absaroka"
        self.assertIs(getFIPS(state=state,county=county),None)

    def test_good_state(self):
        state = "Georgia"
        self.assertEqual(getFIPS(state=state),"13000")

    def test_good_county(self):
        state = "IOWA"
        county = "Dubuque"
        self.assertEqual(getFIPS(state=state,county=county), "19061")
