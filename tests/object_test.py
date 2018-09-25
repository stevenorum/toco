import unittest

class TestObjectMethods(unittest.TestCase):

    def test_noop(self):
        self.assertEqual(True, True)

if __name__ == '__main__':
    unittest.main()
