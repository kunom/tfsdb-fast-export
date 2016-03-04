import os
import tempdir
import unittest

class TempDirTests(unittest.TestCase):

    def test_creation_and_cleanup(self):
        with tempdir.TempDir() as td:
            location = td.location

            self.assertEqual(True, os.path.exists(location))
        self.assertEqual(False, os.path.exists(location))

    def test_creation_namecollision(self):
        with tempdir.TempDir(".td-test") as td:
            self.assertRaises(Exception, lambda: tempdir.TempDir(".td-test"))

        self.assertEqual(False, os.path.exists(".td-test"))

    def test_create_exists(self):
        with tempdir.TempDir() as td:
            self.assertFalse(td.exists("a"))

            td.create("a")
            self.assertTrue(td.exists("a"))

    def test_read_delete_at_end(self):
        with tempdir.TempDir() as td:
            td.create("a")
            self.assertTrue(td.exists("a"))

            for b in td.read("a"):
                pass
            self.assertTrue(td.exists("a"))

            for b in td.read("a", delete_at_end = True):
                pass
            self.assertFalse(td.exists("a"))

if __name__ == '__main__':
    unittest.main(verbosity = 2)