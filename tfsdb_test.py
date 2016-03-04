import hashlib
import tfsdb
import unittest

class PeekableIteratorTests(unittest.TestCase):

    def test_peek(self):
        i = tfsdb.PeekableIterator([11, 22, 33])

        self.assertEqual(11, i.peek())
        self.assertEqual(11, i.peek()) # repeated peaking

        self.assertEqual(11, next(i))
        self.assertEqual(22, next(i))

        self.assertEqual(33, i.peek())

        self.assertEqual(33, next(i))

        self.assertRaises(StopIteration, i.peek)

    def test_iterable(self):
        list(tfsdb.PeekableIterator([11])) # should not fail

class MD5ValidatingIteratorTest(unittest.TestCase):

    def test_check_success_and_mismatch(self):
        c = b'12345'
        h = self.calc_hash(c)

        # run against correct checksum
        list(tfsdb.MD5ValidatingIterator(h, [c])) # should not fail

        # run against modified checksum
        with self.assertRaises(Exception) as cm:
            list(tfsdb.MD5ValidatingIterator(h + b'--', [c]))

        self.assertTrue('checksum' in str(cm.exception))

    @staticmethod
    def calc_hash(content):
        return hashlib.md5(content).digest()

if __name__ == '__main__':
    unittest.main(verbosity = 2)