import msdelta
import unittest

class MSDeltaTests(unittest.TestCase):

    def test_CreateDeltaB(self):
        delta = msdelta.CreateDeltaB(b'some', b'somewhere')
        
        self.assertIsNotNone(delta)

    def test_ApplyDeltaB_roundtrip(self):
        delta = msdelta.CreateDeltaB(b'some', b'somewhere')
        new = msdelta.ApplyDeltaB(b'some', delta)

        self.assertEqual(b'somewhere', new)

if __name__ == '__main__':
    unittest.main(verbosity = 2)