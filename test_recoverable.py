import tempfile
import unittest
import os
import signal

from recoverable import RecoverableFunction, recoverable


class RFConstantFilename(RecoverableFunction):
    def generate_filename(self) -> str:
        return 'constant'


class RecoverableTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.dirpath = self.tmpdir.name

    def test_return_right_value(self):
        @recoverable(self.dirpath)
        def success(s: bytes) -> str:
            return 'success'
        self.assertEqual('success', success(b'test'))

    def test_no_file_on_success(self):
        @recoverable(self.dirpath)
        def success(s: bytes) -> str:
            return 'success'
        self.assertEqual(0, len(os.listdir(self.dirpath)))

    def test_raises_exception(self):
        @recoverable(self.dirpath)
        def failure(s: bytes) -> str:
            raise ValueError
        with self.assertRaises(ValueError):
            failure(b'test')

    def assert_content_unique_file(self, expect: bytes):
        listd = os.listdir(self.dirpath)
        self.assertEqual(1, len(listd))
        path = os.path.join(self.dirpath, listd[0])
        with open(path, 'rb') as f:
            content = f.read()
        self.assertEqual(content, expect)

    def test_saves_content_on_failure(self):
        @recoverable(self.dirpath)
        def failure(s: bytes) -> str:
            raise ValueError

        try:
            failure(b'failure')
        except ValueError:
            pass
        self.assert_content_unique_file(b'failure')

    def test_content_saved_when_process_killed(self):
        @recoverable(self.dirpath)
        def block(s: bytes) -> str:
            input()

        fd = os.fork()
        if fd == 0:
            block(b'blocked')
        else:
            os.kill(fd, signal.SIGKILL)
            os.wait()
            self.assert_content_unique_file(b'blocked')

    def tearDown(self):
        self.tmpdir.cleanup()
