import tempfile
import unittest
import os
import signal

from recoverable import RecoverableFunction, recoverable


class RecoverableTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.dirpath = self.tmpdir.name

    def tearDown(self):
        self.tmpdir.cleanup()

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
        rd_fd, wr_fd = os.pipe()

        @recoverable(self.dirpath)
        def block(s: bytes) -> str:
            os.close(rd_fd)
            os.write(wr_fd, b'1')
            os.close(wr_fd)
            input()

        pid = os.fork()
        if pid == 0:
            block(b'blocked')
        else:
            os.close(wr_fd)
            os.read(rd_fd, 1)
            os.close(rd_fd)
            os.kill(pid, signal.SIGKILL)
            os.wait()
            self.assert_content_unique_file(b'blocked')

    def get_first_file_path(self):
        listd = os.listdir(self.dirpath)
        return os.path.join(self.dirpath, listd[0])

    def test_file_is_locked(self):
        rd_fd, wr_fd = os.pipe()

        @recoverable(self.dirpath)
        def block(s: bytes) -> str:
            os.close(rd_fd)
            os.write(wr_fd, b'1')
            os.close(wr_fd)
            input()

        pid = os.fork()
        if pid == 0:
            block(b'blocked')
        else:
            os.close(wr_fd)
            os.read(rd_fd, 1)
            os.close(rd_fd)

            path = self.get_first_file_path()
            with self.assertRaises(BlockingIOError):
                os.open(path, os.O_RDONLY | os.O_EXLOCK | os.O_NONBLOCK)

            os.kill(pid, signal.SIGKILL)
            os.wait()

    def test_filename_collision(self):
        path = os.path.join(self.dirpath, 'constant')
        with open(path, 'w') as f:
            f.write('hello')

        class RFConstantFilename(RecoverableFunction):
            def generate_filename(self) -> str:
                return 'constant'

        def failure(s: bytes) -> str:
            raise ValueError

        function = RFConstantFilename(self.dirpath, failure)
        try:
            function(b'blabla')
        except ValueError:
            pass
        self.assert_content_unique_file(b'hello')

    def test_recovering_locked_file_should_fail(self):
        path = os.path.join(self.dirpath, 'constant')
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXLOCK)

        @recoverable(self.dirpath)
        def success(s: bytes) -> str:
            return 'success'

        with self.assertRaises(BlockingIOError):
            success.recover_from_filename('constant')
