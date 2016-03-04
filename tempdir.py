import io
import os
import shutil
import tempfile
import time

class TempDir(object):
    """Specific temporary directory implementation with helper methods suited
    for the TFS fast exporter."""

    def __init__(self, location = None, clear_location_if_existing = False):
        if location:
            # cleanup
            if os.path.exists(location):
                if not clear_location_if_existing:
                    raise Exception("temporary directory location '{}' already exists".format(location))
                else:
                    shutil.rmtree(location)
                    time.sleep(1) # maybe prevents an access violation
            # create
            if not os.path.exists(location):
                os.mkdir(location)
        else:
            location = tempfile.mkdtemp()

        self.location = location
        self.subdirs = {}

    def cleanup(self):
        shutil.rmtree(self.location)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.cleanup()

    def get_path(self, name):
        """Creates from the given name a storage location."""

        if not isinstance(name, str):
            name = str(name)
        if '..' in name:
            raise Exception("name '{}' must not contain parent dir navigation".format(name))

        subdir_key = hash(name) % 256

        if subdir_key in self.subdirs:
            subdir_path = self.subdirs[subdir_key]
        else:
            subdir_path = os.path.join(self.location, "{:02X}".format(subdir_key))
            
            os.mkdir(subdir_path)
            self.subdirs[subdir_key] = subdir_path

        return os.path.join(subdir_path, name)

    def exists(self, name):
        """Indicates whether the given file exists or not."""

        return os.path.exists(self.get_path(name))

    def create(self, name, content = None):
        """Creates a new file and fill it, when given, with content. Returns the full path of the new file."""

        path = self.get_path(name)

        with io.open(path, 'wb') as f:
            if content:
                if isinstance(content, bytes):
                    f.writelines(content)
                else:
                    for b in content:
                        f.write(b)

        return path

    def read(self, name, block_size = 1000000, delete_at_end = False):
        """Reads the given file in blocks. Deltes the file after streaming the full content."""

        path = self.get_path(name)

        with io.open(path, "rb") as f:
            while True:
                block = f.read(block_size)
                if not block:
                    break
                yield block
        
        if delete_at_end:
            os.unlink(path)