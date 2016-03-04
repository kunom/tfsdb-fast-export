import re
import stat

class CommitCommand(object):

    def __init__(self, ref, mark, author, committer, message, from_ = None, merges = [], lineno=0, more_authors=None, properties=None):
        self.ref = ref
        self.mark = mark
        self.author = author
        self.committer = committer
        self.message = message
        self.from_ = from_
        self.merges = merges
        self.more_authors = more_authors
        self.properties = properties
        self.lineno = lineno
        # Provide a unique id in case the mark is missing
        self.id = format_mark(mark) if mark else (b'@%d' % lineno)

    def serialize(self, stream):
        stream.write(b"commit refs/heads/%s\n" % self.ref.encode('ascii'))

        if self.mark:
            serialize_mark(stream, self.mark)

        if self.author:
            stream.write(b"author %s\n" % format_who_when(self.author))
        if self.more_authors:
            for author in self.more_authors:
                stream.write(b"author %s\n" % format_who_when(author))

        stream.write(b"committer %s\n" % format_who_when(self.committer))

        serialize_data(stream, (self.message or "").encode('utf-8'))

        if self.from_:
            stream.write(b"from %s\n" % self.from_)
        if self.merges:
            for merge in self.merges:
                stream.write(b"merge %s\n" % merge)

        if self.properties:
            for name in sorted(self.properties):
                value = self.properties[name]
                stream.write(b"%s\n" + format_property(name, value))

class ProgressCommand():

    def __init__(self, message):
        self.message = message
        if "\n" in message:
            raise Exception("message must not contain any newlines")

    def serialize(self, stream):
        stream.write(b"progress %s\n" % self.message.encode('utf-8'))

class TagCommand(object):

    def __init__(self, id, from_, tagger, message):
        self.id = id
        self.from_ = from_
        self.tagger = tagger
        self.message = message

    def serialize(self, stream):
        stream.write(b"tag %s\nfrom %s\n" % (format_path(self.id), self.from_))
        stream.write(b"tagger %s\n" % format_who_when(self.tagger))
        serialize_data(stream, (self.message or "").encode('utf-8'))

class FileModifyCommand(object):

    def __init__(self, path, mode, dataref, data):
        if (dataref is None) == (data is None):
            raise Exception("please provide either dataref or data")

        self.path = check_path(path)
        self.mode = mode
        self.dataref = dataref
        self.data = data

    def _format_mode(self, mode):
        if mode in (0o755, 0o100755):
            return b"755"
        elif mode in (0o644, 0o100644):
            return b"644"
        elif mode == 0o40000:
            return b"040000"
        elif mode == 0o120000:
            return b"120000"
        elif mode == 0o160000:
            return b"160000"
        else:
            raise AssertionError("Unknown mode %o" % mode)

    def serialize(self, stream):
        if stat.S_ISDIR(self.mode):
            dataref = b'-'
        elif self.dataref is not None:
            dataref = self.dataref
        else:
            dataref = b"inline"

        stream.write(b"M %s %s %s\n" % (self._format_mode(self.mode), dataref, format_path(self.path)))
        if self.data is not None:
            serialize_data(stream, self.data)

class FileDeleteCommand(object):

    def __init__(self, path):
        self.path = check_path(path)

    def serialize(self, stream):
        stream.write(b"D %s\n" % format_path(self.path))

class FileCopyCommand(object):

    def __init__(self, src_path, dest_path):
        self.src_path = check_path(src_path)
        self.dest_path = check_path(dest_path)

    def serialize(self, stream):
        stream.write(b"C %s %s" % (format_path(self.src_path, quote_spaces=True), format_path(self.dest_path)))

class FileRenameCommand(object):

    def __init__(self, old_path, new_path):
        self.old_path = check_path(old_path)
        self.new_path = check_path(new_path)

    def serialize(self, stream):
        stream.write(b"R %s %s" % (format_path(self.old_path, quote_spaces=True), format_path(self.new_path)))

class FileDeleteAllCommand(object):

    def serialize(self, stream):
        stream.write(b"deleteall")

def format_mark(id):
    """Convert a numerical ID into a mark identifier."""

    return b":%d" % id

def check_path(path):
    """Check that a path is legal.

    :return: the path if all is OK
    :raise ValueError: if the path is illegal
    """

    if path is None or path == '' or path[0] == "/":
        raise ValueError("illegal path '%s'" % path)
    return path

def format_path(p, quote_spaces=False):
    """Format a path in utf8, quoting it if necessary."""

    if '\n' in p:
        p = re.sub('\n', '\\n', p)
        quote = True
    else:
        quote = p.startswith('"') or (quote_spaces and ' ' in p)
    if quote:
        p = '"' + p + '"'
    return p.encode('utf-8')

def format_who_when(fields):
    """Format a tuple of name,email,secs-since-epoch,utc-offset-secs as a string."""

    offset = fields[3]
    if offset < 0:
        offset_sign = b'-'
        offset = abs(offset)
    else:
        offset_sign = b'+'
    offset_hours = offset // 3600
    offset_minutes = (offset // 60) % 60

    name = fields[0]
    if name.endswith(" "):
        raise ValueError("name %r ends with space" % name)
    if len(name) == 0:
        sep = b''
    else:
        sep = b' '

    return b'%s%s<%s> %d %s%02d%02d' % (name.encode('utf-8'), sep, fields[1].encode("ascii"), fields[2], offset_sign, offset_hours, offset_minutes)

def format_property(name, value):
    """Format the name and value (both unicode) of a property as a string."""

    utf8_name = name.encode('utf8')
    if value is not None:
        utf8_value = value.encode('utf8')
        result = b"property %s %d %s" % (utf8_name, len(utf8_value), utf8_value)
    else:
        result = b"property %s" % utf8_name
    return result

def serialize_mark(stream, id):
    """Writes a mark command to the stream."""

    stream.write(b"mark %s\n" % format_mark(id))

class BlobFragmentIterator(object):
    """Helper class for blob data that is fragmented into multiple blocks."""

    def __init__(self, size, iterator):
        self.size = size
        self.iterator = iterator

    def __len__(self):
        return self.size

def serialize_data(stream, value):
    """Writes a data command to the stream. The given value can either be a bytes object, or
    a BlobFragmentIterator class."""

    stream.write(b"data %d\n" % len(value))

    if isinstance(value, bytes):
        stream.write(value)
    elif isinstance(value, BlobFragmentIterator):
        cnt = len(value)
        for b in value.iterator:
            cnt -= len(b)
            stream.write(b)
        if cnt:
            raise Exception("fragmented blob length mismatch (declared: {}, effective: {})".format(len(value), len(value) - cnt))
    else:
        raise Exception("unexpected value type {}".format(type(value)))

    stream.write(b"\n")
