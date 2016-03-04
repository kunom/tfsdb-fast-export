import adodbapi
import collections
import datetime
import fastimport
import functools
import hashlib
import itertools
import msdelta
import tempdir
import zlib

msdelta_decompress_on_disk_threshold = 10000000
oversize_warning_limit = 10000000

# Tools
# -----

def make_iterable(value):
    """Turns a collection into an iterable. Does nothing if the value given is already an iterable."""

    if hasattr(value, "__next__") or hasattr(value, "next"):
        return value

    return iter(value)

class PeekableIterator(object):
    """An itertator that allows peeking at the next object without actually consuming it."""

    def __init__(self, coll_or_iter):
        self.iter = make_iterable(coll_or_iter)
        self._peeked = []

    def __iter__(self):
        return self

    def peek(self):
        """Returns the next element to be iterated over. Raises a StopIteration exception
        if there is no next element."""

        if not self._peeked:
            self._peeked.append(next(self.iter))

        return self._peeked[0]

    def __next__(self):
        if self._peeked:
            return self._peeked.pop(0)
        else:
            return next(self.iter)

def build_keyed_dict(items, key_extractor, value_transformer = None):
    """Groups a list of items by keys as returned by the key extractor function."""

    tmp = collections.defaultdict(list)
    for i in items:
        tmp[key_extractor(i)].append(value_transformer(i) if value_transformer else i)
    return tmp

def select(conn, query, *args, **kwargs):
    """Helper method to easily use the ADODBAPI to select one ore more records"""

    with conn.cursor() as cur:
        cur.execute(query, *args, **kwargs)
        while True:
            row = cur.fetchone()
            if not row:
                break;
            yield row

def selectone(conn, query, *args, **kwargs):
    """Helper method to easily use the ADODBAPI when exactly a single record is expected"""
    result = None

    for i, row in enumerate(select(conn, query, *args, **kwargs)):
        if not i:
            result = row
        else:
            raise Exception("received more that one record")

    if not result:
        raise Exception("empty result")

    return result

# TFS Utils
# ---------

def tfs_unmangle_path(path):
    """Tfs internally modifies path names, most probably in order to simplify LIKE queries. Reverse that modification."""

    tmp = path.replace(">", "_").replace('"', '-').replace('|', '%')

    if tmp.endswith("\\"):
        tmp = tmp[:-1]

    return tmp

def tfs_unmangle_timestamp(ts):
    # TFS timestamps are UTC
    return ts.replace(tzinfo = datetime.timezone.utc)

def tfs_decompress(compression_type, blockiter):
    """Decompresses a blob series."""

    if compression_type == 0: # uncompressed
        return blockiter

    if compression_type == 1: # GZIP
        # see http://stackoverflow.com/questions/2423866/python-decompressing-gzip-chunk-by-chunk
        d = zlib.decompressobj(zlib.MAX_WBITS | 16)

        return (d.decompress(b) for b in blockiter)

    raise Exception("unexpected compression type {}".format(compression_type))

# Internal object model
# ---------------------

RowRelPath = collections.namedtuple("RowRelPath", ["row", "relpath"])

def split_and_filter_file_rows(rows, hooks, full_path_hook = lambda row: row.FullPath, return_rel_paths = True):
    result = collections.defaultdict(list)

    branch_extract = hooks.branch_extract
    file_filter = hooks.file_filter

    for row in rows:
        branch, relpath = branch_extract(tfs_unmangle_path(full_path_hook(row)))
        if not branch or (relpath and not file_filter(branch, relpath)):
            continue

        result[branch].append(RowRelPath(row, relpath) if return_rel_paths else row)

    return result

class MD5ValidatingIterator(object):
    """Wraps iteration over a sequence of bytes instances and calculates
    the MD5 checksum. At the end of the iteration, compares that sum with
    the checksum given in the constructor."""

    def __init__(self, checksum, coll_or_iter, context = None):
        self.expected_checksum = checksum
        self.iter = make_iterable(coll_or_iter)
        self.context = context

        self.running_checksum = hashlib.md5()

    def __iter__(self):
        return self

    def __next__(self):
        try:
            tmp = next(self.iter)
            self.running_checksum.update(tmp)
            return tmp
        except StopIteration:
            if self.running_checksum.digest() != self.expected_checksum:
                raise Exception("checksum mismatch (in context: {})".format(self.context))
            raise

class FileOperation(object):
    """Represents a file operation inside of a single commit."""

    def __init__(self, conn, id, fullpath):
        self.conn = conn
        self.id = id
        self.fullpath = fullpath

class FileContentChange(FileOperation):
    """Represents a file content change inside of a single commit."""

    def __init__(self, conn, tempdir, id, fullpath, file_length, compressed_length, compression_type, content_type, content_hash, content_blocks_cb):
        super().__init__(conn, id, fullpath)
        self.tempdir = tempdir
        self.file_length = file_length
        self.compressed_length = compressed_length
        self.compression_type = compression_type
        self.content_type = content_type
        self.content_hash = content_hash
        self.content_blocks_cb = content_blocks_cb

    def content(self):
        """Returns the file content in byte blocks"""

        # see also:
        # http://stackoverflow.com/questions/834118/how-do-you-get-a-file-out-of-the-tbl-content-table-in-tfs

        # deltification
        if self.content_type == 1: # full text
            blocks = tfs_decompress(self.compression_type, self.content_blocks_cb())

        elif self.content_type == 2: # MSDelta
            if not self.tempdir.exists(self.id):
                self._unpack_deltas_to_tempdir()

            blocks = self.tempdir.read(self.id, delete_at_end = True)

        else:
            raise Exception("unexpected content type {} for file {}".format(self.content_type, self.id))

        # conssitency check (to see whether we got the decompression / undeltification right)
        return MD5ValidatingIterator(self.content_hash, blocks, context = self.id)

    def _unpack_deltas_to_tempdir(self):
        # TFS seems to store the latest version as full dump and older versions as backwards diff.
        # There are entries with VersionFrom equals to NULL. They have to be ignored.

        rows = select(self.conn, """
            select f1.*, c.*
            from tbl_file f0
            inner join tbl_file f1 
	            on f1.ItemId = f0.ItemId 
	            and f1.FileId >= f0.FileId 
	            and f1.FileId <= (select min(f2.FileId) from tbl_file f2 where f2.ItemId = f0.ItemId and f2.FileId > f0.FileId and f2.VersionFrom is not null and f2.ContentType = 1 /*full content*/)
            inner join tbl_Content c
	            on c.FileId = f1.FileId
                and f1.VersionFrom is not NULL
            where f0.FileId = ?
            order by f1.FileId desc, c.OffsetFrom""",
            [self.id])

        rowsByFileId = itertools.groupby(rows, lambda r: r.FileId)

        if self.file_length > msdelta_decompress_on_disk_threshold:
            # patching is disk based
            deltaBase = None
            for fileId, fileRows in rowsByFileId:
                fileRows = PeekableIterator(fileRows)
                compression_type = fileRows.peek().CompressionType

                if deltaBase is None:
                    deltaBase = self.tempdir.create(fileId, tfs_decompress(compression_type, (r.Content for r in fileRows)))
                else:
                    newFile = self.tempdir.get_path(fileId)
                    fdelta = self.tempdir.create("delta", (r.Content for r in fileRows))

                    msdelta.ApplyDelta(deltaBase, fdelta, newFile)
                    deltaBase = newFile
        else:
            # memory based (but still save the versions to disk, for further use)
            deltaBase = None
            for fileId, fileRows in rowsByFileId:
                fileRows = PeekableIterator(fileRows)
                compression_type = fileRows.peek().CompressionType

                if deltaBase is None:
                    deltaBase = b''.join(tfs_decompress(compression_type, (r.Content for r in fileRows)))
                else:
                    delta = b''.join(r.Content for r in fileRows)
                    deltaBase = msdelta.ApplyDeltaB(deltaBase, delta)

                    self.tempdir.create(fileId, [deltaBase])

class User(object):
    """Represents an user identity."""

    def __init__(self, tfs_id, domain, login, display_name):
        self.tfs_id = tfs_id
        self.domain = domain
        self.login = login
        self.display_name = display_name

    @property
    def qualifiedlogin(self):
        return "{}\{}".format(self.domain, self.login)

    def __str__(self):
        return "{} [{}]".format(self.qualifiedlogin, self.tfs_id)

class Changeset(object):
    """Represents an individual changeset."""

    # tbl_version: 
    #  * names invariant: v.FullPath == v.ParentPath + v.ChildItem
    #  * directory operations: v.FileId is NULL; renames and deletions also show up as subdir and contained files name/deletions
    #  * deletetions: v.DeletionId != 0
    #
    #  * commands: 
    #       2=edit
    #       5=add (directory)
    #       7=add (file)
    #       16=delete
    #       64=[branch] (created a label)
    #       128=[merge]
    #       130=[merge, edit]
    #       144=[merge, delete]
    #       192=[merge, branch] ??
    #       196=[merge, branch] ??
    #       514=[rollback, edit]
    #       1040=delete of [rename, edit], delete of [rename]
    #       1168=[merge, rename]
    #       2112=new entry of [rename]
    #       2114=new entry of [rename, edit]
    #       2240=[merge, rename] of a directory
    #

    ###################################################
    #   Bitmask     Command
    ###################################################
    #   1           add
    #   2           edit (= file)
    #   4           encoding
    #   8           --- (rename, no longer used?)
    #   16          delete
    #   32          undeleted
    #   64          branch
    #   128         merge
    #   256         ---
    #   512         rollback
    #   1024        source rename
    #   2048        rename

    @staticmethod
    def filerowsRelpathsByBranch(id, conn, hooks):
        """Selects all file rows of a single TFS commit and splits them up into
        all configures GIT branches."""

        filerows = select(conn, """
            select *
            from tbl_Version v
            inner join tbl_File f on f.FileId = v.FileId
            where v.VersionFrom=? and v.FileId is not NULL""", 
            [id])

        return split_and_filter_file_rows(filerows, hooks)

    @staticmethod
    def mergeRowsByTargetBranch(id, conn, hooks):
        """Selects all merge rows of a single TFS commit and splits them up into
        all configured GIT (target) branches."""

        # see: http://netexp.blogspot.ch/2012/11/tfs-who-is-father-of-my-branch.html
        # see: https://social.msdn.microsoft.com/Forums/vstudio/en-US/a010da85-39f7-4810-99fc-c33db4800c8f/tfs-11-starting-point-of-a-branch?forum=tfsversioncontrol

        mergerows = select(conn, """
            select mh.*, tv.FullPath as TargetFullPath, sv.FullPath as SourceFullPath

            from tbl_MergeHistory mh
            inner join tbl_version tv 
                on mh.TargetItemId = tv.ItemId 
                and mh.TargetVersionFrom = tv.VersionFrom 
                and tv.ItemType=2 -- files only, no directories
            inner join tbl_version sv 
                on mh.SourceItemId = sv.ItemId 
                and mh.SourceVersionFrom between sv.VersionFrom and sv.VersionTo
                and mh.SourceVersionFrom < mh.TargetVersionFrom -- a sign of history loss/rewrite?
            where 
                ForwardMerge = 1 and RenameHistory != 1 -- merges, but no renames (which also show up in this table)
                and mh.TargetVersionFrom = ?""",
            [id])

        return split_and_filter_file_rows(mergerows, hooks, return_rel_paths = False, full_path_hook = lambda row: row.TargetFullPath)


    def __init__(self, conn, tempdir, hooks, id, owner, creationDate, comment, committer, branch, filerowsRelPaths, mergerows):
        self.conn = conn
        self.tempdir = tempdir
        self.hooks = hooks

        self.id = id
        self.owner = owner
        self.creationDate = creationDate
        self.comment = comment
        self.committer = committer
        self.branch = branch
        self.rowsRelPaths = filerowsRelPaths
        self.mergerows = mergerows

    @functools.lru_cache()
    def first_content_row_by_file_id(self):
        """Returns a dictionary with the first tbl_Content row per file id and an additional column 'HasMoreBlocks'."""

        # tbl_content:
        #  * file content is junked to 1MB blocks appearing in multiple rows.

        rows = select(self.conn, 
            """select 
                c.*, 
                (case when exists(select null from tbl_Content c1 where c1.FileId = c.FileId and c1.OffsetFrom <> 0) then 1 else 0 end) as HasMoreBlocks
            from tbl_Content c 
            inner join tbl_Version v on v.FileId = c.FileId 
            where v.VersionFrom = ? and c.OffsetFrom = 0""", 
            [self.id])

        return {r.FileId:r for r in rows}

    def _content_blocks_for_file(self, file_id):
        """Returns an iterator over all content blocks of a given file."""

        row = self.first_content_row_by_file_id()[file_id]

        if not row.HasMoreBlocks:
            return [row.Content]
        else:
            return (r.Content for r in select(self.conn, "select Content from tbl_Content where FileId = ? order by OffsetFrom", [file_id]))

    def changes(self):
        """Returns an iterator over :FileContentChange: instances."""

        for row, relpath in self.rowsRelPaths:
            if row.DeletionId or not row.FileId:
                continue

            # TODO: perhaps it would be more efficient to delivier Blob commands during unpacking 
            # deltas (see https://www.kernel.org/pub/software/scm/git/docs/git-fast-import.html, Packfile Optimization):
            # 
            # Frontends which have efficient access to all revisions of a single file (for example reading an RCS/CVS ,v file) 
            # can choose to supply all revisions of that file as a sequence of consecutive blob commands. This allows fast-import 
            # to deltify the different file revisions against each other, saving space in the final packfile.

            yield FileContentChange(self.conn, self.tempdir, row.FileId, relpath, row.FileLength, row.CompressedLength, row.CompressionType, row.ContentType, row.HashValue, lambda row=row: self._content_blocks_for_file(row.FileId))

    def deletes(self):
        """Returns an interator over :FileOperation: instances."""

        for row, relpath in self.rowsRelPaths:
            if not row.DeletionId or not row.FileId:
                continue
            yield FileOperation(self.conn, row.FileId, relpath)

    def merges(self):
        """Returns an iterator over (branch-name, changeset-id) tuples. The changeset-id can be :None: if it 
        cannot any more be reconstructed (but we know the branch)."""

        mergerowsBySourceBranch = split_and_filter_file_rows(self.mergerows, self.hooks, return_rel_paths = False, full_path_hook = lambda row: row.SourceFullPath)

        for b, rows in mergerowsBySourceBranch.items():
            sourceVersions = [r.SourceVersionTo for r in rows if r.SourceVersionTo < self.id]
            yield b, max(sourceVersions) if sourceVersions else None

class Label(object):
    """Represents a label."""

    def __init__(self, changesetId, branch, name, comment, user, creationDate):
        self.changesetId = changesetId
        self.branch = branch
        self.name = name
        self.comment = comment
        self.user = user
        self.creationDate = creationDate

# Exporter
# --------

class ExporterHooks(object):
    """Holds customization hooks for the the Exporter class. 
    
    For the description of the individual function signatures, please look at cfg-empty.py.
    
    The warning hook is invoked with a string argument to indicate an unexpected
    conversion situation."""
    
    def __init__(self, branch_extract, file_filter, content_rewrite, user_lookup, warning):
        self.branch_extract = branch_extract
        self.file_filter = file_filter
        self.content_rewrite = content_rewrite
        self.user_lookup = user_lookup
        self.warning = warning

BranchesInfo = collections.namedtuple("BranchesInfo", ["names", "unassigned", "assigned_by_branch", "ignored_by_branch", "oversized_by_branch"])

class Repository10(object):
    """Implements access to a TFS 2010 database."""

    def __init__(self, conninfo, hooks, temp_dir = None):
        self.hooks = hooks
        self.tempdir = None
        self.conn = None

        try:
            self.conn = adodbapi.connect(conninfo)
            self.tempdir = tempdir.TempDir(temp_dir or "fe_tmp_swit", clear_location_if_existing = True) # "_swit" extension to exclude it from our McAfee
        except:
            self.cleanup()
            raise

    def cleanup(self):
        """Explicitly cleans up any temporarily acquired resources."""

        if self.conn:
            self.conn.close()
        if self.tempdir:
            self.tempdir.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.cleanup()

    @functools.lru_cache(maxsize = None)
    def get_user(self, id):
        """Returns a single user"""

        row = selectone(self.conn, """
            select * 
            from Constants c 
            inner join tbl_Identity i on c.TeamFoundationId=i.TeamFoundationId 
            where i.IdentityId=?""", 
            [id])

        return User(id, row.DomainPart, row.NamePart, row.DisplayPart)

    def active_users(self):
        """Returns all acitve users in the form of User objects."""
        
        rows = select(self.conn, """
            select OwnerId as UserId from tbl_Changeset
            union
            select CommitterId as UserId from tbl_Changeset
            union
            select OwnerId as UserId from tbl_Label""")

        return [self.get_user(i) for i in sorted({r.UserId for r in rows})]

    @functools.lru_cache(maxsize = None)
    def get_user_displayname_email_timezone(self, user):

        return self.hooks.user_lookup(user)

    def get_branches_info(self):
        """Evaluates the configured branch extraction callback against the list of stored files.
        
        returns a BranchesInfo object"""

        rows = select(self.conn, """
            select distinct v.FullPath, f.FileLength
            from tbl_Version v
            inner join tbl_File f on v.FileId = f.FileId""")

        # branch names, files outside of a branch (branch_extract hook)
        rowsWithBranchAndLocalPath = ((r, self.hooks.branch_extract(tfs_unmangle_path(r.FullPath))) for r in rows)
        rowsWithLocalPathByBranch = build_keyed_dict(rowsWithBranchAndLocalPath, lambda i: i[1][0], lambda i: (i[0], i[1][1]))

        unassigned = sorted({tfs_unmangle_path(i[0].FullPath) for i in rowsWithLocalPathByBranch[None]})
        rowsWithLocalPathByBranch.pop(None)

        names = sorted(rowsWithLocalPathByBranch.keys())

        # ignored files within a branch (file_filter hook)
        assigned_by_branch = {}
        ignored_by_branch = {}

        for branch in rowsWithLocalPathByBranch:
            rowsWithLocalPath = rowsWithLocalPathByBranch[branch]

            tmp = build_keyed_dict(rowsWithLocalPath, lambda i: (not i[1]) or self.hooks.file_filter(branch, i[1]))

            rowsWithLocalPathByBranch[branch] = tmp[True]

            assigned_by_branch[branch] = sorted({i[1] for i in tmp[True]})
            ignored_by_branch[branch] = sorted({i[1] for i in tmp[False]})

        # oversized files
        oversized_by_branch = {b:sorted({i[1] for i in items if i[0].FileLength > oversize_warning_limit}) for b, items in rowsWithLocalPathByBranch.items()}
       
        # done
        return BranchesInfo(names, unassigned, assigned_by_branch, ignored_by_branch, oversized_by_branch)

    def changesets(self):
        """Iterates over all existing changesets in the form of Changeset objects."""

        # TODO: "MayHaveMerges" could be more precise, but this would mean duplication of logic (maintenance, correctness)
        # and also layering conflicts
        csrows = select(self.conn, """
            select 
                cs.*, 
                case when exists(select null from tbl_MergeHistory mh where mh.TargetVersionFrom = cs.ChangeSetId) then 1 else 0 end as MayHaveMerges
            from tbl_ChangeSet cs
            where cs.Comment != ?
            order by cs.ChangeSetId""", 
            ['All of the changes in this changeset have been destroyed.'])

        for csrow in csrows:
            filerowRelpathsByBranch = Changeset.filerowsRelpathsByBranch(csrow.ChangeSetId, self.conn, self.hooks)
            mergerowsByTargetBranch = Changeset.mergeRowsByTargetBranch(csrow.ChangeSetId, self.conn, self.hooks) if csrow.MayHaveMerges else {}

            for branch, filerowsRelpaths in filerowRelpathsByBranch.items():
                yield Changeset(self.conn, self.tempdir, self.hooks, 
                                csrow.ChangeSetId, 
                                self.get_user(csrow.OwnerId), 
                                tfs_unmangle_timestamp(csrow.CreationDate),
                                csrow.Comment, 
                                self.get_user(csrow.CommitterId), 
                                branch, 
                                filerowsRelpaths, 
                                mergerowsByTargetBranch.get(branch, []))

    def labels(self):
        """Iterates over all existing labels. Returns Label objects. 
        
        The returning ist is sorted by branch, then by changeset number."""

        labelRows = {r.Labelid:r for r in select(self.conn, "select * from tbl_label")}

        entryRows = select(self.conn, """
            select le.*, v.FullPath
            from tbl_LabelEntry le
            inner join tbl_Version v on v.ItemId = le.ItemId and le.VersionFrom between v.VersionFrom and v.VersionTo
            order by le.LabelId""")

        # split branches and filter files
        entryRowsRelpathsByBranch = split_and_filter_file_rows(entryRows, self.hooks)

        branchesByLabelId = collections.defaultdict(set)
        for branch, entryRowsRelpaths in entryRowsRelpathsByBranch.items():
           for labelId in {i[0].LabelId for i in entryRowsRelpaths}:
               branchesByLabelId[labelId].add(branch)

        # enumerate individually per branch
        for branch, entryRowsRelpaths in entryRowsRelpathsByBranch.items():
            for labelId, entryRows in itertools.groupby((r[0] for r in entryRowsRelpaths), lambda r: r.LabelId):

                labelRow = labelRows[labelId]

                # changeset range?
                versionsFrom = {r.VersionFrom for r in entryRows}

                if len(versionsFrom) > 1:
                    # TODO: we could invest a bit more work here
                    self.hooks.warning("ignoring label '{}' on branch '{}' because it is assigned to more than a single changeset ({}). tag fixups are not (yet?) supported. ".format(labelRow.LabelName, branch, len(versionsFrom)))
                    continue

                # unify name
                name = labelRow.LabelName

                if len(branchesByLabelId[labelId]) > 1:
                    name += " [{}]".format(branch)

                yield Label(versionsFrom.pop(), branch, name, labelRow.Comment, self.get_user(labelRow.OwnerId), tfs_unmangle_timestamp(labelRow.LastModified))

def create_repo(conninfo, hooks, temp_dir = None):
    """Creates the correct repository accessir based on certain schema properties.

    This is the main entry point for accessing the DB. Use the result as a 
    context manager.
    """

    with adodbapi.connect(conninfo) as conn:
        has_tbl_identity = bool(select(conn, "select * from sys.tables where name='tbl_Identity'"))

    if has_tbl_identity:
        return Repository10(conninfo, hooks, temp_dir = temp_dir)
    else:
        raise NotImplementedError("accessing TFS 2013 has not yet been implemented.")


# Fast Export Command Generation
# ------------------------------

def git_mangle_path(path):
    """Git generally expects forward slashes."""

    return path.replace("\\", "/")

def git_mangle_tagname(name):
    """Git has much stricter restrictions on tag names."""

    # see: http://git-scm.com/docs/git-check-ref-format
    return git_mangle_path(name).replace("\n", "").replace("\r", "").replace("[", "(").replace("]", ")").replace(" ", "_")

def fastexport_commands(repo, stop_after = 0, skip_tags = False, no_content = False):
    """Produces a stream of fastexport commands that can then be serialized."""

    # Mark generation
    marks = {}
    marks_last_changesetId = None
    marks_last_issued = None
    marks_last_issued_per_branch = {}

    def generate_mark(branch, changesetId):
        """Looks up an issued mark for a specific changeset on a given branch, or generates a new one."""

        nonlocal marks, marks_last_changesetId, marks_last_issued, marks_last_issued_per_branch

        if changesetId != marks_last_changesetId:
            marks_last_changesetId = changesetId
            marks_last_issued = changesetId * 100
        else:
            marks_last_issued += 1

        mark = marks_last_issued
        marks[(changesetId, branch)] = mark
        marks_last_issued_per_branch[branch] = mark
        
        return mark

    # Autor / timestamp formatting
    def who_when(user, date):
        "returns a tuple (name,email,secs-since-epoch,utc-offset-secs)"

        dn, email, tz = repo.get_user_displayname_email_timezone(user)

        return (dn, email, date.astimezone(datetime.timezone.utc).timestamp(), int(date.astimezone(tz).utcoffset().total_seconds()))

    for cs in repo.changesets():
        if stop_after and cs.id > stop_after:
            break # commits are sorted

        yield fastimport.ProgressCommand("changeset {}/{} from {}".format(cs.id, cs.branch, cs.creationDate.astimezone(None)))

        merge_marks = []
        for branch, version in cs.merges():
            if version:
                merge_marks.append(marks[version, branch])
            else:
                merge_marks.append(marks_last_issued_per_branch[branch])

        yield fastimport.CommitCommand(
            git_mangle_path(cs.branch), 
            mark = generate_mark(cs.branch, cs.id),
            author = who_when(cs.owner, cs.creationDate) if cs.owner != cs.committer else None, 
            committer = who_when(cs.committer, cs.creationDate),
            message = cs.comment,
            merges = [fastimport.format_mark(m) for m in merge_marks])

        for f in cs.deletes():
            yield fastimport.FileDeleteCommand(git_mangle_path(f.fullpath))

        for f in cs.changes():
            if no_content:
                length, content = 0, [b'']
            else:
                length, content = f.file_length, f.content()

                if repo.hooks.content_rewrite:
                    length, content = repo.hooks.content_rewrite(cs.branch, f.fullpath, length, content)

                if length >= oversize_warning_limit:
                    repo.hooks.warning("very large file ({} bytes) in changeset {}/{}: {}".format(length, cs.id, cs.branch, f.fullpath))

            yield fastimport.FileModifyCommand(git_mangle_path(f.fullpath), 0o644, None, fastimport.BlobFragmentIterator(length, content)) # TODO: check mode param

    if not skip_tags:
        for lbl in repo.labels():
            if stop_after and lbl.changesetId > stop_after:
                continue # tags are not sorted

            mark = marks.get((lbl.changesetId, lbl.branch))
            if not mark:
                repo.hooks.warning("skipping label '{}' pointing to changeset {}/{} because that changeset is not migrated".format(lbl.name, lbl.changesetId, lbl.branch))
                continue

            yield fastimport.TagCommand(git_mangle_tagname(lbl.name), fastimport.format_mark(mark), who_when(lbl.user, lbl.creationDate), lbl.comment)
