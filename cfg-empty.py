import re
import vs

# This is the connection to the DB server. More about connection strings: 
# https://msdn.microsoft.com/en-us/library/ms130822.aspx
#
# The SQLOLEDB provider will use in some cases a TCP connection even to a 
# local SQL Server (depending on the machine name used in the connection string). You may have 
# to open up that interface in your SQL Server configuration.
conninfo = "Provider=SQLOLEDB;data source=.;initial catalog=Tfs_Foo;Integrated Security=SSPI"


# This is the hook that converts a TFS path name into a (Branch, Relative-Path) pair.
#
# This can also be called for a branch path itself, so you have to be prepared to not find a relative
# path. 
#
# Returning "(None, None)" means that the file will be ignored. This can be used to filter out 
# unwanted files, but alternatively you can configure the :file_filter: hook (see below).
def branch_extract(name):

    # a straight forward implementation (everything is included in the master branch)
    return "master", name[2:]

    # a more sophisticated solution, taking into account different TFS branches
    # (be sure to move the re.compile statement to module scope.)
    branch_re = re.compile(r"\A\$\\PCSA2\\(?P<branch>Development|Hotfixes|Main|Release)(\\(?P<relpath>.*))?\Z")

    m = branch_re.match(name)
    if not m:
        return (None, None)

    return m.group("branch"), m.group("relpath")


# This is the hook that is immediately called after the branch_hook. This can be used 
# in alternative to filter out files.
#
# Shall return False if the file is to be ignored.
def file_filter(branch, relpath):

    # a straight forward implementation (everything is included)
    return True

    # a more sophisticated solution
    # (be sure to move the re.compile statement to module scope.)
    ignores_re = re.compile(r"\.(bacpac|cspkg|bak)\Z")

    return vs.vs_file_filter(branch, relpath) and not ignores_re.search(relpath)


# This is the hook that is called to rewrite (historical) file content. 
#
# Branch and file namesare delivered as received by the :branch_hook: function, 
# :length: is the total length of the file and :blocks: is an iterable collection 
# of bytes objects.
def content_rewrite(branch, relpath, length, blocks):

    # a straight forwared implementation (do not modify anything)
    return length, blocks

    # a more sophisticated solution
    length, blocks = vs.vs_content_rewrite(branch, relpath, length, blocks)

    return length, blocks


# This is the hook that is called to translate TFS user data into GIT user data.
# The function receives a tfsdb.User instance an should return a (display name, e-mail, user time zone) tuple.
#
# Returning :None: as time zone means that the current machine's timezone is used.
# Otherwise you may want to refer to the :pytz: library.
#
# The result of this function will be cached. So it will be only called once per
# invocation and TFS user and you can do potentially expensive operations (but be aware
# that you may receive user information that is no more present in the Active Directory).
def user_lookup(user):

    # a straight forward implementation
    return (user.display_name, "nobody@example.org", None)

    # a more sophisticated solution
    tmp = {'MEK': ('Kuno Meyer', 'kuno.meyer@gmx.ch', None)}
    return tmp[user.login]