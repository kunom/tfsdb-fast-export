"Holds common configuration content for all VisualStudio projects."""

import re

# https://social.msdn.microsoft.com/Forums/vstudio/en-US/9920911d-1a7e-4ada-90cd-b1b910586cf4/why-do-you-need-the-vspscc-and-vssscc-files?forum=tfsgeneral
_vs_ignores_re = re.compile(r"\.vs[sp]scc\Z")

def vs_file_filter(branch, relpath):
    """Returns False if the given relative path is a Visual Studio Source Code Control file."""

    return not _vs_ignores_re.search(relpath)

def vs_content_rewrite(branch, relpath, length, blocks):
    """if the given file is a Visual Studio solution file, removes the source control provider section from that file."""

    if relpath.lower().endswith(".sln"):
        data = b"".join(blocks)
        data = re.sub(br'\s+GlobalSection\(TeamFoundationVersionControl\).*?EndGlobalSection', b'', data, flags = re.DOTALL)
        return len(data), [data]

    return length, blocks
