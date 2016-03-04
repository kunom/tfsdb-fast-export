import argparse
import fastimport
import importlib
import io
import sys
import tfsdb

# Helper Methods
# --------------

class WarningsCollector(object):
    
    def __init__(self, to_stderr = True, to_file = None):
        self.to_stderr = to_stderr
        self.to_file = to_file
        self.lines = []

    def add(self, line):
        if self.to_file:
            self.lines.append(line)
        if self.to_stderr:
            print(line, file = sys.stderr)

    def close(self):
        if self.to_file:
            with io.open(self.to_file, "wt", encoding = "utf-8-sig") as f:
                for l in self.lines:
                    f.write(l)
                    f.write('\n')

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

def register_unicode_fallback_on_stdout():
    """Changes unicode mode as we do not want to crash on unprintable file names."""

    inner = sys.stdout
    sys.stdout = io.TextIOWrapper(inner.buffer, inner.encoding, 'replace', line_buffering = inner.line_buffering)

# Commands
# --------

def cmd_list_branches_info(repo):
    register_unicode_fallback_on_stdout()

    info = repo.get_branches_info()

    print("found branches:")
    for b in info.names:
        print("   {}".format(b))

    print("assigned files:")
    for b in info.names:
        if b in info.assigned_by_branch:
            for name in info.assigned_by_branch[b]:
                print("   {} - {}".format(b, name))
        else:
            print("   {} - <no files !!>".format(b))

    print("ignored files:")
    for b in info.ignored_by_branch:
        for name in info.ignored_by_branch[b]:
            print("   {} - {}".format(b, name))

    print("oversized files:")
    for b in info.oversized_by_branch:
        for name in info.oversized_by_branch[b]:
            print("   {} - {}".format(b, name))

    print("unassigned paths:")
    for p in info.unassigned:
        print("   {}".format(p))

def cmd_list_commits(repo, no_files):
    register_unicode_fallback_on_stdout()

    for cs in repo.changesets():
        print("{} / {} (TFS) / {} / {} / {}: {}".format(cs.id, cs.creationDate, cs.owner, cs.committer, cs.branch, cs.comment))

        for branch, version in cs.merges():
            print("   merged from {} / {}".format(branch, version))

        if no_files:
            continue

        for f in cs.changes():
            print("   change {}: {}".format(f.fullpath, f.file_length))
        for f in cs.deletes():
            print("   del {}".format(f.fullpath))

def cmd_list_labels(repo):
    register_unicode_fallback_on_stdout()

    for lbl in repo.labels():
        print("{} / {} (TFS) / {}: {}".format(lbl.changesetId, lbl.creationDate, lbl.user, lbl.name))

def cmd_list_users(repo, show_ids = False):
    register_unicode_fallback_on_stdout()

    for u in repo.active_users():
        dn, email, tz = repo.get_user_displayname_email_timezone(u)

        line = "{} / {} / tz={}".format(dn, email, tz or '<undef>')
        if show_ids:
            line += " / {}".format(u.tfs_id)

        print(line)

def cmd_fastexport(repo, dry_run = False, stop_after = 0, no_tags = False, no_content = False):

    class NullStream(object):
        def write(self, data):
            pass

    stream = sys.stdout.buffer if not dry_run else NullStream()

    for cmd in tfsdb.fastexport_commands(repo, stop_after, no_tags, no_content):
        cmd.serialize(stream)

        # print feedback
        if dry_run and isinstance(cmd, fastimport.ProgressCommand):
            print(cmd.message)

# Main
# ----

# https://www.kernel.org/pub/software/scm/git/docs/git-fast-import.html

if __name__ == '__main__':
    # command line interface
    p = argparse.ArgumentParser()
    p.add_argument(dest="config", metavar="CONFIG", help="the config file to read the settings from")

    sp = p.add_subparsers()

    p1 = sp.add_parser("branches-info")
    p1.set_defaults(handler = lambda repo, args: cmd_list_branches_info(repo))

    p1 = sp.add_parser("commits")
    p1.add_argument("--no-files", action="store_true", help="does not list individual file changes")
    p1.set_defaults(handler = lambda repo, args: cmd_list_commits(repo, args.no_files))

    p1 = sp.add_parser("labels")
    p1.set_defaults(handler = lambda repo, args: cmd_list_labels(repo))

    p1 = sp.add_parser("users")
    p1.add_argument("--ids", action="store_true", help="also prints the TFS internal user ID")
    p1.set_defaults(handler = lambda repo, args: cmd_list_users(repo, args.ids))

    p1 = sp.add_parser("fast-export")
    p1.add_argument("--temp-dir", type=str, help="the location where to store temporary files. Having this location excluded from from AntiVirus protection is a big plus for performance.")
    p1.add_argument("--dry-run", action="store_true", help="do not send anything to stdout but print progress messages to the screen (useful for debugging)")
    p1.add_argument("--stop-after", type=int, help="stop export after changeset N", metavar="N")
    p1.add_argument("--no-tags", action="store_true", help="do not export any tags")
    p1.add_argument("--no-content", action="store_true", help="does not export file content but only writes empty files")
    p1.add_argument("--export-warnings", dest="warnings", type=str, help="dumps all warnings during fast export into a file")
    p1.set_defaults(handler = lambda repo, args: cmd_fastexport(repo, args.dry_run, args.stop_after, args.no_tags, args.no_content))

    input = sys.argv[1:]
    if len(input) < 2:
        input.append("-h")

    args = p.parse_args(input)

    temp_dir = getattr(args, "temp_dir", None)
    warning_file = getattr(args, "warnings", None)

    # load project configuration
    try:
        config_file = args.config
        if config_file.endswith(".py"):
            config_file = config_file[:-3]

        config = importlib.import_module(config_file)
    except:
        raise Exception("error while loading the project configuration '{}'".format(args.config))

    # main
    with WarningsCollector(to_file = warning_file) as warnings:
        hooks = tfsdb.ExporterHooks(config.branch_extract, config.file_filter, config.content_rewrite, config.user_lookup, warnings.add)

        with tfsdb.create_repo(config.conninfo, hooks, temp_dir) as repo:
            args.handler(repo, args)
