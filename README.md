TFS DB Fast Export
------------------

This tool generates a Git fast-export stream directly from
a Team Foundation Server database.

The advantage of this approach over other tools using the *Microsoft.Teamfoundation.Client* .NET
library (like http://git-tfs.com/, https://gittf.codeplex.com/ and
https://github.com/viceroypenguin/tfs-fast-export) is performance, which was
in our case a show-stopper for using those tools. In addition, you have the possiblity to reconfigure
the branch layout, can do additional file filtering and do content
rewrites (of historical data) right in place.

The drawback of this approach is that the code is purely based on reverse
engineering of internal data structures, not on an interface specification.
The conversion might be buggy and in certain cases it is lossy. If we deliberately
ignore information that somehow *could* be converted, a warning will be
printed. Please test, check and make up your own opinion.

This code currently works for *TFS 2010* databases. Support for *TFS 2013* databases is conceivable,
but not implemented; other versions have not been investigated. I am open for contributions,
 but the current code will need some refactoring to be easily extendible.


Prerequisites
-------------

- *Python 3.5*: earlier versions are not ok, not even *Python 3.4*.

- *pywin32* (`py -m pip install pypiwin32`)

- *adodbapi*: Version 219 of *pywin32* contains a broken *adodbapi* distribution.
So, when you encounter a related runtime error, you may need to remove that manually from the 
`site-packages` folder and install it separately (`py -m pip install adodbapi`).

- optional: *Python Tools for Visual Studio* (http://microsoft.github.io/PTVS/)


Typical Conversion Workflow
---------------------------

1.  Find out about all available commands

    `main.py -h`

2.  Customize your conversion setup (which should come as a Python module
referred to as `<config>` below). Start with the `cfg-empty.py` template.

3.  Test the branch and file filter hooks. (Be sure that you don't miss
the section headings in the typically large output.)

    `main.py <config> branches-info`
    
    `main.py <config> branches-info > out.txt`

4.  Test the user name rewrite / e-mail / timezone lookup hook.

    `main.py <config> users`

5.  Look at the changesets. (The `commits` command merely lists the 
TFS changesets, merges and file operations. The `fast-export --dry-run` command 
also processes the file contents and does the Git marks handling and timezone handling
like for the real export.

    `main.py <config> commits`
    
    `main.py <config> commits --no-files`
    
    `main.py <config> fast-export --dry-run`

6.  Run the conversion.

    `git init d:\new-repo --bare`
    
    `set git_dir=d:\new-repo`

    `main.py <config> fast-export --export-warnings=warnings.txt | git fast-import --export-marks=marks.txt`

7.  Finalize the repository.

    - Add `.gitignore` to all branches. 

    See https://git-scm.com/docs/gitignore. Usually you want to ignore `.vs`. `*.suo`, `*.user`, 
    `bin`, `obj`, `packages`, etc. Maybe, there is already a `.tfsignore` file.

    - Add `.gitattributes` to all branches.

    See https://git-scm.com/docs/gitattributes. Usually you want to set `* -text` for having no line ending conversion at all (the safe bet),
    or `* text=auto` for having automatic line ending conversion for everything that is
    considered by Git as a text file (probably better for cross platform development,
    but be careful with text-like binary formats like .pdf and maybe .csv). Not having this file
    is not recommended as the *libgit2sharp.dll* of the *Visual Studio Git Provider* 
    and the *git 2.6* command line tool seem to have different defaults.

    - `D:\Projekte\test>git repack -a -d (-f)`

    See https://git-scm.com/docs/git-fast-import, section *Packfile Optimization*.


Additional Notes
----------------

- You can run the script within the PTVS debugger in Visual Studio, but running
outside of a debugger is much faster.

- The temp file directory (command line option `--temp-dir` of the `fast-export` command) should not be
monitored by an anti-virus scanner, for clearly noticeable performance reasons. (Rewriting `tempdir.py` to
use Sqlite could perhaps help, too.)

- Since the merge models differ drastically between TFS and Git (cherry picking vs. feature branches),
the merge information is only migrated on a best-effort basis.

- Performance: Producing ~17000 Git revisons out of 40000 TFS changesets, resulting in a optimized Git pack
file size of 367 MB takes approx. 1 hour (time for repacking not counted, TFS DB locally hosted, temp dir
excluded from anti-virus scanning).

- Unit tests are run by `py -m unittest discover . *_test.py`. Test coverage is not extensive, though.
