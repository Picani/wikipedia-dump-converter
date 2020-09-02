wikipedia-dump-converter
========================

A very specific tool, that converts the [SQL dump of Wikipedia][0] into RDF
files suitable to be imported by [dgraph][1].

For now, it only converts the **[page][2]** and the **[pagelinks][3]** tables.
This is because I'm only interested in the links between pages (and especially
between *encyclopedia* pages). I might add support for the **[category][4]** and
**[categorylinks][5]** tables.


Installation
------------

It's written in rust and makes use of cargo. So just type:

    $ git clone https://github.com/Picani/wikipedia-dump-converter.git
    $ cd wikipedia-dump-converter
    $ cargo build --release
    
The executable is now `target/release/wikipedia-dump-converter`. Move it
somewhere on your `PATH`.


Usage
-----

Print the help with:

    $ wikipedia-dump-converter -h


First, convert the `page` table dump:

    $ wikipedia-dump-converter -i pages -e page_table_dump.sql.gz converted_pages.rdf.gz
    
Remove the `-i` argument to stop when encountering a text encoding error in the
dump, instead of printing it and continuing.

Remove the `-e` argument to also convert non-encyclopedia pages (like user
pages, help pages, _etc_).

The resulting file looks like the following:

    $ zcat converted_pages.rdf.gz | head
    <3> <namespace> "0" .
    <3> <title> "Antoine Meillet" .
    <7> <namespace> "0" .
    <7> <title> "Algèbre linéaire" .
    <9> <namespace> "0" .
    <9> <title> "Algèbre générale" .
    <10> <namespace> "0" .
    <10> <title> "Algorithmique" .
    <11> <namespace> "0" .
    <11> <title> "Politique en Argentine" .
    
In [RDF triple][6] terminology, the subject is the page unique ID, the
predicate is either `title` or `namespace` and the object is either the page
title (when the predicate is `title`) or the namespace unique ID (when the
predicate is `namespace`). The list of namespaces is available [here][7].


Then, convert the `pagelinks` table dump:

    $ wikipedia-dump-converter -i links pagelinks_table_dump.sql.gz converted_page.rdf.gz converted_links.rdf.gz
    
Again, remove the `-i` argument to stop when encountering a text encoding error
in the dump, instead of printing it and continuing.

The resulting file looks like the following:

    $ zcat converted_links.rdf.gz | head
    <177374> <linksto> <222657> .
    <315352> <linksto> <222657> .
    <1175072> <linksto> <222657> .
    <3578724> <linksto> <222657> .
    <7917621> <linksto> <222657> .
    <222376> <linksto> <4433171> .
    <4452220> <linksto> <4433171> .
    <7563679> <linksto> <4433171> .
    <7591490> <linksto> <4433171> .
    <90880> <linksto> <351979> .

In [RDF triple][6] terminology, the subject is the page unique ID from which
the link starts, the predicate is `linksto`, and the object is the unique ID
of the page pointed to by the link.

**Note: ** Only the links for which both pages are present in
`converted_pages.rdf.gz` are converted.


Licence
-------

Copyright © 2020 Sylvain PULICANI <picani@laposte.net>

This work is free. You can redistribute it and/or modify it under the
terms of the Do What The Fuck You Want To Public License, Version 2,
as published by Sam Hocevar. See the LICENSE file for more details.


[0]: https://dumps.wikimedia.org/backup-index.html
[1]: https://dgraph.io/docs/deploy/fast-data-loading/
[2]: https://www.mediawiki.org/wiki/Manual:Page_table
[3]: https://www.mediawiki.org/wiki/Manual:Pagelinks_table
[4]: https://www.mediawiki.org/wiki/Manual:Category_table
[5]: https://www.mediawiki.org/wiki/Manual:Categorylinks_table
[6]: https://www.w3.org/TR/n-quads/
[7]: https://www.mediawiki.org/wiki/Manual:Namespace
