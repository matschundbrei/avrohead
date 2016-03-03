avrohead
========

This script aims to get the same functionality as the Unix command `head` for [Avro](https://avro.apache.org/)-files.

Dependencies
============

You will need to install the latest python bindings for your python version from [https://avro.apache.org](https://avro.apache.org/)

Usage
=====

You call this script with an optional number (`-n`) and a filename (`-f`) and you will get back the number you've put in of Avro objects as formatted JSON from that given Avro-file.

You can also redirect the output to another Avro file with the `-d` switch.

For convenience I've added a switch to pretty-print JSON (`-i`) and one to only extract the schema from the file (`-s`)

Example:

`python avrohead.py -n 42 -f /path/to/my/file.avro`

License
=======

This software is released under the Beerware license. Almost too detailed License information on [Wikipedia](https://en.wikipedia.org/wiki/Beerware)
