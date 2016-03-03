avrohead
========

This script aims to enable users to extract bits of information from [Avro](https://avro.apache.org/)-files.
The first iteration only contained the functionality of `head`, we now aim to integrate something like grep and have the possibility to extract the schema and write a result-avro file.

Dependencies
============

You will need to install the latest python bindings for your python version from [https://avro.apache.org](https://avro.apache.org/)

Usage
=====

You call this script with an optional number (`-n`) and a filename (`-f`) and you will get back the number you've put in of Avro objects as formatted JSON from that given Avro-file.

You can also redirect the output to another Avro file with the `-d` switch.

You can search/grep/filter for fields and their value by adding `-g "field:value"`. *Note:* If you also set the number of lines, the program will exit only if it found that amount of lines with your search/grep/filter, if the combination is not found, it will search the whole file.

For convenience I've added a switch to pretty-print JSON (`-i`) and one to only extract the schema from the file (`-s`)

Example:

`python avrohead.py -n 42 -f /path/to/my/file.avro`

License
=======

This software is released under the Beerware license. Almost too detailed License information on [Wikipedia](https://en.wikipedia.org/wiki/Beerware)
