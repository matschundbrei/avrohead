AVROHEAD
========

This script aims to get the same functionality as the Unix command `head` for [Avro](https://avro.apache.org/)-files.

DEPENCIES
=========

You will need to install the latest python bindings for your python version from [https://avro.apache.org](https://avro.apache.org/)

USAGE
=====

you call this script with an optional number (-n) and a filename (-f) and you will get back the number you've put in "lines" (really: Avro datums as formatted JSON) from that Avro-file.

You can also redirect the output to another Avro file with the -d switch.

LICENSE
=======

This software is released under the Beerware license. Almost too detailed License information on [Wikipedia] (https://en.wikipedia.org/wiki/Beerware)
