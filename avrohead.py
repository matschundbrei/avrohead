#!/usr/bin/env python
#Author: @matschundbrei
#Copyright 2016
#licensed under Beerware License
#more info: https://en.wikipedia.org/wiki/Beerware

import os
import sys
import getopt
import json
import avro.schema
from itertools import islice
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


def show_help():
    "well, this'll show the help, alright!"
    print "Use this like ..."
    print "python {0} -n 4711 -f /path/to/somefile.avro".format(os.path.basename(__file__))
    print "you can add a '-d /path/to/newfile.avro' to redirect the output to a new avro file"


def get_schema(f):
    "will grab and return the schema from file f"
    with DataFileReader(open(f, "r"), DatumReader()) as avrofeed:
        return avrofeed.datum_reader.writers_schema


def head_avro(f, n):
    "will return n lines from avrofile f"
    with DataFileReader(open(f, "r"), DatumReader()) as avrofeed:
        return list(islice(avrofeed, n))


def write_avro(f, n, s):
    "will create an avro file f with n lines from file s"
    schema = get_schema(s)
    headers = head_avro(s, n)
    with DataFileWriter(open(f, "wb"), DatumWriter(), schema) as writer:
        for line in headers:
            writer.append(line)


def main(argv):
    "main foo happening here, alright!"
    avro = False
    dest = False
    num = 5
    try:
        opts, args = getopt.getopt(argv, 'f:n:d:')
    except getopt.GetoptError as err:
        show_help()
        sys.exit(2)
    for o, a in opts:
        if o == "-f":
            avro = a
        elif o == "-d":
            dest = a
        elif o == "-n":
            num = a
        else:
            print "I am not sure what to do with option {0} you set to {1}".format(o, a)

    if not avro:
        print "We at least need a file, ok?"
        show_help()
        sys.exit(2)

    if not dest:
        print json.dumps(head_avro(avro, num), indent=True)
    else:
        write_avro(dest, num, avro)


if __name__ == "__main__":
    main(sys.argv[1:])
