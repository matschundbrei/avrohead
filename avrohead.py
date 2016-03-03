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
    print "The -f option has to be present, if you do not give me a number(-n)"
    print "I will take 5!"
    print "You can add the -s option to only receive the schema in JSON"
    print "If you want the JSON to be printed pretty, add the -i switch"
    print "You can add a '-d /path/to/newfile.avro' to redirect the output to a new avro file"


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
    schema = False
    pretty = False
    num = 5
    try:
        opts, args = getopt.getopt(argv, 'f:n:d:si')
    except getopt.GetoptError as err:
        show_help()
        sys.exit(2)
    for o, a in opts:
        # print "{0} ===> {1}".format(o, a) # DEBUG vars
        if o == "-f":
            avro = a
        elif o == "-d":
            dest = a
        elif o == "-n":
            num = a
        elif o == "-s":
            schema = True
        elif o == "-i":
            pretty = True
        else:
            print "I am going to ignore option {0} you've set to {1}".format(o, a)

    if not avro:
        print "We at least need a file, ok?"
        show_help()
        sys.exit(2)

    if not dest and schema:
        schema = get_schema(avro)
        print schema.to_json()
    elif not dest and not schema:
        print json.dumps(head_avro(avro, num), indent=pretty)
    else:
        write_avro(dest, num, avro)


if __name__ == "__main__":
    main(sys.argv[1:])
