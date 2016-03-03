#!/usr/bin/env python
#Author: @matschundbrei
#Copyright 2016
#licensed under Beerware License
#more info: https://en.wikipedia.org/wiki/Beerware

import os
import sys
import json
import avro.schema
from itertools import islice
from optparse import OptionParser
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


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


def search_avro(f, n, t):
    "will search n occurances of searchterm t in file f"
    (field, value) = t.split(":", 1)
    with DataFileReader(open(f, "r"), DatumReader()) as avrofeed:
        schema = avrofeed.datum_reader.writers_schema
        if field in schema.fields_dict:
#            TODO: does not yield pure dict!!!
        else:
            raise NameError("Cannot find {0} in schema for {1}".format(field, f))


def main():
    "main foo happening here, alright!"
    usage = "%prog [Options]"
    parser = OptionParser(usage=usage, version="%prog v0.1")
    parser.add_option("-f", "--file", dest="avro", help="an Avro file to read from", metavar="some/file.avro")
    parser.add_option("-s", "--schema", dest="schema", default=None, action="store_true",
                      help="only extract and return the avro-schema in JSON")
    parser.add_option("-i", "--pretty", dest="pretty", default=None, action="store_true",
                      help="indent [and keysort, if schema] any JSON on the output")
    parser.add_option("-n", "--number", dest="num", type="int", default=5, help="integer number of lines to put out")
    parser.add_option("-d", "--destination", dest="dest", help="optional destination file to write to")
    parser.add_option("-g", "--search", dest="search", help="search for a specific field with a specific value",
                      metavar="\"field:value\"")
    (opts, arg) = parser.parse_args()

    if not opts.avro:
        print "We at least need a file, ok?"
        parser.print_help()
        sys.exit(2)

    if not opts.dest and opts.schema:
        schema = get_schema(opts.avro)
        if not opts.pretty:
            print schema.to_json()
        else:
            print json.dumps(schema.to_json(), sort_keys=True, indent=opts.pretty)  # I know, it's silly
    elif not opts.dest and not opts.schema:
        if not opts.search:
            print json.dumps(head_avro(opts.avro, opts.num), indent=opts.pretty)
        else:
            print json.dumps(search_avro(opts.avro, opts.num, opts.search), indent=opts.pretty)
    else:
        write_avro(opts.dest, opts.num, opts.avro)


if __name__ == "__main__":
    main()
