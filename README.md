# etlcmd

[![Build Status](https://travis-ci.org/samuelhug/etlcmd.svg?branch=master)](https://travis-ci.org/samuelhug/etlcmd)

A utility to assist with the automation of ETL tasks.


Tasks and associated data-pipelines are defined using a configuration file. Support for easily extensible inputs and outputs. Comes with an embeded Javascript VM for dynamic transformations.

Currently has support for:

| Module    | Mode       |
|-----------|------------|
| Unidata   | Read       |
| MongoDB   | Write      |
| CSV File  | Read/Write |
| JSON File | Read/Write |

Uses modules from the [Ratchet](https://github.com/rhansen2/ratchet) library, as well as a number of custom ones (https://github.com/samuelhug/ratchet_processors)
