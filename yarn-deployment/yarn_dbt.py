#!/usr/bin/env python

import os
import sys

import dbt_commands
import dbt_docs

commands = ["debug", "run", "seed", "test", "snapshot"]
docs = ["docs"]

def main():
  args = sys.argv

  if (len(args) <= 1):
     print("usage: yarn_dbt [run|debug|seed|test|snapshot|docs]")
     sys.exit(10)

  args = sys.argv[1:]

  if (args[0] in commands):
     print("call dbt_commands", dbt_commands)
     dbt_commands.main(args)
  elif (args[0] in docs):
     print("call dbt_docs")
     dbt_docs.main()
  else:
     print("option not supported" + args[0])
