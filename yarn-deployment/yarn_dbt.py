#!/usr/bin/env python

import os
import sys

import dbt_command
import dbt_docs

dbt_commands = ["debug", "run", "seed", "test", "snapshot"]
dbt_docs = ["docs"]

def main(args):
  if (args[0] in dbt_commands):
     print("call dbt_commands")
     dbt_command.main(args)
  elif (args[0] in dbt_docs):
     print("call dbt_docs")
     dbt_docs.main()
  else:
     print("option not supported" + args[0])

if __name__=="__main__":
   if (len(sys.argv) > 1):
      main(sys.argv[1:])
   else:
      print("usage: yarn_dbt [run|debug|seed|test|snapshot|docs]")

