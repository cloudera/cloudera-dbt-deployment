import subprocess
import os
import sys

#Sample usage: 
#python ~/scripts/job-dbt-docs-generate.py ~/dbt-hive-example/dbt_hive_demo --profiles-dir ~/dbt-hive-example/dbt_hive_demo --vars '{"key1": "value1"}'

if len (sys.argv) < 2 :
    print("Usage: python job-dbt-docs-generate.py <project-path> <arg1..argn>")
    sys.exit (1)

else:
    DBT_PATH=sys.argv[1]
    if os.path.exists(DBT_PATH):
        os.chdir(DBT_PATH)
        dbtArgs=['dbt','docs','generate']
        for arg  in  sys.argv[2:]:
            dbtArgs = dbtArgs + [arg]
        print("Executing {}".format(dbtArgs))
        if (subprocess.run(dbtArgs).returncode != 0):
            print("{} failed".format(dbtArgs))
            sys.exit(1)
    else:
        print("Path with dbt_project.yml and profiles.yml does not exist")
        sys.exit(1)
