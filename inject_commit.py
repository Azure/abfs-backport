#!/usr/bin/env python3

import sys
import subprocess
import re

# Get the git commit hash
commid_id = subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode("utf-8").strip()
p = re.compile(r'(.*)\$\:.*\:\$(.*)')
repl_pattern = r'\1$:' + commid_id + r':$\2'

for line in sys.stdin.readlines():
    sys.stdout.write(p.sub(repl_pattern, line))
