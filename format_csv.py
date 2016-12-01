#!/bin/env python

# take a bunch of files reporting jobs/user and format them so that I can read them in pandas and plot

# gerbaudo@at301 ~ $ tar czf - tmp/at3-queue-monitor/ | ssh lxplus.cern.ch "cat > foo.tgz"
# gerbaudo@lxplus ~ $ tar xzf ~/foo.tgz

# davide.gerbaudo@gmail.com
# Jan 2016

import glob
import os
import re
import subprocess

def main():
    ""
    input_files = sorted(glob.glob('tmp/at3-queue-monitor/*.txt'))
    cmd = "grep --no-filename \" : \" tmp/at3-queue-monitor/*.txt | awk '{ print $1 }' | sort | uniq"
    usernames = sorted(getCommandOutput(cmd)['stdout'].split())
    print ','.join(['date ']+usernames)
    re_min_sec = re.compile(r':\d{2}:\d{2}')
    for input_file_name in input_files:
        timestamp = os.path.splitext(os.path.basename(input_file_name))[0]
        min_sec = re_min_sec.search(timestamp)
        if min_sec:
            timestamp = timestamp.replace(min_sec.group(), ':00:00')
        counts = dict()
        with open(input_file_name) as input_file:
            for line in input_file.readlines():
                if ':' not in line: continue
                user, jobs = line.split(':')
                user, jobs = user.strip(), jobs.strip().split()[0].strip()
                counts[user] = jobs
        print ','.join([timestamp] + [counts[u] if u in counts else '0' for u in usernames])

def getCommandOutput(command, cwd=None):
    "lifted from supy (https://github.com/elaird/supy/blob/master/utils/io.py)"
    p = subprocess.Popen(command, shell = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE, cwd = cwd)
    stdout,stderr = p.communicate()
    return {"stdout":stdout, "stderr":stderr, "returncode":p.returncode}



if __name__=='__main__':
    main()
