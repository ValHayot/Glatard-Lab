#!/usr/bin/env python

from radical.entk import Pipeline, Stage, Task, AppManager
from glob import glob
from subprocess import Popen, PIPE
from os import linesep, environ, path as op

ssh = "gsissh -p 2222 bridges.psc.xsede.org"
dir_ = "/pylon5/mc3bggp/vhayot/bb_blocks"
out_dir = "/pylon5/mc3bggp/vhayot/inc_out"
iterations = 10

if environ.get('RADICAL_ENTK_VERBOSE') == None:
    environ['RADICAL_ENTK_REPORT'] = 'True'


# Description of how the RabbitMQ process is accessible
# No need to change/set any variables if you installed RabbitMQ has a system
# process. If you are running RabbitMQ under a docker container or another
# VM, set "RMQ_HOSTNAME" and "RMQ_PORT" in the session where you are running
# this script.
hostname = environ.get('RMQ_HOSTNAME', 'localhost')
port = environ.get('RMQ_PORT', 5672)


def main():

    cmd = "{0} 'ls {1}'".format(ssh, dir_)
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    out,_ = p.communicate()
    
    out = out.decode('utf-8').strip().split(linesep)

    fullpaths = [op.join(dir_, p) for p in out]
    print(fullpaths)


    # Start radical entk pipeline

    p = Pipeline()

    for i in range(iterations):

        s = Stage()

        for fp in fullpaths:

            t = Task()
            t.name = 'Incrementation {}'.format(i)
            t.pre_exec = ['source /home/vhayot/miniconda3/etc/profile.d/conda.sh', 'conda activate radenv']
            t.executable = 'python /home/vhayot/inc.py'

            if i == 0:
                t.arguments = [fp, out_dir, i]
            else:
                # Note: assuming all data is accessible through shared dir
                # radical entk functions without sharedfs, however
                t.arguments = [op.join(out_dir, "it-{0}-{1}".format(i-1, op.basename(fp))), out_dir, i]

            s.add_tasks(t)

        # Create a new stage everytime there's a dependency
        p.add_stages(s)

    
    appman = AppManager(hostname=hostname, port=port)

    appman.resource_desc = {
        'resource': 'xsede.bridges',
        'walltime': 20,
        'cpus': 5,
        'project': 'mc3bggp',
        'schema': 'gsissh'
    }

    appman.workflow = set([p])

    appman.run()


if __name__ == '__main__':
    main()

