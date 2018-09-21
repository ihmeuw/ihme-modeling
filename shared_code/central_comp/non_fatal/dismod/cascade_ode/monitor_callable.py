import sys
from jobmon.monitor import JobMonitor

if __name__ == "__main__":
    out_dir = sys.argv[1]
    print('Creating a mondb in %s' % out_dir)
    mon = JobMonitor(out_dir)
    mon.run()
