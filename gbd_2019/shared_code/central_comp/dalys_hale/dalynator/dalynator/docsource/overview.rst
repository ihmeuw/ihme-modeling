General Comments on the Dalynator and Burdenator
************************************************

These two applications move a lot of data. To reduce runtimes, the computation is performed in
distributed jobs and must be launched on the **cluster (within a qlogin)**. They are both
Python programs.

They both uses the jobmon system (aka Central Job Monitor) to control and monitor all their (sub) jobs on the Cluster.

Jobmon
======

Every dalynator and burdenator job connects to the jobmon. Each job reports when it starts, when it stops,
and whether it was successful or died with an exception.
There is now one central jobmon server running on a VM accessible from the cluster,
therefore you will not need to start and stop jobmon. If there are problems with jobmon, slack the sci-comp-gbd channel.

