# TODO #
## DB-workpack ##

  * add version to db to check if db is correct (half way done...)

  * make jobid unique by appending 2-digit year

  * think about filesystems (table sources)

## ReadDB ##
  * check if db is correct version

  * plot all nids (for one job) over time

  * plot first nid compared to average of nids over time (for one job)




# Done #

  * rename Table jobs.end to jobs.t\_end

  * rename Table jobs.start to jobs.t\_start

  * sum of all nids (for one job) over time
get\_sum\_nids\_to\_job(jobID)

  * find other nids for a job
get\_nid\_to\_Job(self, jobID):

  * find nids doing (most) IO in time interval -> readDB.py
getAll\_Nid\_IDs\_Between(self, timeStamp\_start, timeStamp\_end, threshold\_b = 0)

  * find job to a nid at a given time
getAll\_Jobs\_to\_Nid\_ID\_Between(self, timeStamp\_start, timeStamp\_end, nidID)

  * find user for a job
get\_User\_To\_Job(self, jobID)