Deployment
Deployment will be done on a single VM, via Docker compose.  There should be four MapReduce worker containers, each limited to use 1 CPU, such that better performance is achievable if tasks are distributed across all workers (your VM has 4 CPUs).

A client program running outside of any container should allow submission of jobs, with the following info:
where inputs can be found in shared storage
where outputs should be written to shared storage
a user-provided file containing map and reduce functions
the number of map tasks
the number reduce tasks

Your final repo should have a README.md that makes it easy for users (like me) to evaluate your code.  In particular, tell them:
what to build (e.g., protobufs, Docker images, etc)
what to install (e.g., specific packages for the client)
how to upload example data (that you provide!) to shared storage
how to write a MapReduce job (provide a simple, complete example that works with your example data)
how to submit the job
Design Doc
What will the client interface look like?  E.g., what commands will be run to upload starter data to shared storage, and what commands will be used to submit a MapReduce job?
What language will you use to implement the system?
what will you use for communication in the system (client => boss, boss => workers, etc)?
What will you use for shared storage between the workers?  For example, you could use HDFS, a shared volume mount across all containers (since you will demo on a single VM), or something else.
What formats will you use for input/output data?  For intermediate data?  For example, you could use protobufs, Arrow, Parquet, or something else.
What tests will you write to evaluate your system?
How will you evaluate your system's performance?  In particular, what measurements and plots will you perform/create to show that your "special feature" (see below) works as intended?
Special Feature(s)
Beyond the basic MapReduce functionality, your implementation should do at least one "cool" thing, such as:

implement combiners
launch extra tasks when stragglers are detected
handle worker failures
schedule tasks to run where data is (e.g., if you deploy HDFS DataNodes in the same containers where your MapReduce workers are running)
Evaluation
I'll evaluate your work based on the following:
quality of your spec
quality of your code
teamwork (was work distributed reasonably among team members, and is there any record of you giving each other code reviews via pull requests?)
quality of documentation in your repo (it should be very easy for me to run and try your code)
quality of your performance evaluation (are the plots well designed, and do they show something meaningful)
quality of your tests
quality of your in-person demo

