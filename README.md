# Map Reduce Implementation using Go

This is a Map Reduce implementation using Go

## To Get Started
1. Open two terminal instances, one for the coordinator and one or more for the workers.
2. Make sure both terminals are in the main directory of the project.

## Starting the Coordinator
In the coordinator terminal, run the following command:

<pre>
```bash
go run mrcoordinator.go pg-*.txt
```
</pre>

This command will start the coordinator and a server. The coordinator will take the input txt files (8 in total) and add their filenames to the map task channel.

## Starting the Workers
In the worker terminal(s), run the following command:

<pre>
```bash
go run mrworker.go wc.so
```
</pre>

This command will start a worker instance that listens to the server. The worker will receive a map task from the channel and process it. The computation performed by the worker is key-value counting of words in the txt file. The resulting key-value pairs will be added to intermediate files.

## Reduce Phase
Once all the map tasks are finished, the coordinator will start the reduce phase. It will assign reduce tasks to the workers using a new channel.

### Bucket Number Assignment
Before receiving the reduce tasks, the workers need to be assigned a bucket number. This assignment is done to manage and balance the workload. Each worker will receive a bucket number assignment from the coordinator.

### Processing Reduce Tasks
Using the assigned bucket number, the worker will request the associated reduce task files from the coordinator. The worker will then process the reduce tasks.

## Output
After completing the reduce tasks, each worker will produce output files for their assigned bucket number. These output files can be consolidated into a single output file at the end if needed.
# MapReduce
