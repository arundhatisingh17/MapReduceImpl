# MapReduceImpl

1) Client Interface: We will be developing our Map Reduce Implementation in Python. The client module (client.py) will specifically be responsible for submitting jobs to the server module (scheduler.py) while including the following specifications:

    (a) The path to the dataset, which will be stored on HDFS.

    (b) The number of partitions, which corresponds to the number of output files. 

    (c) The user-defined map and reduce functions which will be imported into the worker nodes before invoking map and reduce functions. 

    (d) Optionally, users could mention a threshold for difference between the size of the original partition and the mapped partition for the map-reduce program to automatically make repartitioning decisions, before calling the shuffle function.

    (e) A custom hash partitioning function can also optionally be defined by the user. In the case where the user does not provide a hash partitioning function, a default partition function/shuffle function will be utilized. 

    (f) The client module will communicate with the server module via grpc calls and the following functions can be invoked by the client module to increase observability:

            (i) A job can be submitted to server.py. Users can refer to proto.py to understand the schema of the job and submit requests by complying to that format.

            (ii) The server.py will typically generate a print statement, declaring the commencement of a specific job request. 

            (iii) If the print statement is not invoked within a certain duration of time, the user will have an option of triggering the job execution again to indirectly assign a higher
                  priority for that specific job request. 

            (iv) Once the job has been executed, the user will be able to access the files from HDFS.

 
2) Language and Framework: All the files will be written using Python 3.10. The following libraries and frameworks will be used:

    (a) gRPC will be utilized for communicating with the server file. The proto file will contain schema-related specifications like the format of the requests being sent to the server and the
        return type. The return object will depict the status of the job which the user can poll frequently to check if the map/reduce functions have correctly been invoked. 

    (b) HDFS will be used for creating the shared storage file system. 

    (c) Docker Compose will be utilized for orchestrating the client, server and worker nodes (mapper and reducer).

    (d) Parquet tables will be used for storing intermediate data to ensure low overhead caused by schema inference.
   

3) System Communication: Our map-reduce program will utilize gRPC for system communication:

    (a) The client submits a job to the server via a gRPC call.

    (b) The server (scheduler) parses the job request based on the schema defined in the .proto file.

    (c) The scheduler partitions the input data into the specified number of chunks and assigns them to mapper worker nodes.

    (d) Each mapper executes the user-defined map function on its assigned partition and writes intermediate results to Parquet files in a local or shared directory.

    (e) After all mappers complete, the scheduler initiates the shuffle phase, where intermediate files are redistributed across reducer nodes based on the partitioning function (user-defined or
        default).

    (f) The reducers execute the user-defined reduce function, generating the final output files (one per partition) and writing them back to HDFS.


    (g) Upon completion, the server sends a gRPC response to the client, indicating the job status and HDFS output path.
   

4) Shared Storage: For shared storage between workers, we will be using HDFS.


  All input, intermediate, and final output data will be stored in HDFS to ensure fault tolerance, distributed accessibility, and persistence across worker containers.


  We will mount the HDFS directories as shared Docker volumes to allow read/write access between the client, server, and worker containers to leverage the benefits of distributed storage.

  The storage workflow will operate as follows:

    (a) Input Data Upload: The client uploads datasets to HDFS using client.py.

    (b) Map Phase: Worker nodes read their assigned partitions directly from HDFS.

    (c) Intermediate Data: Map outputs pushed back to HDFS before the shuffle phase.

    (d) Reduce Phase: Reducer nodes fetch the relevant intermediate files from HDFS, perform aggregation, and store the final results back into HDFS.

    (e) Output Retrieval: The client can download or view the final results from HDFS after job completion, and can pipe the files to another loop of Map-reduce or simply merge all the output files depending on the use-case.



5) File Formats: For file formats, we will be exclusively using Parquet files for both input/output files as well as the intermediate files. This is due to the advantages that Parquet files have over other file formats in terms of efficiency.

6) Testing: To evaluate our MapReduce implementation, we will run test queries with our MapReduce implementation and compare their execution time against the same queries when run on a traditional MySQL database. 

7) Special Feature Testing: To test the impact of our special feature, we will run the same test queries discussed previously on our MapReduce implementation with and without our special feature. This way we can isolate the effect of our special feature. For each test query, we will make a bar graph depicting the execution time of our query on a traditional MySQL database, on our MapReduce implementation without our special feature, and on our MapReduce implementation with our special feature.

