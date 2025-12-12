# GreenSecurityGNS
setup for GNS3 in order to run distributed task using hadoop. 

Part of the Green Security project.
Link to the project's repository:

https://github.com/sapirani/GreenSecurityMeasurementAndOptimizationFramework

## Execute the GNS 

### Configure the main working directory
Download the entire ivan directory.
Make sure to add execution premissions to that directory by running the command:

`chmod -R +x "path_to_ivan_dir"`

-> _Notice_: if you pass the files from windows to the linux, you must run the command (before building the images):

`sed -i -e 's/\r$//' ./build_gns_images.sh` 


### Build GNS images
Enter the ivan directory and run the command:

`sudo ./build_gns_images.sh`

### Configure the GNS GUI
First, Open the terminal and run: `sudo -E gns3`.
Next, the GUI will open a screen. 

- If you run the gns on the server - Select "open existing project...". click Open and than Open again on the first project.
- Else, create new project or open your own project.

Now, configure where the gns server should run:

- If you run the gns with local server - you don't need to configure anything special.
- If you run the gns with server that runs on a virtual machine (mainly in windows) - you need to configure the server in the gns Gui.
After configuring the gns server (if needed) go to EDIT -> PREFERENCES -> DOCKER -> DOCKER CONTAINERS to configure the containers.
Click on NEW to create new container. Choose the image of the specific container. Do it for:
1. resourcemanager
2. namenode
3. historyserver
4. datanode X num_of_datanodes (for each datanode create environment variable called DATANODE_NUMBER)
Lastly, click on APPLY.

### Run GNS GUI
Now, you can move to the main screen the containers that you want to run.
Connect them using a "Ethernet switch".
If needed, you can connect to the switch a NAT component (to access nodes outside of the network - e.g. the web). If you add a NAT, remember to configure a **real** ip address using DHCP (you can configure it manually on the container itself before turning on the containers).
Next, you can select all the connected containers and click on the green arrow to start all the components.

## Important Note
Do not add more than one continer for resourcemanager, namenode, historyserver.
The reason is that gns adds a trailing number to the image name. The names resourcemanager-1, namenode-1, and historyserver are hard-coded in this framework.
Adding another container for each type is currently not supported.
It is ok to have multiple containers of datanodes. The valid names are datanode-1, datanode-2, datanode-3, etc.

### Run the parallel task
First, enter the namenode container.
You can choose the size of the parallel task by changing the file "generate_random_words.py".

Execute the file using the command `python3 generate_random_words.py`.
Notice that this file can receive 3 parameters: number_of_words, len_of_word, file_name. If you send all 3 - you don't need to change the file manually.
Then, run the command:

`hadoop fs -mkdir /input`

and then 

`hadoop fs -put <file of random words> /input`

After finishing to define the parallel task, exit the namenode container.
Now, enter the resourcemanager node.
Run the command 

`cd /home`

Change the tasks' configuration inside (press 'i' to insert, then ':wq' to save, or ':q!' tp quit)
`vim automatic_experiments_parameters.py`

Run the tasks
`python3 automatic_experiments.py`

Or, run a single task without resource measurement code (i.e., the scanner):
`python3 run_task.py`


### Supported Configuration Fields

| Group | Field | Shortcut | Default | Type | Constraints | Description |
|:------|:------|:----------|:---------|:------|:-------------|:-------------|
| **Task Definition** | `input_path` | `-i` | `/input` | `Path` | — | HDFS path to the input directory. |
|  | `output_path` | `-o` | `/output` | `Path` | — | HDFS path to the output directory. |
|  | `mapper_path` | `-mp` | `/home/mapper.py` | `Path` | — | Path to the mapper implementation. |
|  | `reducer_path` | `-rp` | `/home/reducer.py` | `Path` | — | Path to the reducer implementation. |
| **Parallelism & Scheduling** | `number_of_mappers` | `-m` | `2` | `int` | `gt=0` | Number of mapper tasks. |
|  | `number_of_reducers` | `-r` | `1` | `int` | `gt=0` | Number of reducer tasks. |
|  | `map_vcores` | `-mc` | `1` | `int` | `gt=0` | Number of vCores per map task. |
|  | `reduce_vcores` | `-rc` | `1` | `int` | `gt=0` | Number of vCores per reduce task. |
|  | `application_manager_vcores` | `-ac` | `1` | `int` | `gt=0` | Number of vCores for the application master. |
|  | `shuffle_copies` | `-sc` | `5` | `int` | `gt=0` | Parallel copies per reduce during shuffle. More copies speed up shuffle but risk saturating network or disk I/O. |
|  | `jvm_numtasks` | `-jvm` | `1` | `int` | `gt=0` | Number of tasks per JVM to reduce JVM startup overhead. |
|  | `slowstart_completed_maps` | `-ssc` | `0.05` | `float` | `ge=0`, `le=1` | Fraction of maps to finish before reduce begins. Higher values delay reduce start and reduce shuffle load. |
| **Memory** | `heap_memory_ratio` | `-hmr` | `0.8` | `float` | `0 < hmr ≤ 1` | Ratio of container memory allocated to JVM heap. |
|  | `map_memory_mb` | `-mm` | `1024` | `int` | `gt=0` | Memory per map task (MB). |
|  | `reduce_memory_mb` | `-rm` | `1024` | `int` | `gt=0` | Memory per reduce task (MB). |
|  | `application_manager_memory_mb` | `-am` | `1536` | `int` | `gt=0` | Memory for application master (MB). |
|  | `sort_buffer_mb` | `-sb` | `100` | `int` | `gt=0` | Sort buffer size (MB). |
|  | `min_split_size` | `-sm` | `0` | `int` | `ge=0` — human-readable units supported | Minimum input split size (supports `B`, `KB`, `MB`, `GB`). Larger values reduce number of map tasks (lower startup overhead) but may reduce parallelism. |
|  | `max_split_size` | `-sM` | `134217728 (128 MB)` | `int` | `gt=0` — human-readable units supported | Maximum input split size (supports `B`, `KB`, `MB`, `GB`). Determines mapper count together with input size. |
|  | `map_min_heap_size_mb` | `-mhm` | `2` | `int` | `gt=0` | Initial heap size for mapper JVM. |
|  | `map_max_heap_size_mb` | `-mhM` | `256` | `int` | `gt=0` | Maximum heap size for mapper JVM. |
|  | `map_stack_size_kb` | `-ms` | `1024` | `int` | `gt=0` | Stack size per mapper thread. |
|  | `reduce_min_heap_size_mb` | `-rhm` | `2` | `int` | `gt=0` | Initial heap size for reducer JVM. |
|  | `reduce_max_heap_size_mb` | `-rhM` | `256` | `int` | `gt=0` | Maximum heap size for reducer JVM. |
|  | `reduce_stack_size_kb` | `-rs` | `1024` | `int` | `gt=0` | Stack size per reducer thread. |
| **Shuffle & Compression** | `io_sort_factor` | `-f` | `10` | `int` | `gt=0` | Number of streams merged during map output sort. |
|  | `should_compress` | `-c` | `False` | `bool` | — | Enable compression of map outputs before shuffle (reduces network traffic at the cost of CPU). |
|  | `map_compress_codec` | `-mcc` | `CompressionCodec.DEFAULT` | `CompressionCodec` (enum) | — | Compression codec for map output. See `CompressionCodec` enum for options (e.g., `DEFAULT`, `GZIP`, `SNAPPY`, ...). |
|  | `map_garbage_collector` | `-mgc` | `ParallelGC` | `GarbageCollector` | — | Garbage collector used by mapper JVM. |
|  | `reduce_garbage_collector` | `-rgc` | `ParallelGC` | `GarbageCollector` | — | Garbage collector used by reducer JVM. |
| **JVM & Garbage Collection Settings** | `map_garbage_collector_threads_num` | `-mgct` | `1` | `int` | `gt=0` | GC thread count for mapper JVM. |
|  | `reduce_garbage_collector_threads_num` | `-rgct` | `1` | `int` | `gt=0` | GC thread count for reducer JVM. |



#### Note!
it is highly recommended to install the Pydantic plugin for Pycharm (for autocompletion and typing)
Press shift+shift quickly, type 'Plugins' and press enter.
Inside the marketplace - search for 'Pydantic' and install

### Collect results
After the task is done, you can go back to the terminal and enter the ivan folder. 
Now you can run: 

`sudo python3 ./nodes_configuration_code/sender_scanner_results.py n` where n is the number of **total** containers that are part of the network.

### Change scanner parameters
You can change the parameters of all the connected containers automaticaly by changing the program_parameters_template.py file inside the nodes_configuration_code directory.
Then, run the command: 

`sudo python3 ./nodes_configuration_code/update_volumes.py n` where n is the number of **total** containers that are part of the network.

## Execute scanner on regular container
First, configure images as above.
Now, go to the terminal.

1. Check the ids of the generated images: `docker images`
2. Select an image and run: `docker run -it <img_id> bash`
   * <img_id> is the first 3 digits of the id of the image that you chose.
   * You can add a specific flag to this command in order to control the core that the container will run on.
3. Now, you have a running container! make sure that you are in a directory called `Scanner`.
4. You can change the scanner parameters using `nano program_parameters.py`.
5. You now need to activate the environment by running: `. ../green_security_venv/bin/activate`
6. Run the scanner: `python3 scanner.py`
7. After the run will end, you will see a "results_x" directory
   * x is the name of the container
   * Now you can see and analyze the results.
  
## Configure Elastic Logs
The scanner program writes logs into local elastic database.
In order for it to succeed, we should configure containers of elastic that will run localy on the server.
1. go into the local elastic directory using the command: `cd /home/gns3/elastic-start-local`
2. execute `docker compose down` to shut down existing containers (if something is wrong with the previous ones)
3. In the file `docker-compose` file change the ip addresses from 127.0.0.1 to be 0.0.0.0.
4. execute `docker compose up --build --wait` to turn on the containers.
5. Run the command `docker ps` and make sure that 2 new containers with elastic in their name exist.
   * `docker.elastic.co/kibana/kibana:9.0.1` on port 5601
   * `docker.elastic.co/elasticsearch/elasticsearch:9.0.1` on ports 9200 and 9300.
6. Now, you can go to the firefox and run: `localhost:5601`
7. You might need to enter username and password:
   * Username: elastic
   * Password: saved in .env file in the elastic directory (from step 1)
8. Go to the burger sign in the top left of the site and clicke on Discover.
9. In the DataView tab click on `scanner` to view the logs collected by running the scanner.

## For Developers

### Support DNS resolutions of nodes hostname
The NAT cloud in GNS serves as DHCP server and DNS server.
We use dhclient as a DHCP client.

dhclient requests an ip from the NAT cloud, and also announces the node's hostname (as configured in /etc/dhcp/dhclient.conf).
That hostname is translated to the ip in the NAT cloud, and that's how other nodes receive the ip.

by default, when resolving a domain name, the nodes request to translate both to IPV4 (A request) and IPV6 (AAAA request). The NAT cloud probably does not support IPV6.
This situation causes failures in IPV6 translations and a lot of time wasted on retries (evem though IPV4 succeeds right away).
Hence, we disbale IPV6 requests in the /etc/resolve.conf file using the no-aaaa option.
Since dhclient restarts the /etc/resolve.conf in every request (hence, out no-aaaa option is being deleted), a hook for that restart function was created in such a way that the new function does nothing.
The hook is an executable file (/etc/dhcp/dhclient-enter-hooks.d/noupdate-resolv) that overrides the function `make_resolv_conf` of dhclient, and basically does nothing.

This logic resides inside the `entrypoint.sh` file.

In case we want that the DNS will be translated without trailing additions by GNS (e.g., "namenode" instead of "namenode-1", "resourcemanager" instead of "resourcemanager-1" and so on),  put it in the entrypoint:

```
#cut out the -<number> suffix from hostname
HOSTNAME_FULL=$(hostname)
HOSTNAME_PREFIX="${HOSTNAME_FULL%%-*}"

# write the HOSTNAME_PREFIX instead of the gethostname() in /etc/dhcp/dhclient.conf
SOURCE_FILE=/etc/dhcp/dhclient.conf
sed -i "s/gethostname()/\"$HOSTNAME_PREFIX\"/g" "$SOURCE_FILE"
```

