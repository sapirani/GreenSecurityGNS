# GreenSecurityGNS
setup for GNS3 in order to run distributed task using hadoop. 

Part of the Green Security project.
Link to the project's repository:

https://github.com/sapirani/GreenSecurityMeasurementAndOptimizationFramework

## Execute the GNS 

### Configure the main working directory
Download the entire ivan directory.
Make sure to add execution premissions to that directory by running the command:
'chmod -R +x "path_to_ivan_dir"'

### Build GNS images
Enter the ivan directory and run the command:

'sudo ./build_gns_images.sh'

### Configure the GNS GUI
First, Open the terminal and run: 'sudo -E gns3'.

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

### Run the parallel task
First, enter the namenode container.
You can choose the size of the parallel task by changing the file "generate_random_words.py".
Execute the file using the command `python3 generate_random_words.py`.
Notice that this file can receive 3 parameters: number_of_words, len_of_word, file_name. If you send all 3 - you don't need to change the file manually.
Then, run the command:
`hadoop fs -mkdir /input`
and then `hadoop fs -put <file of random words> /input`
After finishing to define the parallel task, exit the namenode container.
Now, enter the resourcemanager node.
Run the command `/home/run_task_with_scanner.sh n` or the command `/home/run_task_without_scanner.sh n` - based on whether we want to run the scanner code during the parallel task.
The parameter n represents the number of additional datanodes that are connected to the network.

### Collect results
After the task is done, you can go back to the terminal and enter the ivan folder. 
Now you can run: `sudo python3 ./nodes_configuration_code/sender_scanner_results.py n` where n is the number of **total** containers that are part of the network.

### Change scanner parameters
You can change the parameters of all the connected containers automaticaly by changing the program_parameters_template.py file inside the nodes_configuration_code directory.
Then, run the command: `sudo python3 ./nodes_configuration_code/update_volumes.py n` where n is the number of **total** containers that are part of the network.
