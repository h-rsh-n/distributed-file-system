# Yet Another Distributed File System (YADFS)

## Prerequisites:
This DFS is designed to run on Ubuntu with a python version 3.5 or above. Please ensure the following packages are installed by running the command:

```bash
pip install prompt_toolkit tqdm
sudo pip install rpyc
```

## Description of Libraries and Why They're Used:

1. **rpyc:** A transparent and symmetrical Python library for remote procedure calls (RPC). It facilitates communication between different components of the distributed file system.

2. **prompt_toolkit:** A library for building powerful interactive command-line applications. It enhances the user experience in the client interface.

3. **tqdm:** A fast, extensible progress bar for loops and CLI. It provides a visual indication of the progress during tasks like file transfers.

## How to Run the Files:

### 1. Make a Storage Place:
```bash
sudo mkdir /yadfs
```

### 2. Datanode:
Navigate to the path of the datanode and run the following command. Repeat this process three times with different port numbers to set up three datanodes.

```bash
cd path/to/datanode
sudo python3 datanode.py [port to run datanode]
```
*Note: Datanode can currently be run on ports 18861, 18862, 18863. If unspecified, it takes port 18861 by default.*

### 3. Namenode:
Navigate to the path of the namenode and run the following command:
```bash
cd path/to/namenode
python3 namenode.py
```

### 4. Client:
Navigate to the path of the client and run the following command:
```bash
cd path/to/client
python3 client.py
```

*Note: A session is started on the client, and a prompt is generated from the root directory. Use the `help` command to view all available commands, `show` to display the entire directory structure, and `quit` to exit from the session.*
