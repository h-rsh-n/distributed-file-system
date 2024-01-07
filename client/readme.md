# Yet Another Distributed File System (YADFS) - Client

## Overview

The client directory contains the implementation of the client-side commands and the interactive shell for the Yet Another Distributed File System (YADFS).

## Dependencies

Ensure you have the required dependencies installed before running the client. Use the following command to install them:

```bash
pip install prompt_toolkit tqdm rpyc
```

## Working
The client tries to establish a connection with the namenode at port 18860. If the connection is established, it provides an interface to run client commands, which are then sent to the namenode.

To run the client, navigate to the client directory and execute the following command:

```bash
python3 client.py
```

## TODO
Create: `mkdir`

Delete: `delete`

Move: `move`

Copy: `copy`

List files and directories: `show` and `ls`

Traverse directories: `cd`

Upload file: `upload`

Download File: `download`

## Understanding all the Commands

### `cd` - Change Current Directory

```bash
cd target_directory
```

Change the current directory.

### `delete` - Delete Files or Directories

```bash
delete [-force] file1 ... fileN
```

Delete files with given pathnames. Use `-force` to delete a file or directory with files inside.

### `help` - Get Help Information

```bash
help [cmd]
```

List available commands with "help" or detailed help with "help cmd".

### `mkdir` - Create New Folder

```bash
mkdir dir_name
```

Create a new folder with the given `dir_name`.

### `move` - Move File or Contents

```bash
move source_path target_path
```

Move file/contents from `source_path` to `target_path`.

### `upload` - Upload File to DFS

```bash
upload file_local_path target_dfs_directory
```

Upload a file from the local file system to DFS.

### `copy` - Copy File to Target Directory

```bash
copy file_path target_dir
```

Copy a file to the target directory.

### `download` - Download Files from DFS

```bash
download file_dfs_path target_local_directory
```

Download files from DFS to the local system.

### `ls` - List Directory Contents

```bash
ls [target_directory]
```

List directory contents.

Create new files with the given names and extension.

### `quit` - Quit the Application

```bash
[exit] [x] [q] [Ctrl-D]
```

Quit the application.

### `show` - Show File System Structure

```bash
show [any_other_command]
```

Shows the file system after performing any other command given.
