from __future__ import unicode_literals, print_function
from prompt_toolkit import print_formatted_text as print, HTML
from prompt_toolkit.styles import Style
import os, sys
import math
import time
import random
from uuid import uuid4
import json

import rpyc
from tqdm import tqdm

from cmd import Cmd

style = Style.from_dict({
  'orange': '#ff7e0b',
  'red': '#e74c3c',
  'green': '#22b66c',
  'blue': '#0a84ff',
  'i': 'italic',
  'b': 'bold',
})

CONN = None
NS_IP = None
NS_PORT = None

#Function to connect to the namenode
def connect_to_ns(count_retry=1, max_retry=3):
  global CONN
  print(HTML('<orange>Connecting to namenode...</orange>'))
  try:
    CONN = rpyc.connect(NS_IP, NS_PORT)
  except ConnectionError:
    CONN = None
  
  time.sleep(1)

  if CONN is None:
    print(HTML("<red>Connection couldn't be established.\n</red>"))
    time.sleep(1)
    if count_retry<=max_retry:
      print("Attempts so far = {}. Let's retry!".format(count_retry))
      return connect_to_ns(count_retry+1, max_retry)
    else:
      print("Maximum allowed attempts made. Closing the application now!\n")
      return False
  else:
    print(HTML("<green>Connected!</green>"))
    time.sleep(1)
    return True

# Handle connection or print error
def ns_is_responding():
  global CONN
  try:
    temp = CONN.root.refresh()
    return True
  except EOFError:
    CONN = None
    print(HTML("<red>Error</red>: Connection to namenode lost!"))
    time.sleep(0.7)
    print(HTML("<green>Retrying</green> in 5 seconds..."))
    time.sleep(5)
    
    return True if connect_to_ns() else False


# put in storage server
def put_in_ss(ss, remote_path, block_name, block_data):
  ss = ss.split(":")
  try:
    conn = rpyc.connect(ss[1], ss[2])
    target_path = os.path.join(remote_path, block_name)
    conn.root.put(target_path, block_data)
  except ConnectionRefusedError:
    print('Connection refused by {} while trying to put block {}'.format(ss, block_name))
  return None

# get from storage server
def get_from_ss(ss, remote_path):
  ss = ss.split(":")
  try:
    conn = rpyc.connect(ss[1], ss[2])
    return conn.root.get(remote_path)
  except ConnectionRefusedError:
    print('Connection refused by {} while trying to get {}'.format(ss, remote_path))

# Get printing format of the file size
def parse_size_from_bytes(num):
  num = float(num)
  # this function will convert bytes to MB.... GB... etc
  for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
    if num < 1024.0:
      return "%3.2f %s" % (num, x)
    num /= 1024.0

# Print the dictionary as a tree
def prettyDictionary(t,s):
  for key in sorted(t.keys()):
    if "blocks" in t[key]:
      print(HTML("   "*(s) + '|- ' + str(key)))
    else:
      key_str = "root" if key=="yadfs" else key
      if key_str=="root":
        print(HTML(" <b>ROOT</b>"))
      else:
        print(HTML("   "*s + '|- <b><green>' + str(key_str) + "</green></b>"))
    
      if not isinstance(t[key],list):
        prettyDictionary(t[key],s+1)

# Create a class to create and handle all prompts
class MyPrompt(Cmd):
  global CONN
  ABSOLUTE_ROOT = "/yadfs"
  CURRENT_DIR = '/yadfs'
  prompt = "[yadfs] (.) >> "
  intro = "Welcome to yadfs Distributed File System.\n > Type '?' or 'help' to see available commands.\n > The root volume is /yadfs/ \n"

  # The first executed
  def preloop(self):
    print(HTML('\n------------- <green>Session Started</green> -------------\n'))

  # Check if the args for a cmd are correct
  def parse_args(self, cmd_name, args, min_required=0, max_required=float('inf')):
    args = args.strip()
    args = args.split(" ") if len(args)>0 else args
    N = len(args)

    if N<min_required or N>max_required:
      if min_required == max_required:
        required = min_required
      else:
        required = "{} ".format(min_required)
        if math.isinf(max_required):
          required += "or more"
        else:
          required += "to {}".format(max_required)

      print(HTML('<red>Error</red>: <b>{}</b> expected <orange>{}</orange> args, got <orange>{}</orange>.'.format(cmd_name, required, N)))
      print(HTML("<green><b>TIP</b></green>: Try <b>help {}</b> for correct usage.\n".format(cmd_name)))
      return None
    return args

  # Pretty print the Help Results
  def print_help(self, form, result):
    form = form.split(" ")
    form[0] = "<orange>{}</orange>".format(form[0])
    form = " ".join(form)
    print(HTML("Usage Format: {} \nResult: {}\n".format(form, result)))

  # Pretty print the response to a command
  def print_response(self, response):
    output = "<green>Success</green>:" if response["status"]==1 else "<red>Failed</red>:"
    output += " {}".format(response["message"])
    print(HTML(output))
  
  # Pretty print the dictionary
  def print_dictionary(self, d):
    print('')
    print(HTML('<orange>File System</orange>:'))
    prettyDictionary(d, 0)
    print("")

  # End the session
  def do_quit(self, inp):
    print(HTML("\n-------------- <red>Session Ended</red> -------------- \n"))
    return True
  
  # Help for quit
  def help_quit(self):
    self.print_help('[exit] [x] [q] [Ctrl-D]', '<red>Quit</red> the application.')

  # Change the current directory
  def change_current_directory(self, dir):
    self.CURRENT_DIR = dir
    dir = '.' + dir[6:] if dir[:6]==self.ABSOLUTE_ROOT else dir
    self.prompt = "[yadfs] (" + dir + ") >> "
  
  # Main function for show command
  def do_show(self, args):
    args = args.strip().split(" ", 1)
    
    cmd = None
    myargs = ""
    if len(args)==1:
      cmd = args[0]
    elif len(args)>1:
      cmd, myargs = args
    
    if cmd and cmd!="show":
      my_cmd = "do_" + cmd
      try:
        method_to_call = getattr(self, my_cmd)
        method_to_call(myargs)
      except AttributeError:
        print(HTML('No such command <red>{}</red>'.format(cmd)))
    if ns_is_responding:
      result = json.loads(CONN.root.get("/yadfs"))["dfs"]
      self.print_dictionary(result)    
    
  # Print help for show command
  def help_show(self):
    self.print_help('show [any_other_command]', 'Shows file system after performing any_other_command given.')

  # return the path wrt absolute path
  def parse_path(self, arg):
    if arg==".":
      return self.CURRENT_DIR
    elif arg == "..":
      arg = self.CURRENT_DIR.rsplit("/", 1)[0]
      return self.ABSOLUTE_ROOT if arg=="" else arg
    elif arg[:6]==self.ABSOLUTE_ROOT:
      # return absolute path, remove '/' at the end if there is.
      return arg[:-1] if arg[-1]=="/" else arg
    else:
      # parse relative path
      return os.path.join(self.CURRENT_DIR, arg)

  # Main function for cd command
  def do_cd(self, dir):
    args = self.parse_args('cd', dir, 1, 1)

    if not ns_is_responding():  self.do_quit()

    if args:
      arg = self.parse_path(args[0])
      print(HTML('Changing dir to: <green>{}</green>'.format(arg)))
      result = json.loads(CONN.root.get(arg))
      if result["status"]==1:
        try:
          blocks = result["data"]["blocks"]
          result["status"] = 0
          result["message"] = "Can't <b>cd</b> to a file."
          self.print_response(result)
        except:
          self.change_current_directory(arg)
      else:
        self.print_response(result)

  # Print help for cd command
  def help_cd(self):
    self.print_help('cd target_directory', 'Change current directory.')

  # Main function for ls command
  def do_ls(self, dir="."):
    args = self.parse_args('ls', dir, 0, 1)
    if args is None: return
    try:
      arg = self.parse_path(args[0])
    except IndexError:
      arg = self.parse_path(args)
    
    if not ns_is_responding():  self.do_quit()
    result = json.loads(CONN.root.get(arg))

    if result["status"]==1:
      try:
        blocks = result["data"]["blocks"]
        result["status"] = 0
        result["message"] = "Can't <b>ls</b> to a file."
        self.print_response(result)
      except:
        items = result["data"].keys()
        out = []
        for x in items:
          if x[:2]!="__":
            try:
              blocks = result["data"][x]["blocks"]
              out.append("<b>{}</b>".format(x))
            except:
              out.append("<b><green>{}</green></b>".format(x))
        if len(items)==0: out.append("<b>Empty directory!</b>")
        print(HTML("\t".join(out)))
    else:
      self.print_response(result)
      
  # Print help for ls command
  def help_ls(self):
    self.print_help('ls [target_directory]', 'List directory contents.')

  # Main function for mkdir command
  def do_mkdir(self, dir):
    args = self.parse_args('mkdir', dir, 1, 1)
    if not ns_is_responding():  self.do_quit()
    if args:
      arg = self.parse_path(args[0])
      temp = json.loads(CONN.root.get(arg))
      if (temp["status"]==1):
        result = {
          "status": 0,
          "message": "Directory already exists!",
          "data": {}
        }
      else:
        result = json.loads(CONN.root.mkdir(arg))
      self.print_response(result)
  
  # Print help for mkdir command
  def help_mkdir(self):
    self.print_help('mkdir dir_name', 'Create new folder with given <orange>dir_name</orange>')
  
  # Main function for delete command
  def do_delete(self, files):
    args = self.parse_args('delete', files, 1)
    if args:
      if args[0] == "-force":
        force = True
        args = args[1:]
      else:
        force = False
      
      if not ns_is_responding():  self.do_quit()

      for arg in args:
        path = self.parse_path(arg)
        result = json.loads(CONN.root.delete(path, force_delete=force))
        
        if result["status"]==1:
          if path == self.parse_path(self.CURRENT_DIR):
            self.change_current_directory(self.ABSOLUTE_ROOT)

        self.print_response(result)
  
  # Print help for delete command
  def help_delete(self):
    self.print_help('delete [-force] file1 ... fileN', 'Delete files with given pathnames.\nUse <red>-force</red> to delete a file or directory with files inside.')

  # Main function for copy command
  def do_copy(self, args):
    args = self.parse_args('copy', args, 2, 2)
    if args:
      src = self.parse_path(args[0])
      dest = self.parse_path(args[1])

      if not ns_is_responding():  self.do_quit()

      result = json.loads(CONN.root.copy(src, dest))
      self.print_response(result)

  # Print help for copy command
  def help_copy(self):
    self.print_help('copy file_path target_dir', 'Copy a file to target directory.')

  def do_move(self, args):
    args = self.parse_args('move', args, 2, 2)
    if args:
      src = self.parse_path(args[0])
      dest = self.parse_path(args[1])

      if not ns_is_responding():  self.do_quit()

      result = json.loads(CONN.root.move(src, dest))
      if src == self.parse_path(self.CURRENT_DIR):
        self.change_current_directory(dest)
      self.print_response(result)

  # Print help for move command
  def help_move(self):
    self.print_help('move source_path target_path', "Move file/contents from source_path to target_path.")

  # Main function for upload command
  def do_upload(self, args):
    args = self.parse_args('upload', args, 2, 2)
    if args:
      local_path = args[0]
      remote_path = self.parse_path(args[1])
      
      if not ns_is_responding():  self.do_quit()
      res = json.loads(CONN.root.get(remote_path))
      
      result = {
        "status": 1
      }
      msg = []

      if not os.path.exists(local_path):
        result["status"]=0
        msg.append('No such resoure in local path!')
      
      if res["status"]==0:
        result["status"]=0
        msg.append('No such target remote directory!')
      
      if result["status"]==0:
        result["message"] = "\n".join(msg)
        self.print_response(result)
        return
      
      # get configurations
      replication_factor =  int(res["nsconfig"].get('replication_factor'))
      block_size = int(res["nsconfig"].get('block_size'))
      data_nodes_need = os.path.getsize(local_path)/replication_factor
      estimated_servers_need = os.path.getsize(local_path)/replication_factor
      if not ns_is_responding():  self.do_quit()
      storage_servers = CONN.root.get_alive_servers(estimated_servers_need + 4)

      ss_block_map = []
      total_size = os.path.getsize(local_path)
      blocks_len = math.ceil(total_size/block_size)
      print(HTML('File splitted into <green>{}</green> blocks.'.format(blocks_len)))
      done_size = 0
      with open(local_path, 'rb') as lf:
        buff = lf.read(block_size)
        i = 0
        while buff:
          i += 1
          print('Uploading block', i)
          block_name = str(uuid4())
          target_ss = random.sample(storage_servers, replication_factor) if len(storage_servers)>replication_factor else storage_servers
          
          # Pretty print the progress
          for ss in tqdm(target_ss):
            put_in_ss(ss, remote_path, block_name, buff)
            done_size += block_size
            ss_block_map.append([ss, block_name])
          
          buff = lf.read(block_size)

      # send this block map to namenode for storing
      if not ns_is_responding():  self.do_quit()
      local_path = local_path[:-1] if local_path[-1]=="/" else local_path
      try:
        file_name = local_path.rsplit("/", 1)[1]
      except IndexError:
        file_name = local_path
      
      file_name = os.path.join(remote_path, file_name)
      result = json.loads(CONN.root.new_file(file_name, ss_block_map))
      self.print_response(result)

  # Print help for upload command
  def help_upload(self):
    self.print_help("upload file_local_path target_dfs_directory", "Upload file from local file system to DFS.")

  # Main function for download command
  def do_download(self, args):
    args = self.parse_args('download', args, 2, 2)
  
    if args:
      remote = self.parse_path(args[0])
      local = args[1]

      if not ns_is_responding():  self.do_quit()
      #print(remote)
      res = json.loads(CONN.root.get(remote))
      
      result = {
        "status": 1
      }
      msg = []

      if not os.path.exists(local):
        result["status"]=0
        msg.append('No such target local directory!')
      
      blocks = []
      if res["status"]==0:
        result["status"]=0
        msg.append('No such resource in remote path!')
      else:
        blocks = res["data"]["blocks"]
      
      if not blocks or len(blocks)==0:
        result["status"]=0
        msg.append('No file/empty file in given remote directory.')
      
      if result["status"]==0:
        result["message"] = "\n".join(msg)
        self.print_response(result)
        return
      
      out_fname = remote.rsplit("/", 1)[1]
      response = {
        "status": 1, "message": ""
      }
      target_out_file = os.path.join(local, out_fname)
      target_remote_path = remote.rsplit("/", 1)[0]
      print(HTML('<green>{}</green> blocks need to be fetched.'.format(len(blocks))))
      with open(target_out_file, "wb") as lf:
        i = 0
        for block_id in tqdm(blocks):
          i += 1
          target_ss = CONN.root.get_ss_having_this_block(block_id)
          data = None
          problematic_ss = []
          for ss in target_ss:
            try:
              block_path = os.path.join(target_remote_path, block_id)
              data = get_from_ss(ss, block_path)
              lf.write(data)
              print(HTML('Block <b>{}</b> fetched.'.format(i)))
              break
            except:
              data = None
              problematic_ss.append(ss)
          
          if len(problematic_ss)>0 and data is None:
            print('Problematic storage servers were', problematic_ss)
          
          if not data:
            response["status"]=0
            response["message"]="At least 1 block missing! Can't retrieve the file."
            self.print_response(response)
            return

      response["message"] = "File saved to {}".format(local)
      self.print_response(response)
      
  # Print help for download command
  def help_download(self):
    args = self.print_help('download file_dfs_path target_local_directory', "Download files from DFS to local system.")

  # If any other invalid command is given
  def default(self, inp):
    if inp=='x' or inp=='q':
      return self.do_quit(inp)
    else:
      print(HTML('"{}" is NOT a valid command! \n > Try <green>help</green> to see available commands.\n'.format(inp)))
  
  def emptyline(self):
    pass

  
def main(ns):
  global CONN
  global NS_IP, NS_PORT
  NS_IP = ns[0]
  NS_PORT = ns[1]

  # After connection start the Prompt/Session
  if connect_to_ns():
    MyPrompt().cmdloop()
  
# Run the client on localhost
if __name__=='__main__':
  local = ['localhost', 18860]
  main(local)
