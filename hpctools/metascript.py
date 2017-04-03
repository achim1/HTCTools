"""
Inspect objects and writes new files which contains object source
"""

import json

from time import ctime
from os import chmod
from os.path import split

from utils import RotatingFileNamer,strip_file_ending

class MetaScript(file):
    """
    A script which will contain the source of 
    added objects
    """
    
    def __init__(self,filename,mode):

        path,name        = split(filename)
        #ending           = name.split(".")[-1]
        #ending           = "." + ending
        #name             = name.rstrip(ending)
        name,ending      = strip_file_ending(filename)
        self.filename    = RotatingFileNamer(path, name, ending)
        file.__init__(self,self.filename,mode)
        self.code_buffer = """#! /usr/bin/env python
        
# This script was auto-created by invoking MetaScript at %s\n\n     
""" %ctime()
        
    def __str__(self):
        return "<Metascript with filename: %s>" %self.filename
    
    def __repr__(self):
        return "<Metascript with filename: %s>" %self.filename
   
    def add_import(self,module):
        """
        import a module in the new script
        """
        
        import_string = "import %s\n\n" %(module.__name__)
        self.code_buffer += import_string
    
    def add_variable(self,name,value):
        """
        add a variable and its correspondend value to the script
        """
        
        vstring = "%s=%s\n\n" %(name,value)
        self.code_buffer += vstring
         
    def add_function(self,func,func_args=[],func_kwargs={},decorator=None):
        """
        Add a function with args and kwargs to 
        the new script
        """    
        func_string  = "from %s import %s\n\n" %(func.__module__,func.__name__)
        if decorator is not None:
            func_string += "from %s import %s\n\n" %(decorator.__module__,decorator.__name__)
            func_string += "@%s\n" %decorator.__name__ 
        func_string += "%s(*%s,**%s)\n\n" %(func.__name__,func_args.__repr__().replace("'",""),func_kwargs.__repr__())
        self.code_buffer += func_string
        
    def add_json_dumpable(self,name,dumpable):
        """
        Add any object which can be digested by json.dumps
        """
        
        jsonic = json.dumps(dumpable)
        self.code_buffer += "\n%s=%s \n\n" %(name,jsonic)
        
    def write_exectutable(self):
        """
        Write an exectutable script
        """
        self.write(self.code_buffer)
        chmod(self.filename,0755) # make it executable
        self.close() # it does not make sense to add something after file is closed 
        