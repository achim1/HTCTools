"""
Job I/O and monitoring tools
"""

import subprocess as sub
import sys
import re
import tables
import os
import inspect
import bz2
import resource
import numpy as n
import dashi as d

from os.path import join,split,exists,isdir,isfile
from os import walk,sep
from glob import glob   
from subprocess import Popen 
    

from time import sleep

from .. import Logger,CONFIG
import analysis_package.datasets.database as file_db

from .structs import pdg_to_ptype,ptype_to_pdg

########################################

def _getAFSToken():
    """
    Keep the job alive and get a new afs token,
    so that it can still write data to disk
    """
    Popen(['kinit', '-R'])
    return None

########################################

def strip_file_ending(filename):
    """
    Extract the char-sequance after the dot from a string
    :param filename: string
    :return: string,
    """
    pat = re.compile(r"(?P<name>.+)(?P<ending>\..{2,3})$")
    match  = pat.search(filename).groupdict()
    name,ending = match["name"],match["ending"]
    while True:
        add_end = pat.search(name)
        if add_end is None:
            break
        else:
            add_end = add_end.groupdict()
            name = add_end["name"]
            ending = add_end["ending"] + ending

    return name,ending


########################################

class KeepAlive:
    """
    Periodically call a function and keep it 
    alive with an always freshly gathered afs-token
    """ 
    def __init__(self,func,cycle=3600):
        self.cycle = cycle
        
    def __call__(self,func,*args,**kwargs):
        while True:
            func(*args,**kwargs)


###########################################

def KeepAliveCall(func,sleeptime=3600):
    """
    Periodically get afs token
    """
    while True:
        _getAFSToken()
        func()
        sleep(sleeptime)

####################################

def ListSlicer(list_to_slice,slices):
    """
    helper function to slice a list
    """

    #FIXME: there must be something in the std lib..
    # implementing this because I am flying anyway
    # right now and have nothing to do..

    if slices == 0:
        slices = 1 # prevent ZeroDivisionError
    maxslice = len(list_to_slice)/slices
    if (maxslice*slices) < len(list_to_slice) :
        maxslice += 1
    Logger.info("Sliced list in %i slices with a maximum slice index of %i" %(slices,maxslice))
    for index in range(0,slices):
        lower_bound = index*maxslice
        upper_bound = lower_bound + maxslice
        thisslice = list_to_slice[lower_bound:upper_bound]
        #if not thisslice: #Do not emit empty lists!
        yield thisslice

######################################

def MaskMerger(mask1,mask2):
    """
    combine 2 bundled masks
    """
    
    newmask = d.objbundle.bundle(mask1)
    for k in mask1.keys():
        newmask.set(k,n.logical_and(mask1.__getattribute__(k),mask2.__getattribute__(k)))
    return newmask

########################################

def AddMaskToBundleMask(bundlemask,mask):
    """
    add another mask to an existing bundle of masks
    """
    
    for k in bundlemask.keys():
        bundlemask.set(k,n.logical_and(bundlemask.__getattribute__(k),mask))
    return bundlemask

######################################

def ExitFunc(thedict,thekey):
    """
    A simple exit function which exits if 
    key is not conained in dict
    """
    
    try:
        thedict[thekey]
    except KeyError:
        sys.exit(0)
        
#########################################

def GetInfiles(infile,globpattern='*.i3.bz2',data=False,no_gcd=True):
    """
    Check if infile is a directory and if this is true, then 
    get all files which match globpattern
    """

    if not exists(infile):
        raise SystemError('Path does not exist! %s' %infile)
    
    elif isdir(infile):
        tmpindirs  = []
        tmpinfiles = []

        if infile.startswith("/acs"):
            ls = sub.Popen(["ls","-a",infile],stdout=sub.PIPE,stdin=sub.PIPE)
            ls_out = ls.communicate()[0].split()
            try:
                ls_out.remove(".")
            except:
                pass
            try:
                ls_out.remove("..")
            except:
                pass
            
            tmpindirs = ls_out
            
            for direc in tmpindirs:
                if os.path.isdir(join(infile,direc)):
                
                    if direc == "logs":
                        continue
                    ls = sub.Popen(["ls","-a",join(infile,direc)],stdout=sub.PIPE,stdin=sub.PIPE)
                    ls_out = ls.communicate()[0].split()
                    try:
                        ls_out.remove(".")
                    except:
                        pass
                    try:
                        ls_out.remove("..")
                    except:
                        pass
                    tmpinfiles += [join("dcap://" + infile,join(direc,item)) for item in ls_out]
                elif os.path.isfile(join(infile,direc)):
                    tmpinfiles += [join("dcap://" + infile,direc)]
                infiles = tmpinfiles
            
            pattern = globpattern.replace("*","")
            infiles = filter(lambda x : x.endswith(pattern),infiles)
        else:   
            for item in walk(infile,followlinks=True):
                tmpindirs.append(item[0])
            infiles = reduce(lambda x,y : x+y,map(glob,[join(direc,globpattern) for direc in tmpindirs]))
  
    else:
        infiles = [infile]

    if no_gcd:
    # remove any GCD
        infiles = filter(lambda x : not "GCD" in x, infiles)
    # remove any EHE files
    infiles = filter(lambda x : not "EHE" in x, infiles)
    
    if not len(infiles):
        Logger.warn("No files found for searching in %s!" %infile.__repr__())
    else:
        Logger.info("Found %i files,firstfile %s" %(len(infiles),infiles[0].__repr__()))       
    
    return sorted(infiles)

################################################################
        
def GetDataSetIDFromFile(path):
    """
    Tries to find out the dataset number of any filepath
    """
    
    #dataset_pattern = re.compile(r"[\_A-Za-z0-9]\.([0-9]{6})\.")
    dataset_pattern = re.compile(r"[\_A-Za-z0-9]\.([0-9]{6})")
    filename        = split(path)[1]
    
    number = 3000
    
    result = dataset_pattern.search(filename)
    if result is not None:
        number = int(result.groups()[0])
        if number > 4000: # ignore invented data number
            return number
    
    Logger.warning("Was not able to find out dataset id for path %s, will return %i" %(path,number) )   
    if "IC79_data_Run00" in path:
        number = 1000

    if ("IC86" in path) and ("data" in path):
        number = 2000
        pattern = re.compile("_(Run[0-9]{8})")
        match   = pattern.search(path)
        runnr   = match.groups()[0]
        if runnr.endswith("0"):
            number = 2000
        else:
            number = 2200
        
    if number == 1000 and not path.endswith("0.i3.bz2"):
        number = 1200
    
    if number < 1000: #mugun specific
        number = 3000
    
        
    Logger.warning("Patching dataset number to be %i" %number)
    return number

#########################################

def GetRunIDFromCorsikaFile(path):
    """
    Tries to find out the dataset number of any filepath
    """
    
    
    #dataset_pattern = re.compile(r"[\_A-Za-z0-9]\.[0-9]{6}\.([0-9X]{6})\.")
    dataset_pattern = re.compile(r"[\_A-Za-z0-9]\.[0-9]{6}\.([0-9X]{6})")
    filename        = split(path)[1]
    
    number = 0
    
    result = dataset_pattern.search(filename)
    if result is not None:
        try:
            number = int(result.groups()[0])
        except:
            number = int(result.groups()[0].rstrip("X").ljust(6,"0"))
    return number
    
#########################################################

def GetDataGCD(infile,ic86=False):
    """
    Find assigned GCD file for input datafile
    infile: eithre runnr (int) or filename
    """
    

    if isinstance(infile,int):
        runnr = infile
    else:
        pattern = re.compile("_(Run[0-9]{8})")
        match   = pattern.search(infile)
        runnr   = match.groups()[0]

    gcd = ""
#     if ic86:
#         runnr = infile.split('_')[-2]
#     else:
#         runnr = infile.split('_')[-1] 
#        
#     runnr = runnr.split('.')[0]
#     try:
#         int(runnr)
#     except:
#         
#         runnr = infile.split('_')[-4]
#         runnr = runnr.split('.')[0]
    #print runnr 
    if ic86:
        all_gcd = GetInfiles(CONFIG.get("datapaths","ic86datagcd"),globpattern='*GCD*',no_gcd=False)
    else:
        all_gcd = GetInfiles(CONFIG.get("datapaths","ic79datagcd"),globpattern='GCD*',no_gcd=False)
        all_gcd += GetInfiles(CONFIG.get("datapaths","ic79gsgcd"),globpattern='GCD*',no_gcd=False)
    for thisgcd in all_gcd:
        if runnr in thisgcd:
            gcd = thisgcd
            break 

    if not gcd:
        raise ValueError("No gcd found")
    Logger.info("Found gcd %s" %gcd.__repr__())
    return gcd

###################################################

#FIXME: this is doubled in datasets.datasets
def GenericDatasetGetter(ds_module):
    """
    Create a function which returns datasets from 
    a given module
    """

    def cleaner(x):
        try:
            return not(x.__name__.startswith("_")) 
        except:
            return False


    all_sets = []
    for name, obj in inspect.getmembers(ds_module,cleaner):
        if inspect.isclass(obj):
            all_sets.append(obj())
                
    return all_sets

###############################################

#FIXME: this method is doubled to avoid import errors! this 
#needs to be fixed!
def GetDataSet(dset_id,module="analysis_package.ic79cascades.analysis_datasets",coincident=False):
    """
    Get the dataset from a module
    """

    all_dsets = CONFIG.items("datasetdb")
    all_dsets = [a[1] for a in all_dsets]

    d_sets = None
    #FIXME this must not depend on module name!
    #FIXME this is too ugly!
    all_available_sets = []
    for module in all_dsets:
        exec("import %s as d_sets" %module)
        all_available_sets += GenericDatasetGetter(d_sets)
    #all_available_sets = reduce(lambda i,j :i+j,[GenericDatasetGetter(mod)for mod in [corsika,data,nugen]])
    #all_available_sets = GenericDatasetGetter(d_sets)
    matchers = []
    for klass in all_available_sets:
        if str(klass.dataset) == str(dset_id):
            matchers.append(klass)
        elif str(klass.old_dataset_no) == str(dset_id):
            matchers.append(klass)
            
    if len(matchers) > 1:
        matchers = [thematch for thematch in matchers if thematch.coincident == coincident]
    elif len(matchers) == 1 and coincident:
        Logger.warning("There might be no coincident dataset available for dset_id %i" %dset_id)
    if len(matchers) > 1:
        Logger.warning("Non distint dataset id, %s for dataset %i" %(matchers.__repr__(),dset_id))
    try:    
        return matchers[0]
    except IndexError:
        Logger.warning("Could not find dataset %s in module %s" %(dset_id.__repr__(),module.__repr__()))
        return None

####################################################

def CreateOutfileName(infile,ending="*.i3",outfilepath = CONFIG.get("datapaths","ic79l3apath"),coincident=False,merge=None):
    """
    Compile a suitable path for ta processed file
    """

    if isinstance(infile,list):
        thefile = infile[0]
    else:
        thefile = infile

    filename,__ = strip_file_ending(thefile)
    if merge is not None:
        filename += '.merged' + str(merge) + "."

    filename = split(filename)[1]

    datasetid = GetDataSetIDFromFile(filename)
    dataset = GetDataSet(datasetid,coincident=coincident)
    #detector = "IC79"
    #if dataset is None:
    #    dataset = GetDataSet(datasetid,coincident=coincident,module="analysis_package.ic86cascades.analysis_datasets")
    #    detector = "IC86"
    Logger.info("Getting dataset for id %i for detector %s" %(datasetid,dataset.detector))
    outfile = str(join(join(outfilepath,dataset.type),filename + ending))
    return outfile

################################################

def CreateInfilesList(infile,coincident=False,gcd=None):
    """
    Compile a list of files to compute and take care of the gcd
    being at first
    """
    
    dataset  = 0
    filelist = []
    if isinstance(infile,list):
        datasetid  = GetDataSetIDFromFile(infile[0])
        filelist = infile
    else:
        datasetid  = GetDataSetIDFromFile(infile)
        filelist = [infile]

    dataset = GetDataSet(datasetid,coincident=coincident)
    #if dataset is None:
    #    dataset = GetDataSet(datasetid,coincident=coincident,module="analysis_package.ic86cascades.analysis_datasets")
    #    # for data, always return gcd + single file
    if dataset.type == "data": 
        
        if gcd is not None:
            Logger.warning("Can not use this gcd %s with data!" %gcd)

        gcd = GetDataGCD(filelist[0], ic86= (dataset.detector == "IC86"))
        Logger.warn("Overiding gcd with specific gcd for experimental data %s " %gcd.__repr__())
        filelist = [gcd] + filelist      
#         if isinstance(infile,list):
#             Logger.warning("Can not merge multiple data files!")
#             filelist = [gcd] + infile
#         elif not "l3a" in infile:
#             gcd = GetDataGCD(infile,ic86 = (dataset.detector == "IC86")) 
#             Logger.warn("Overiding gcd with specific gcd for experimental data %s " %gcd.__repr__())
#             filelist = [gcd] + filelist         
    else:
        filelist = [gcd] + filelist
    
    return filelist
        


################################################
 
def UniversalFileGetter(infile,ending=".i3",gcd=None,outfilepath = CONFIG.get("datapaths","ic79l3apath"),merge=None):
    """
    Make sure always a list of is returned, even if infile is a single file
    return the correct outfilepath dependent on the infile type
    """
 
    if merge is not None:
        ending = '_m' + str(merge) + ending
 
    Logger.info("Printing representation of infile(s) %s ...\n...done " %infile.__repr__()) 
 
    filelist = []
    if isinstance(infile,list):
        filelist   = infile
        # get the first file and inspect it
        dbfile     = file_db.GetFileByName(filelist[0])
         
    else:
        filelist   = [infile]
        dbfile     = file_db.GetFileByName(infile)
       
    #print infile
    #print dbfile
    #print dir(dbfile)
    filename   = split(dbfile.filename)[1]
    datatype   = dbfile.type
    detector   = dbfile.detector
 
    # for data, always return gcd + single file
    if datatype == "data": 
         
        if gcd is not None:
            Logger.warning("Can not use this gcd %s with data!" %gcd)
        if isinstance(infile,list):
            Logger.warning("Can not merge multiple data files!")
         
        if not "l3a" in filename:
            gcd = GetDataGCD(filename,ic86 = (detector == "IC86")) 
            Logger.warn("Overiding gcd with specific gcd for experimental data %s " %gcd.__repr__())
            filelist = [gcd] + filelist
                 
    else:
        filelist = [gcd] + filelist
         
    # the outfile will contain the merged infiles                
    outfile = str(join(join(outfilepath,datatype),filename.replace('.bz2',"").replace('.i3',"") + '_l3a_' + ending))            
    return filelist,outfile
    
######################################################## 

def GetTiming(func):
    """
    Use as decorator to get the effective execution time of the decorated function 
    """

    from time import time
    

    def wrapper(*args,**kwargs):
        t1 = time()
        res = func(*args,**kwargs)
        t2 = time()
        seconds = t2 -t1 
        mins  = int(seconds)/60
        hours = int(seconds)/3600
        res_seconds  = int(seconds)%3600
        mins         = int(res_seconds)/60
        left_seconds = int(res_seconds)%60
        Logger.info('Execution of %s took %i hours, %i mins and %i seconds' %(func.func_name,hours,mins,left_seconds))
        Logger.info('Execution of %s took %f seconds' %(func.func_name,seconds))
        max_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        Logger.info('Execution might have needed %i kB in memory (highly uncertain)!' %max_mem)
        return res

    return wrapper

##################################

def RandomDelayedStart(func):
    """
    Use as decorator to delay the function start
    """
    
    from time import sleep
    from random import random
    
    def wrapper(*args,**kwargs):
        
        #sleep at maximum 5mins (300s)
        sleeptime = random()*300
        Logger.info("Delaying job start for %4.2f seconds" %sleeptime)
        sleep(sleeptime)
        res = func(*args,**kwargs)
        return res
    
    return wrapper

#############################################
       
def NotifyOnKill(func):
    """
    Define a handler which catches the Kill signal and notifies the user
    """
    
    import signal
    
    def kill_handler(signum,frame):
        """
        Do a graceful exit of the script
        """   
        pass
        
###################################################

def RotatingFileNamer(path,name,suffix,nzeros=0):
    """
    Returns a unique file name in the directory path,
    adding a increasing number to the filename
    path: a valid path
    name: the filename
    suffix: the ending, e.g ".i3"
    nzeros: the number of zeros preceeding the number, e.g. "foo001.i3"
    """

    if not exists(path):
        raise OSError("Path does not exist: %s" %path)

    cnter = 0
    while True:
        if not nzeros:
            if isfile(join(path,name+str(cnter) + suffix)):
                cnter += 1
            else:
                break
        else:
            if isfile(join(path,name+str(cnter).zfill(nzeros) + suffix)):
                cnter += 1
            else:
                break

    if not nzeros:
        return join(path,name + str(cnter) +suffix)
    else:
        return join(path,name + str(cnter).zfill(nzeros) + suffix)

##############################################

def GetEnvshells(self,sim=False):
    """
    Return a list of available envshells
    """
    envlist = []
    if sim:
        shellpath = CONFIG.get("software","icesim")
    else:
        shellpath = CONFIG.get("software","icerec")
 
    for root,dirs,__ in walk(shellpath):
        if 'src' in dirs:
            dirs.remove('src')
            dirs.remove('build')
            envshell = root + sep + 'build' + sep + "env-shell.sh"
            envlist.append(envshell)


    envlist.sort()
    return envlist

################################################

def CheckHDFIntegrity(infiles,checkfor = None ):
    """
    Checks if hdfiles can be openend and returns 
    a tuple integer_files,corrupt_files
    """

    integer_files = []
    corrupt_files = []
    allfiles = len(infiles)
    
    for file_to_check in infiles:
    
        test = sub.Popen(['h5ls','-g',file_to_check],stdout=sub.PIPE,stderr=sub.PIPE)
        __,error = test.communicate()

        if error:
            Logger.warning(error)
            corrupt_files.append(file_to_check)

        else:
            if checkfor != None:
                f = tables.openFile(file_to_check)
                try:
                    f.getNode(checkfor)
                except tables.NoSuchNodeError:
                    Logger.info("File %s has no Node %s" %(file_to_check,checkfor))
                    corrupt_files.append(file_to_check)
                    continue
                finally:
                    f.close() 

            integer_files.append(file_to_check)

    Logger.debug("These files are corrup! %s",corrupt_files.__repr__())
    Logger.info('%i of %i files corrupt!' %(len(corrupt_files),allfiles))
    return integer_files,corrupt_files

##########################################################

def CheckBzipIntegrity(filename):
    """
    Check if a bzipped file is corrupt
    """
    if isinstance(filename,list):
        if len(filename) > 1:
            Logger.warning("will not check all files in list")
        filename = filename[0]

    fine = False
    if filename.startswith('dcap://'):
        filename = filename.replace('dcap://',"")
    try:
        bz2.BZ2File(filename).read()
        fine = True
    except (IOError,EOFError) as e:
        Logger.error("%s: File %s corrupt!" %(e,filename))
    Logger.info("finish")
    return fine

##########################################################

def CalculateMD5Sum(filename):
    pass




##################################################################

def HoursToSeconds(runtime_string):
    """
    Get a string of format "8.00" and convert it to
    seconds
    """
    
    hours,mins = runtime_string.split(".")
    return float(hours)*3600. + float(mins)*60.
    
####################################    
   
    
def GetRunFromFile(f):
    """
    Extract run number from file
    """
    pattern = re.compile("_(Run[0-9]{8})")
    match   = pattern.search(f)
    runnr   = match.groups()[0]
    #print runnr
    return runnr
    #return os.path.split(f)[1].split("_")[4].lstrip("Run00")

###########################################

def GetRunNrFromFile(f):
    """
    Extract run number from file
    """
    pattern = re.compile("_Run([0-9]{8})")
    match   = pattern.search(f)
    runnr   = match.groups()[0]
    return int(runnr)
    #return os.path.split(f)[1].split("_")[4].lstrip("Run00")


###########################################

def ParseGoodrunList(goodrunlist,datatype="Burn"):
    """
    Return the files corresponding to the Burnsample taken from the goodrun list
    Set datatype either "Burn" or "Blind"
    """
    
    assert (datatype == "Burn" or datatype == "Blind")
    
    with open(goodrunlist) as g:
        # type 0 denotes burnsample
        goodlist     = filter(lambda i : i[7] == "GOOD",[line.split() for line in g.readlines()])              
        if datatype == "Burn":
            bs       = filter(lambda i : i[0].endswith("0"),goodlist)
        if datatype == "Blind":
            bs       = filter(lambda i : not i[0].endswith("0"),goodlist)
        runs        = [b[0] for b in bs]
        total_ltime = reduce(lambda i,j : i+j,map(HoursToSeconds,[b[3] for b in bs])) 
        g.close()
    
    Logger.info("Total livetime of this %s sample is %s" %(datatype,total_ltime.__repr__()))
    return runs

###########################################

def GetDataSampleFromRuns(runlist,datadir=CONFIG.get("datapaths","ic86rawdatapath")):
    """
    Filter the datasample to get only files from the burnsample
    """
    
    files      = GetInfiles(datadir,globpattern='*.i3.bz2', data=True)
    run_files  = [f for f in files if GetRunFromFile(f) in runlist]
    return run_files

###########################################################

def ConvertPrimaryToPDG(pid):
    """
    Convert a primary id in an i3 file to the new values
    given by the pdg
    """
    try:
        return ptype_to_pdg[pid]
    except KeyError:
        return pid

###########################################################

def ConvertPrimaryFromPDG(pid):
    """
    Get back the old classification scheme
    from pdg codes
    """
    try:
        return pdg_to_ptype[pid]
    except KeyError:
        return pid



###########################################################

def GetFileFromDB(file_id,outfilepath,ending,dataset,coincident,analysis_level,merge=None):
    # FIXME
    Logger.info("Search for file with id %s in db" %file_id.__repr__())
    if merge is not None:
        start_id = file_db.GetMinID(dataset=dataset, coincident=coincident, type=type, analysis_level=analysis_level,hdf="i3")
        infile   = file_db.getSingleChunkfromDB(file_id, start_id, merge, dataset=dataset, coincident=coincident, analysis_level=analysis_level,hdf="i3")

        Logger.debug("Got file")
        filename    = split(infile[0].filename)[1]
        datatype    = infile[0].type
        coincident  = infile[0].coincident 
        probe       = infile[0] 
        detector    = infile[0].detector
        
        if datatype == "data": 
            ic86 = False
            if detector == "IC86":
                ic86=True
            gcd = GetDataGCD(filename,ic86=ic86) 
            Logger.warn("Overiding gcd with specific gcd for experimental data %s " %gcd.__repr__())

    else:
        probe      = file_db.GetFileById(file_id)
        filename   = split(probe.filename)[1]
        datatype   = probe.type
        detector   = probe.detector
        coincident = probe.coincident
        infile     = [probe]
        # for data, always return gcd + single file
        if datatype == "data": 
            ic86 = False
            if detector == "IC86":
                ic86=True
            gcd = GetDataGCD(filename,ic86=ic86) 
            Logger.warn("Overiding gcd with specific gcd for experimental data %s " %gcd.__repr__())

    filelist       = [dbfile.filename for dbfile in infile]
    if (gcd is not None) or datatype == "data":
        filelist = [gcd] + filelist 
        
    outfilesubpath = join(outfilepath,datatype)    
    outfile = str(join(outfilesubpath,filename.rstrip('.bz2').rstrip('.i3') + '_l3a_' + ending))
    return filelist,outfile


