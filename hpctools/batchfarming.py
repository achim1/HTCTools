"""
Management of Zeuthen Batch farmjobs
"""

import shlex
import sys
import tarfile
import time
import logging
import zmq
import os 
import sqlite3
import re

from .. import CONFIG, Logger
from .metascript import MetaScript
from .utils import ListSlicer, RotatingFileNamer, GetEnvshells, ExitFunc, CheckBzipIntegrity
from .databases import OpenSQLiteDB, CloseSQLiteDB
import databases as file_db


from os.path import join,split
from subprocess import Popen,PIPE
from glob import glob
from copy import copy
from time import sleep

#########################################

class SubmissionError(Exception):
    pass

#########################################

def JobPropertiesFactory(jobtype):
    """
    Return predefined sets of JobProperties
    """
    props = JobProperties()
    if jobtype == "long_run_high_mem":
        props.fortyeighthour_queue()
        props.request_ram(10000)
        props.as_sl6()
    else:
        raise ValueError("Please give a keyword for a certain job settings preset")
    return props


###################################################

class JobProperties(object):
    """
    A wrapper for numerous job properties
    """
    def __init__(self):
        self.mailoption           = ''
        self.ready_for_submission = False
        self.ressources           = {'hcpu':'h_cpu=47:59:59','hvmem':'h_rss=2000M'} # parameter for -l
        self.shelltype            = '/bin/zsh' # parameter for -S
        self.finished             = False
        self.project              = "icecube"
        self.arrayjob             = False
        self.usetmpdir            = False
        self.envshell             = self.choose_envshell("V04-04-01")
        self.logdir               = "/dev/null"

    def specify_tmpdir_size(self,megabytes):
        """
        Request the size of $TMP
        """
        self.ressources['tmpdir'] = 'tmpdir_size=' + str(megabytes) +  'M '
        self.usetmpdir            = True

    def send_mail(self,mailoption):
        """
        mailoption could be either 'b','e' or 'a'
        """ 
        self.mailoption = mailoption

    def as_nuastro(self):
        """
        Set the project to z_nuastr
        """
        self.project = "z_nuastr"

    def short_queue(self):
        """
        Submit for short queue
        """
        self.ressources['hcpu']='h_cpu=00:29:59'

    def specify_logdir(self,directory):
        """
        Giva a name for the logfile
        """
        self.logdir = directory

    def as_sl6(self):
        """
        Select nodes with Scientific Linux 6
        """
        self.ressources['os'] = 'os=sl6'

    def as_sl5(self):
        """
        Select nodes with Scientific Linux 5
        """
        self.ressources['os'] = 'os=sl5'

    def as_arrayjob(self,minarray,maxarray):
        """
        Run job as arrayjob in range min, max
        """
        self.arrayjob = (minarray,maxarray)

    def request_gpu(self):
        """
        Run job on gpu nodes
        """
        self.ressources['gpu'] = "gpu"

    def specify_hcpu(self,hours,minutes):
        """
        Sel hard limit for cpu time
        """
        self.ressources['hcpu']="h_cpu=" + str(hours) + ":" + str(minutes) + ":59"

    def request_ram(self,megabites):
        """
        Set hard limit for ram
        """
        self.ressources['h_rss'] ="h_rss="+str(megabites)+"M"

    def twelvehour_queue(self):
        """
        Run job in 12 h queue
        """
        self.ressources['hcpu']='h_cpu=11:59:59'

    def twentyfourhour_queue(self):
        """
        Run job in 24 h queue
        """
        self.ressources['hcpu']='h_cpu=23:59:59'

    def fortyeighthour_queue(self):
        """
        Run job in 48 h queue
        """
        self.ressources['hcpu']='h_cpu=47:59:59'

    def prettyprint(self):
        """
        Give a nice representation which can be written in a
        submit file
        """

        prettystring = '#$ -S %s \n' %self.shelltype
        for item in self.ressources.keys():
            prettystring += '#$ -l %s \n' %self.ressources[item]
      
        prettystring += '#$ -cwd\n'
        prettystring += '#$ -j y \n'
        prettystring += '#$ -o %s \n' %self.logdir
        prettystring += '#$ -P %s \n' %self.project
        if self.arrayjob:
            prettystring += '#$ -t %i-%i\n' %(self.arrayjob)

        if self.mailoption:
            prettystring += '#$ -m %s\n\n' %self.mailoption + '\n\n'
        else:
            prettystring += '\n'

        return prettystring

    def choose_envshell(self, qualifier, sim=False):
        """
        Pick an env-shell for this job
        """     
        for shell in GetEnvshells(qualifier,sim=sim):
            if qualifier in shell.split("/"):
                self.envshell = shell
                return shell

    def __repr__(self):
        return self.prettyprint()

    def __str__(self):
        return self.prettyprint()
        

######################################################

class FarmJob(object):
    """
    A wrapper for desy batch farmjobs array-style
    """
    
    def __init__(self,script,jobprops,jobname="batch",script_args=[],delay=0):

        jobdir = CONFIG.get("farmjobs","farmscriptdir")

        self.script      = script
        self.jobname     = jobname
        self.jobprops    = jobprops
        self.script_args = script_args
        self.ready       = False
        self.jobfile     = RotatingFileNamer(jobdir,self.jobname,".sh")
        self.delay       = delay
        self.jobid       = 0
    
    def create_submission_file(self,copy_source=None,copy_dest=None):
        """
        Write the submission file to the 
        output directory
        """
        
        f = open(self.jobfile,'w')
        f.write('#!/bin/zsh \n\n')
        if self.delay:
            delay_cond = "if (($SGE_TASK_ID > 600)); then\n sleep 600\n else\n sleep $SGE_TASK_ID\n fi\n\n"
            f.write(delay_cond)
        specs = self.jobprops.prettyprint()
        f.write(specs)
        
        # implement Hape's trap
        f.write("trap 'echo Job successfully completed' 0\n")
        f.write("trap 'echo Job killed by hitting limit;exit 2' USR1\n\n")
        if self.jobprops.usetmpdir:
            
            assert(copy_source is not None)
            #assert(copy_dest is not None)
            
            f.write('cd $TMPDIR \n')
            if copy_source.startswith("/acs"):
                f.write("dccp " + copy_source + '. \n')
            else:
                f.write('cp ' + copy_source + ' . \n')

            
        f.write('source /afs/ifh.de/user/s/stoessl/.zshrc' + '\n') 
        f.write('envshell='+self.jobprops.envshell+'\n')
        f.write('script='+self.script.filename+'\n\n')
     
        commandostring = '$envshell $script '
        for s_arg in self.script_args:
            commandostring += s_arg
            

        f.write(commandostring)
        if self.jobprops.usetmpdir:
            #rm_filename = split(copy_source)[1]
            #f.write('rm -f ' + rm_filename + ' \n')
            if copy_dest is not None:
                f.write('cp * '  + copy_dest   + ' \n')
            
        f.close()
        self.ready = True

    def submit(self):
        """
        submit the job to the batch system
        """
        if not self.ready:
            raise SubmissionError('need to prepare submission file first, not submitted!')
        
        cline = 'qsub ' + self.jobfile   
        jobid = Popen(shlex.split(cline),stdout=PIPE).communicate()[0]
        self.jobid = jobid.split()[2]
        Logger.info('Job submitted with id %s!' %self.jobid)
        return self.jobid
            
##############################################################

def ArrayJobFiles(files,chunksize,subjobs=[]):
    """
    Slice infiles in chunks and return an dict with
    chunknumber, chunk
    """

    file_dict   = {}
    slices      = int(float(len(files))/chunksize)
    for index,file_slice in enumerate(ListSlicer(files, slices)):
        if subjobs:
            if (index + 1) in subjobs:
                file_dict[index + 1] = file_slice # 0 is not a valid index for arrayjobs
        else:
            file_dict[index + 1] = file_slice # 0 is not a valid index for arrayjobs
    
    # filter out lists of length 0
    filtered_file_dict = dict()
    for k in file_dict.keys():
        if file_dict[k]:
            filtered_file_dict[k] = file_dict[k]
    return filtered_file_dict

###############################################################

def SimpleFarmer(func,job_kwargs,jobname="batch.sh",func_args=[],func_kwargs={},decorator=None):
    """
    Calculate func on farm as simple job
    """
    raise NotImplementedError("SimpleFarmer is not implemented yet!")


#################################################################

def CopyFileToTmp(file,dcache=False):
    #subprocess.Popen("")

    pass

#################################################################

#FIXME: make it a class for inheritance
#class ArrayJobFarmer:

#    def __init__(self):
#        logdir      = CONFIG.get('logging','farmlogs') 
#        farmjobdir  = CONFIG.get('farmjobs','farmscriptdir')
#
#    def __call__(self,merge,files,jobprops,jobname="batch",func_kwargs={},delay=False,subjobs=[]):
#        pass
    
def ArrayJobFarmer(func,merge,files,jobprops,jobname="batch",func_kwargs={},delay=False,subjobs=[],presliced=False):
    """
    Calculate func on the farm with arrayjobs with properties defined in job_kwargs
    CAVE: func_args are managed automatically
    """

    #if jobprops.usetmpdir:
    #    assert merge == 1, "Can not currently copy more than 1 file to tmpdir"
    #    copy_files = files
    #    files = map(lambda x: os.path.split(x)[1],files)

    logdir      = CONFIG.get('logging','farmlogs') 
    farmjobdir  = CONFIG.get('farmjobs','farmscriptdir')
    
    if presliced:
        slicedfiles = files
        n_jobs  = len(slicedfiles.keys())
    else:
    # claculate n_jobs seperately, to avoid errors
        n_jobs      = len(ArrayJobFiles(files,merge).keys())
        slicedfiles = ArrayJobFiles(files,merge,subjobs=subjobs)

    #print slicedfiles
    #print False in map(bool,[j for j in slicedfiles.values()])
    # write a python script
    # magicindex trick:
    # ensure that the sys.argv[1] of the script is used to 
    # get the correct slize of files
    
    func_args    = ["files[magicindex]"] 
    farmpyscript = MetaScript(join(farmjobdir,jobname + ".py"),"w")
    farmpyscript.add_import(sys)
    farmpyscript.add_variable("magicindex","sys.argv[1]")
    farmpyscript.add_json_dumpable("files",slicedfiles)
    #print slicedfiles
    #raise
    if subjobs:
        farmpyscript.add_function(ExitFunc,["files","magicindex"],{},None)
    farmpyscript.add_function(func, func_args, func_kwargs)
    farmpyscript.write_exectutable()
    
    jobprops.as_arrayjob(1,n_jobs) # index starts at 1
    jobprops.specify_logdir(logdir)
    jobprops.as_sl6()
    #if jobprops.usetmpdir:
    #    job = FarmJob(farmpyscript,jobprops,jobname=jobname,script_args=["$SGE_TASK_ID"],delay=delay,copy_source)
    #else:
    job = FarmJob(farmpyscript,jobprops,jobname=jobname,script_args=["$SGE_TASK_ID"],delay=delay)
    job.create_submission_file()
    job.submit()
    Logger.info("Job %s submitted with a range of %i:%i!" %(job.jobid,job.jobprops.arrayjob[0],job.jobprops.arrayjob[1]))
    return int(job.jobid.split(".")[0])

######################################################################

def FitJobFarmer(func,jobprops,n_jobs,jobname="fit_",func_kwargs={},decorator=None,func_selectable_args=None):
    """
    Calculate func on the farm with arrayjobs with properties defined in job_kwargs
    CAVE: func_args are managed automatically
    """
    
    logdir      = CONFIG.get('logging','farmlogs') + "/fit/"
    farmjobdir  = CONFIG.get('farmjobs','farmscriptdir')
    
    # write a python script

    #func_args = []
    farmpyscript = MetaScript(join(farmjobdir,jobname + ".py"),"w")
    farmpyscript.add_import(sys)
    if func_selectable_args is not None:
        farmpyscript.add_variable("magicindex","int(sys.argv[1]) - 1")
        farmpyscript.add_json_dumpable("seeds",func_selectable_args)
        func_args    = ["seeds[magicindex]"]
    else:
        func_args    = ["sys.argv[1]"]

    farmpyscript.add_function(func, func_args, func_kwargs, decorator)
    farmpyscript.write_exectutable()
    
    jobprops.as_arrayjob(1,n_jobs) # index starts at 1
    #jobprops.specify_logdir(logdir) # NO LOG! who needs that?
    job = FarmJob(farmpyscript,jobprops,jobname=jobname,script_args=["$SGE_TASK_ID"])
    job.create_submission_file()
    job.submit()
    Logger.info("Job %s submitted with a range of %i:%i!" %(job.jobid,job.jobprops.arrayjob[0],job.jobprops.arrayjob[1]))
    return int(job.jobid.split(".")[0])

######################################################

def TarFiles(infiles,filename="logs.tar"):
    """
    tar files together
    """
    archive = tarfile.open(filename,"w")
    map(archive.add,infiles)
    archive.close()
    Logger.info("Tar file written with %i files and name %s" %(len(infiles),filename))
    return filename

######################################################

def Resubmit(jobnum,errorlist):
    """
    Resubmit the failed jobs of an job-array
    """
    raise NotImplementedError

######################################################

def ResubmitFromDB(jobnum):
    """
    Get jobs from the db which failed and resubmit them
    """
    
    raise NotImplementedError

##################################################

def StandardErrorGrepper(logs,summaryfile=None):
    """
    Search the logfiles for typical error patterns
    :param logs:
    :return:
    """
    errors     = []
    unfinished = []
    mem_errors = []
    non_processed_files = []
    example_message = ""

    def file_len(fname):
        i = 0
        with open(fname) as f:
            for i, l in enumerate(f):
                pass
        return i + 1

    infilesfromlog = []
    for log in logs:
        linecnt = file_len(log)
        if not linecnt:
            Logger.warning("%s has zero lenght" %log.__repr__())
        f   =  open(log)
        txt = f.read()
        try:
            infilestring = txt.split("reader2:  FilenameList =")[1].split("\n")[0].strip("[").strip("]").replace("[","").replace("'","").strip()
            infilesfromlog = infilestring.split(",")
        except:
            infilesfromlog = ""
        #exec("infilesfromlog = " + infilestring)
        if "rror" in txt:
            errors.append(log)
            example_message = txt
            non_processed_files += infilesfromlog
        if "MemoryError" in txt:
            mem_errors.append(log)
            non_processed_files += infilesfromlog
        if not "finish" in txt and (not linecnt ==2):
            unfinished.append(log)
            non_processed_files += infilesfromlog

        f.close()

    if errors or unfinished or mem_errors:
        Logger.info("Errors found, last log \n\n\n %s" %example_message)

    return errors,unfinished,mem_errors,example_message,non_processed_files

##################################################

def FailedBZipTestSearcher(logs,summaryfile="dummy"):

    errors     = []
    unfinished = []
    mem_errors = []
    example_message = ""
    summary = open(summaryfile,"w")


    def file_len(fname):
        i = 0
        with open(fname) as f:
            for i, l in enumerate(f):
                pass
        return i + 1

    for log in logs:
        linecnt = file_len(log)
        if not linecnt:
            Logger.warning("%s has zero length" %log.__repr__())
        f   =  open(log)
        txt = f.read()
        if "ERROR" in txt:
            errors.append(log)
            example_message = txt
            summary.write(txt)
        if "MemoryError" in txt:
            mem_errors.append(log)
        if not "finish" in txt and (not linecnt ==2):
            unfinished.append(log)
        f.close()

    if errors:
        Logger.info("Errors found, last log \n\n\n %s" %example_message)

    summary.close()
    return errors,unfinished,mem_errors,example_message,[]



##################################################

def PostProcess(jobnum,tar=True,grepfunc=StandardErrorGrepper):
    """
    Check the logs if everthing went fine for this job
    tar: tar the logfiles together and delete the untared
    """
    # change to logdir to avoid nested tar-file
    current_dir = os.getcwd()
    
    logdir  = CONFIG.get('logging','farmlogs') 
    os.chdir(logdir)
    logs    = glob("*.o%s.*" %str(jobnum))

    if not logs: 
        tared_logs = glob("*.%i.tar" %jobnum)[0]
        Logger.info("Found a tared log for this job %s" %tared_logs)
        tared_logs = tarfile.TarFile(tared_logs)
        tared_logs.extractall()
        logs    = glob("*.o%s.*" %str(jobnum))

    Logger.info("%i logs found for job %s" %(len(logs),str(jobnum)))
    # use unix grep, as it is faster
    #grepstring_finished = "fgrep finish %s/*.o%s.* | wc -l" %(logdir,str(jobnum))
    #grepstring_errors   = "fgrep rror %s/*.o%s.* | wc -l" %(logdir,str(jobnum))
    #print shlex.split(grepstring_finished)
    #finished = Popen(shlex.split(grepstring_finished),stdout=PIPE).communicate()[0]
    #errors   = Popen(shlex.split(grepstring_errors),stdout=PIPE).communicate()[0]
    #print finished,errors
    # look through the logs
    summary = join(logdir,"job%isummary.log" %jobnum)
    errors,unfinished,mem_errors,example_message,non_processed_files = grepfunc(logs,summaryfile=summary)

    Logger.info("Found %i jobs with errors" %len(errors))
    Logger.info("Found %i unfinished jobs"  %len(unfinished)) 
    Logger.info("Found %i jobs with memory errors"  %len(mem_errors)) 
     
    error_nums      = [int(err.split(".")[-1]) for err in errors]  
    unfinished_nums = [int(un .split(".")[-1]) for un in unfinished] 
    mem_error_nums  = [int(mem.split(".")[-1]) for mem in mem_errors] 
    Logger.info("List of subjobs with errors %s" %error_nums)
    Logger.info("List of subjobs which are not finished %s" %unfinished_nums)
    Logger.info("List of subjobs with memory errors %s" %mem_error_nums)
    if tar:
        tarname = logs[0].split(".")[0] + ".%s" %jobnum.__repr__() + ".tar"
        TarFiles(logs,filename=tarname)
        map(os.remove,logs)
    
    # change back to previous dir    
    os.chdir(current_dir) 
    
    sgejobinfo = GetJobInfoSGEJobs(jobnum)
    mem_exceeded = []
    failed = []
    for thisjob in sgejobinfo:
        try:
            maxmem   = int(thisjob["category"].split(",")[2].split("vmem")[1].rstrip("M"))
            usedmem  = thisjob["maxvmem"]  
            if usedmem > maxmem:
                mem_exceeded.append(int(thisjob["task_number"]))
            if int(thisjob["exit_status"] != 0):
                failed.append(int(thisjob["task_number"]))
        except Exception as e:
            Logger.debug("Exception %s arised during job check for database!" %e.__repr__())    
       
    error_dict = dict()
    error_dict["mem"]   = list(set(mem_error_nums + mem_exceeded))
    error_dict["unfin"] = list(set(unfinished_nums + failed))
    error_dict["error"] = error_nums
    error_dict["nonprfiles"] = non_processed_files
    return error_dict
        


##################################################

def CheckDataSet(dataset,filetype="raw",usefarm=False):
    """
    Check if all files are fine with bzip -t
    """

    assert filetype in ["raw","l3a"], "Can only check for 'raw' or 'l3a' filetype"

    rmtmp = False

    if filetype == "raw":
        files = dataset.raw_files_on_disk

    if filetype == "l3a":
        files = dataset.l3afiles

    if usefarm:
        jobprops = JobProperties()
        jobprops.short_queue()
        thejob = ArrayJobFarmer(CheckBzipIntegrity,1,files,jobprops=jobprops,jobname="ch%i" %dataset.dataset)
        while CheckJobOnFarm(thejob):
            sleep(120)
        result = PostProcess(thejob,grepfunc=FailedBZipTestSearcher)

    else:
        corrupt = 0
        logfile = open("/afs/ifh.de/user/s/stoessl/scratch/" + "bztest%i" %dataset.dataset + ".log","w")
        for i in files:
            if (i.startswith("/acs") or i.startswith("dcap")):
                if i.startswith("dcap"):
                    name = i.replace("dcap://","")

                    command   = "dccp %s /lustre/fs15/group/icecube/stoessl/tmp/" %name
                    Popen(shlex.split(command),stdout=PIPE).communicate()[0]
                    thefile = join("/lustre/fs15/group/icecube/stoessl/tmp/",split(i)[1])
                    print command, thefile
                    time.sleep(1)
                    rmtmp = True
            else:
                thefile = i
                name = thefile
            print thefile
            #raise
            if not CheckBzipIntegrity(thefile):
                logfile.write("BZ2 Error " + i + "\n" )
                corrupt += 1
            if rmtmp:
                command = "rm /lustre/fs15/group/icecube/stoessl/tmp/%s" %split(name)[1]
                Popen(shlex.split(command),stdout=PIPE).communicate()[0]

        logfile.close()
        print len(files),"files"
        print corrupt,"corrupt"
        print "Done"
        


####################################################

def _parse_arcx_sgejobs(sgejobinfo):
    
    all_jobs = []
    cnt = 1
    jobinfo = dict()
    for line in sgejobinfo.split("\n"):
        data = line.split("=")
        data = (data[0].strip(),"".join(data[1:]).strip())
        jobinfo[data[0]] = data[1]
        # each job has 29 fields of inrormation
        cnt += 1
        if cnt == 30:
            all_jobs.append(copy(jobinfo))
            cnt = 1
            jobinfo = dict()
            
    return all_jobs
            
#########################################       

def _parse_jobinfo(qstatjobinfo):
    """
    parse the output of qstat -j
    """
    
    qstat_dict = dict()
    for line in qstatjobinfo.split("\n"):
        if ":" in line:
            data = line.split(":")
            if len(data) != 2:
                continue
            data = (data[0].strip(),data[1].strip())
            qstat_dict.update([data])
            
    return qstat_dict
    
###############################################

def CheckJobOnFarm(jobnum):   
    x = GetJobinfo(jobnum)
    return len(x.keys()) > 0


###############################################

def GetJobinfo(jobnum):
    """
    call qstat -j jobnum and parse the output
    """

    command   = "qstat -ext -j " + str(jobnum)
    qstatinfo = Popen(shlex.split(command),stdout=PIPE).communicate()[0]
    infodict  = _parse_jobinfo(qstatinfo)
    return infodict
    
#################################
    
def GetJobInfoSGEJobs(jobnum):
    
    command    = "arcx sgejobs %s" %str(jobnum)
    sgejobinfo = Popen(shlex.split(command),stdout=PIPE).communicate()[0]
    infos      = _parse_arcx_sgejobs(sgejobinfo)
    return infos
    
#################################

def GetGCD():
    """
    Get some random gcd
    """
    
    raise NotImplementedError
    
    
    
#################################################

class Shepherd(object):
    """
    Watch the sheep lingering on the beautiful farming grounds in Zeuthen,
    but don't fall asleep....
    """

    def __init__(self,fnct,fnct_kwargs,datasetlist,ana_level,maxjobs=20000,dbmanangertype=None,logfile=CONFIG.get("logging","shepherd_log"),port=59322,notify_on_port=None,delay=0):
        self.maxjobs       = maxjobs
        self.datasets      = datasetlist
        self.fnct          = fnct
        self.delay         = delay
        self.fnct_kwargs   = fnct_kwargs
        self.ana_level     = ana_level
        self.notify_on_port= notify_on_port
        self.running_jobs  = []
        self.finished_jobs = []
        self.merge         = 1
        fh = logging.FileHandler(logfile)
        Logger.addHandler(fh)
        Logger.info("Shepherd initialized!")
        Logger.info("Will process the following datasets %s" %self.datasets.__repr__())
        self.dbmanagertype = dbmanangertype
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind("tcp://127.0.0.1:%s" %str(port))

    #def alter_fnct_kwargs(self,fnct_kwargs):
    #    self.fnct_kwargs   = fnct_kwargs

    #def receive_dataset(self):
    #    dataset = self.socket.recv()
    #    dataset = int(dataset)
    #    Logger.info("Got dataset %s" %dataset.__repr__())
    #    return dataset

    def get_active_jobs(self):
        """
        Count the number of active jobs for this user
        """
        c_line       = ['qstat', '-g','d','-u', 'stoessl']
        batch_status = Popen(c_line,stdout=PIPE)
        c_line_wc    = ['wc', '-l']
        counter      = Popen(c_line_wc,stdin=batch_status.stdout,stdout=PIPE)
        batch_status.stdout.close()
        jobsonfarm   = int(counter.communicate()[0])
        batch_status.kill()
        Logger.debug("Got %i jobs currently on the batch system" %jobsonfarm)
        return jobsonfarm


    def submit_dataset(self,dataset,gcd = "/lustre/fs6/group/i3/stoessl/GeoCalibDetectorStatus_IC79.55380_L2a.i3",ram=4000,cpu="23",subjobs=[],jobname="L3afilter"):
        """
        Submit a dataset to the batch system
        """



        if int(dataset) in [6451,6939]:
            infiles = file_db.GetFilesFromDB( dataset=int(dataset),analysis_level=self.ana_level,coincident=1) 
        else:
            infiles = file_db.GetFilesFromDB( dataset=int(dataset),analysis_level=self.ana_level) 
        infiles = [ str(file.filename) for file in infiles]
        if len(infiles) > 10000:
            self.merge=10
        else:
            self.merge=1
        
        Logger.info("Will submit job for dataset %i with %i infiles, will merge %i files" %(int(dataset),len(infiles),int(self.merge)))

        self.fnct_kwargs.update([("merge",int(self.merge))])
        self.fnct_kwargs.update([("dataset",int(dataset))])
        #jobprops = JobPropertiesFactory("long_run_high_mem") 
        jobprops = JobProperties()
        jobprops.request_ram(ram)
        jobprops.specify_hcpu(cpu,"59")
        jobid = 0
        if int(dataset) in [6451,6939]:
        # coincident + polygonato corsika needs double processing
            for __ in range(1):
                jobid = ArrayJobFarmer(self.fnct, int(self.merge), infiles, jobprops, jobname + str(dataset), func_kwargs= self.fnct_kwargs,subjobs=subjobs,delay=self.delay) 
                self.fnct_kwargs.update([('coincident',True)])
                self.running_jobs.append(jobid)
                Logger.info("Submitted job %i" %(int(jobid)))

        else:  
            jobid = ArrayJobFarmer(self.fnct, int(self.merge), infiles, jobprops, jobname + str(dataset), func_kwargs= self.fnct_kwargs,subjobs=subjobs,delay=self.delay) 
            self.running_jobs.append(jobid)
            Logger.info("Submitted job %i" %(int(jobid)))
        
        
        return jobid
    
    def _getAFSToken(self):
        """
        Keep the job alive and get a new afs token,
        so that it can still write data to disk
        """
        Popen(['kinit', '-R'])
    

    #def add_dataset(self,dataset):
    #    """
    #    Add another dataset to the inline queue
    #    """
    #    self.datasets.append(dataset)
        

    def do_farming(self):
        """
        Submit and surveil all the datasets which are in 
        self.datasets
        """
        #dbmanager = DBManager(managertype=self.dbmanagertype)
        Logger.info("Start to process jobs...")
        jobdict = dict()

        if not len(self.datasets):
            raise ValueError("No datasets available for farming!")

        while True:

            time.sleep(30)
            #maxjobsexceeded = False
            # receive new jobs
            #try:
            #    ds = self.receive_dataset()
            #    self.datasets.append(ds)
            #    Logger.info("Received dataset %s to process"%ds.__repr__()) 
            #except Exception as e:
            #    Logger.warning("Caught exception %s" %e.__repr__())
            
            self._getAFSToken()
            
            while len(self.datasets):
                if self.get_active_jobs() > self.maxjobs:
                    #maxjobsexceeded = True
                    break
                
                thisset = self.datasets.pop()
                jobid = self.submit_dataset(thisset)
                jobdict[jobid] = thisset
                self.running_jobs.append(jobid)
                time.sleep(5)
            
            for job in self.running_jobs:
                if not CheckJobOnFarm(job): # assume job is finished
                    errors = dict()
                    try:
                        errors = PostProcess(job)
                    except Exception as e:
                        Logger.warn("Caught Exception %s" %e.__repr__())
                        Logger.warn("Can not postprocess %s" %job.__repr__())
                    newmem = 4000
                    newcpu = "47"
                    if errors:
                        if errors["mem"]:
                            newmem = 10000
                    
                        failed_jobs = errors["mem"] + errors["unfin"] + errors["error"]
                        failed_jobs = list(set(failed_jobs))    
                        if failed_jobs:
                            self.submit_dataset(thisset,ram=newmem, cpu=newcpu, subjobs=failed_jobs)
    
                        else:
                        #    cc = False
                        #    thisset = jobdict[jobid]
                        #    if int(thisset) in [6451,6939]:
                        #        cc = True
                        #    dbmanager.append(thisset,cc)
                            self.finished_jobs.append(job)
                        


                        #sendercontext = zmq.Context()
                        #sendsocket = sendercontext.socket(zmq.REQ)
                        #sendsocket.connect("tcp://127.0.0.1:%s" %str(self.notify_on_port))
                        #sendsocket.send(str(thisset))
                    # ping a another instance here and let it know that we are finished
            
            
            for job in self.finished_jobs:
                if job in self.running_jobs:
                    self.running_jobs.remove(job)
                

    def shutdown(self):
        pass
    
##################################################

class JobDBProxy(object):
    """
    Connect to a job-table and manage datastream
    """
    
    def __init__(self,dbfile=CONFIG.get("database","jobdb")):
        self.dbfile = dbfile
  
    def _re_initialize_tables(self,force=False):
        connection,cursor = OpenSQLiteDB(self.dbfile)
        if not force:
            raise ValueError("Need to force re-initialization!")
        #curs = SQLite()
        try:
            cursor.execute("DROP TABLE JOBS")
        except sqlite3.OperationalError as e:
            Logger.warning("Got sqlite3 error %s" %e.__repr__())
        cursor.execute("CREATE TABLE JOBS ( id INTEGER PRIMARY KEY AUTOINCREMENT,dataset int,jobid int)")  
        CloseSQLiteDB(connection)
        
    def add_job(self,jobid,dataset):
        connection,cursor = OpenSQLiteDB(self.dbfile)
        cursor.execute("INSERT INTO JOBS (dataset,jobid) VALUES (?,?)" %(dataset,jobid))
        CloseSQLiteDB(connection)
        
    def get_job(self,jobid):
        connection,cursor = OpenSQLiteDB(self.dbfile)
        cursor.execute("SELECT * FROM JOBS WHERE jobid=%i" %(jobid))
        data = cursor.fetchall()
        CloseSQLiteDB(connection)
        return data
        
    def get_dataset(self,jobid):
        connection,cursor = OpenSQLiteDB(self.dbfile)
        cursor.execute("SELECT * FROM JOBS WHERE jobid=%i" %(jobid))
        data = cursor.fetchall()
        CloseSQLiteDB(connection)
        return data
        
           
        
    
    
    
    

