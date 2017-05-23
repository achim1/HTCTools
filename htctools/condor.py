
import os
import os.path

def create_empty_props():
    """
    Set up a dictionary with default fields for the
    job

    Returns:
        dict
    """

    props = dict()
    props["executable"] = ""
    props["logfile"] = "/dev/null"
    props["errorlog"] = "/dev/null"
    props["args"] = []
    props["n_jobs"] = []
    props["id"] = 1
    return props


def write_submit_script(props, submit_dir=""):
    """
    Write a simple submit script for the HTCondor scheduler

    Args:
        props (dict): A dictionary with the job argurments and constraints

    Keyword Args:
        submit_dir (str): the directory for storing the jobs

    Returns:
        None
    """

    arguments = " ".join(props["args"])
    submit_script  = "executable  = {}\n".format(props["executable"])
    submit_script += "arguments   = {}\n".format(arguments)
    submit_script += "log         = {}\n".format(props["logfile"])
    submit_script += "error       = {}\n".format(props["errorlog"])
    submit_script += "getenv      = True\n"
    submit_script += "queue {}\n".format(props["n_jobs"])
    seed = ""
    try:
        seed = int(props["seed"])
        seed = "_" + str(seed)
    except KeyError:
        pass

    submitscriptname = os.path.join(submit_dir, "job{}.sub".format(props["id"]))
    if os.path.exists(submitscriptname):
        cnt = 1
        while os.path.exists(submitscriptname):
            submitscriptname = os.path.join(submit_dir,"job{}.sub".format(props["id"] + cnt))
            cnt += 1

    with open(submitscriptname, "w") as sub:
        sub.write(submit_script)

    sub.close()
    return None
