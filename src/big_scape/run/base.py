"""Module containing classes to keep track of current BiG-SCAPE run details

Author: Arjan Draisma
"""
import logging
import sys
import os
import time

from src.big_scape.run.dir_param import DirParam
from src.big_scape.run.gbk_param import GbkParam
from src.big_scape.run.mibig_param import MibigParam
from src.big_scape.run.pfam_param import PfamParam
from src.big_scape.run.distance_param import DistParam
from src.big_scape.run.cluster_param import ClusterParam
from src.big_scape.run.network_param import NetworkParam

class Run:
    """Class to keep track of important run-specific details, based on given options
    """
    # TODO: reduce instance attributes to <8
    ## subsections

    # options used to generate this run
    options: object

    # files
    directories: DirParam
    gbk: GbkParam

    # mibig
    mibig: MibigParam

    # pfam
    pfam: PfamParam

    # distance calculation
    distance: DistParam

    # clustering
    cluster: ClusterParam

    # networking
    network: NetworkParam

    ## other run parameters
    # run mode
    run_mode: str

    # domain include list
    has_includelist: bool
    domain_includelist: set

    # valid/banned classes
    valid_classes: set
    user_banned_classes: set

    # for logging
    start_time: time.struct_time
    run_name: str
    run_label: str
    run_data: dict

    def set_run_mode(self, options):
        """Parses and sets the run mode of this run from options

        Inputs:
        - options: options object from CMD_parser"""
        run_mode_string = ""
        networks_folder_all = "networks_all"
        if options.hybrids:
            networks_folder_all += "_hybrids"
            run_mode_string += "_hybrids"
        if options.mode == "auto":
            networks_folder_all += "_auto"
            run_mode_string += "_auto"
        elif options.mode == "glocal":
            networks_folder_all += "_glocal"
            run_mode_string += "_glocal"
        else:
            run_mode_string += "_global"

        self.run_mode = run_mode_string

    def set_domain_includelist(self, options):
        """Sets flag to use includelist to true if it is present in options

        Inputs:
        - options: options object from CMD_parser"""
        self.has_includelist = False
        if options.domain_includelist:
            bigscape_path = os.path.dirname(os.path.realpath(__file__))
            if os.path.isfile(os.path.join(bigscape_path, "domain_includelist.txt")):
                self.domain_includelist = set()
                for line in open(os.path.join(bigscape_path, "domain_includelist.txt"), "r"):
                    if line[0] == "#":
                        continue
                    self.domain_includelist.add(line.split("\t")[0].strip())
                if len(self.domain_includelist) == 0:
                    logging.warning("--domain_includelist used, but no domains found in the file")
                else:
                    self.has_includelist = True
            else:
                logging.error("domain_includelist.txt file not found")
                sys.exit(1)

    def set_valid_classes(self, options):
        #define which classes will be analyzed (if in the options_classify mode)
        self.valid_classes = set()
        for key in self.distance.bgc_class_weight:
            self.valid_classes.add(key.lower())
        self.user_banned_classes = set([a.strip().lower() for a in options.banned_classes])
        self.valid_classes = self.valid_classes - self.user_banned_classes


    def init(self, options):
        self.options = options

        self.directories = DirParam(options)
        self.gbk = GbkParam(options)

        self.mibig = MibigParam(options)
        self.pfam = PfamParam(options)
        self.distance = DistParam(options)
        self.cluster = ClusterParam(options)
        self.network = NetworkParam(options)

        self.set_run_mode(options)

        self.set_domain_includelist(options)

        self.set_valid_classes(options)

    def start(self, skip_dir=False):
        """Start the run: set a run name and record the start time

        Inputs:
        - options: options object from CMD_parser"""
        # start time
        self.start_time = time.time()

        localtime = time.localtime(self.start_time)
        # generate run name
        self.run_name = "{}{}".format(time.strftime("%Y-%m-%d_%H-%M-%S", localtime), self.run_mode)

        if self.options.label:
            self.run_name = self.run_name + "_" + self.options.label

        # record run data
        # TODO: find out whether this is needed in this way
        self.run_data = {}
        self.run_data["start_time"] = time.strftime("%d/%m/%Y %H:%M:%S", localtime)
        self.run_data["parameters"] = " ".join(sys.argv[1:])
        self.run_data["input"] = {}

        if skip_dir:
            return

        self.directories.set_run_dependent_dir(self.run_name)
        self.directories.prepare_run_dependent_dir()

    def end(self):
        end_time = time.time()
        duration = end_time - self.start_time
        self.run_data["end_time"] = time.strftime("%d/%m/%Y %H:%M:%S", time.localtime(end_time))
        hours = (duration // 3600)
        minutes = ((duration % 3600) // 60)
        seconds = ((duration % 3600) % 60)
        self.run_data["duration"] = "{}h{}m{}s".format(hours, minutes, seconds)

    def report_runtime(self):
        runtime = time.time()-self.start_time
        runtime_string = "Main function took {:.3f} s".format(runtime)
        with open(os.path.join(self.directories.log, "runtimes.txt"), 'a') as timings_file:
            timings_file.write(runtime_string + "\n")

        # print runtime
        logging.info(runtime_string)
