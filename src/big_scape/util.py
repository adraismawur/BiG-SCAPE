"""Module containing utilities relating to common BiG-SCAPE functionality

Specifically, this contains a number of functions relevant in result generation

Authors: Jorge Navarro, Arjan Draisma
"""
import os
import json
import shutil

from src.legacy.bgctools import sort_bgc
from src.js import add_to_bigscape_results_js
from src.utility import create_directory

def fetch_genome_list(run, input_clusters_idx, cluster_names, mibig_set, bgc_info, gen_bank_dict):
    """Retrieves the genome list from a given set of BGCs"""
    genomes = []
    classes = []
    cluster_names_to_genomes = {}
    cluster_names_to_classes = {}
    for idx, bgc in enumerate(cluster_names):
        if bgc in mibig_set:
            continue
        input_clusters_idx.append(idx)
        # get class info
        product = bgc_info[bgc]["product"]
        predicted_class = sort_bgc(product)
        if predicted_class not in classes:
            cluster_names_to_classes[bgc] = len(classes)
            classes.append(predicted_class)
        else:
            cluster_names_to_classes[bgc] = classes.index(predicted_class)
        # get identifier info
        identifier = ""
        if len(bgc_info[bgc]["organism"]) > 1:
            identifier = bgc_info[bgc]["organism"]
        else: # use original genome file name (i.e. exclude "..clusterXXX from antiSMASH run")
            file_name_base = os.path.splitext(os.path.basename(gen_bank_dict[bgc][0]))[0]
            identifier = file_name_base.rsplit(".cluster", 1)[0].rsplit(".region", 1)[0]
        if len(identifier) < 1:
            identifier = "Unknown Genome {}".format(len(genomes))
        if identifier not in genomes:
            cluster_names_to_genomes[bgc] = len(genomes)
            genomes.append(identifier)
        else:
            cluster_names_to_genomes[bgc] = genomes.index(identifier)
    # TODO: simplify list comprehension
    run.run_data["input"]["accession"] = [{"id": "genome_{}".format(i),
                                           "label": acc
                                          } for i, acc in enumerate(genomes)]

    run.run_data["input"]["accession_newick"] = [] # todo ...
    # TODO: simplify list comprehension
    run.run_data["input"]["classes"] = [{"label": cl} for cl in classes] # todo : colors
    # TODO: simplify list comprehension
    run.run_data["input"]["bgc"] = [{"id": cluster_names[idx],
                                     "acc": cluster_names_to_genomes[cluster_names[idx]],
                                     "class": cluster_names_to_classes[cluster_names[idx]]
                                    } for idx in input_clusters_idx]

def update_family_data(rundata_networks_per_run, input_clusters_idx, cluster_names, mibig_set):
    """Updates family information on the generated network"""
    for network_key in rundata_networks_per_run:
        for network in rundata_networks_per_run[network_key]:
            for family in network["families"]:
                new_members = []
                mibig = []
                for bgc_idx in family["members"]:
                    if bgc_idx in input_clusters_idx:
                        new_members.append(input_clusters_idx.index(bgc_idx))
                    else: # is a mibig bgc
                        cluster_name = cluster_names[bgc_idx]
                        if cluster_name in mibig_set:
                            mibig.append(cluster_name)
                family["mibig"] = mibig
                family["members"] = new_members


def generate_results_per_cutoff_value(run, rundata_networks_per_run, html_subs_per_run):
    """Generates a results javascript file per cutoff for use in the results
    view webpage
    """
    for cutoff in run.cluster.cutoff_list:
        # update overview.html
        cutoff_html_folder = "{}_c{:.2f}".format(run.directories.network_html, cutoff)
        cutoff_run_data = run.run_data.copy()
        cutoff_run_data["networks"] = rundata_networks_per_run[cutoff_html_folder]
        with open(os.path.join(cutoff_html_folder, "run_data.js"), "w") as run_data_js:
            json_string = json.dumps(cutoff_run_data, indent=4, separators=(',', ':'),
                                     sort_keys=True)
            run_data_js.write("var run_data={};\n".format(json_string))
            run_data_js.write("dataLoaded();\n")
        # update bgc_results.js
        run_string = "{}_c{:.2f}".format(run.run_name, cutoff)
        results_path = os.path.join(run.directories.output, "html_content", "js",
                                    "bigscape_results.js")
        add_to_bigscape_results_js(run_string, html_subs_per_run[cutoff_html_folder],
                                   results_path)

def copy_template_per_cutoff(run, root_path):
    """Copies the html template per given cutoff for the results folder"""
    template_path = os.path.join(root_path, "html_template", "overview_html")
    for cutoff in run.cluster.cutoff_list:
        network_html_folder_cutoff = "{}_c{:.2f}".format(run.directories.network_html, cutoff)
        create_directory(network_html_folder_cutoff, "Network HTML Files", False)
        shutil.copy(template_path, os.path.join(network_html_folder_cutoff, "overview.html"))

def prepare_cutoff_rundata_networks(run):
    """Prepares a data structure for each cutoff in the networks per run
    variable
    """
    rundata_networks_per_run = {}
    for cutoff in run.cluster.cutoff_list:
        network_html_folder_cutoff = "{}_c{:.2f}".format(run.directories.network_html, cutoff)
        rundata_networks_per_run[network_html_folder_cutoff] = []
    return rundata_networks_per_run

def prepare_html_subs_per_run(run):
    """Prepares a data structure for each cutoff in the html subs per run
    variable
    """
    html_subs_per_run = {}
    for cutoff in run.cluster.cutoff_list:
        network_html_folder_cutoff = "{}_c{:.2f}".format(run.directories.network_html, cutoff)
        html_subs_per_run[network_html_folder_cutoff] = []
    return html_subs_per_run

def write_network_annotation_file(run, bgc_collection):
    """Writes the network annotations to a tsv file"""
    network_annotation_path = os.path.join(run.directories.network, "Network_Annotations_Full.tsv")
    with open(network_annotation_path, "w") as network_annotation_file:
        header = "BGC\tAccession ID\tDescription\tProduct Prediction\tBiG-SCAPE class\tOrganism\t\
            Taxonomy\n"
        network_annotation_file.write(header)
        for bgc in bgc_collection.bgc_name_tuple:
            bgc_info = bgc_collection.bgc_collection_dict[bgc].bgc_info
            product = bgc_info.product
            bgc_info_parts = [bgc, bgc_info.accession_id, bgc_info.description,
                              product, sort_bgc(product), bgc_info.organism,
                              bgc_info.taxonomy]
            network_annotation_file.write("\t".join(bgc_info_parts) + "\n")
