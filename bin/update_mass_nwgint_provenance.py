#!/usr/bin/env python3
# $Id: update_mass_nwgint_provenance.py v1.0 2023-10-04 16:26:23Z rgruendl $
"""
Code to add provenance information to OPM_WAS_DERIVED_FROM when pipelines 
use mass_coadd_nwgint (rather than coadd_nwgint)
"""

import argparse
import time
import os
import re
import sys
import despydb.desdbi

#import yaml
##from pixcorrect.PixCorrectDriver import filelist_to_list
#import subprocess

#import matplotlib.path
#import fitsio
#import numpy as np


######################################################################################
def get_file_ids(FileDict,ProvDict,Pkey,dbh,dbSchema="",AreCompressed=False,verbose=0):
    """ Query code to obtain image IDs.

        Use an existing DB connection to execute a query for desfile info 
        (desfile.ID and desfile.WGB_TASK_ID) in preparation for updating provenance.

        Inputs:
            FileDict:  Dict with filenames and link/key back to ProvDict
            ProvDict:  Dict with data organized for downstream use (and where query results will land)
            PKey:      File type within ProvDict where output lands
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            ProvDict:   Returns ProvDict with updates
    """
#
#   Obtain desfile.IDs and wgb_task_id
#
    tmp_fname=[]
    for fname in FileDict:
        tmp_fname.append([fname])
#    print(tmp_fname[0])
    curDB = dbh.cursor()
    curDB.execute('delete from GTT_FILENAME')
    print(f"# Loading GTT_FILENAME table with {Pkey:s} filenames for to get IDs with {len(tmp_fname):d} images")
    dbh.insert_many('GTT_FILENAME', ['FILENAME'], tmp_fname)

    if (AreCompressed):
        query = f"""SELECT d.filename, d.id, d.wgb_task_id FROM {dbSchema:s}desfile d, gtt_filename g WHERE d.filename=g.filename and d.compression is not null"""
    else:
        query = f"""SELECT d.filename, d.id, d.wgb_task_id FROM {dbSchema:s}desfile d, gtt_filename g WHERE d.filename=g.filename and d.compression is null"""

    if verbose > 0:
        print("# Executing query")
        if verbose == 1:
            print(f"# sql = " + ' '.join([d.strip() for d in query.split('\n')]))
        if verbose > 1:
            print(f"# sql = {query:s}")
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

#
#   Move results into ProvDict
#
    cnt=0
    wcnt=0
    for row in curDB:
        rowd = dict(zip(desc, row))
        fname = rowd['filename']
        if (fname in FileDict):
            expccd=FileDict[fname]
            ProvDict[expccd][Pkey]['id']=rowd['id']
            ProvDict[expccd][Pkey]['wgb_task_id']=rowd['wgb_task_id']
            cnt=cnt+1
        else:
            print("Warning: {:} returned but has no corresponding entry in ProvDict".format(rowd))
            wcnt=wcnt+1

    print("Completed update for filetype/key: {:s}.  Added {:d} entries.  Problematic entries {:d}".format(Pkey,cnt,wcnt))

    return ProvDict


######################################################################################
def get_used_recs(ProvDict,dbh,dbSchema,verbose=0):
    """ Query code to obtain image IDs.

        Use an existing DB connection to execute a query for desfile info 
        (desfile.ID and desfile.WGB_TASK_ID) in preparation for updating provenance.

        Inputs:
            ProvDict:  Dict with data organized for downstream use:
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            UsedList:   Returns Dict of task_id and parent desfile_ids

        On a side note…   
        A prior variant of the COADD pipeline would create a massive set of not so accurate/helpful provenance (due to a “feature” in the processing framework)
        This can be O(500k records) per tile in OPM_USED.  If run 1000's of times --> O(100M GARBAGE records)
        The current version of pipeline has a work-around... but in case it needs to change back... this routine is here to help find them..

    """
#
#   Obtain desfile.IDs and wgb_task_id
#
    tmp_id=[]
    for key in ProvDict:
        tmp_id.append([ProvDict[key]['out']['id']])
    curDB = dbh.cursor()
    curDB.execute('delete from GTT_ID')
    print(f"# Loading GTT_ID table with coadd_nwgint(output) filenames for to get IDs with {len(tmp_id):d} images")
    dbh.insert_many('GTT_ID', ['ID'], tmp_id)

    query = f"""SELECT u.task_id,u.desfile_id FROM {dbSchema:s}desfile d, gtt_id g, {dbSchema:s}opm_used u, {dbSchema:s}desfile d2 WHERE d.id=g.id and d.wgb_task_id=u.task_id and u.desfile_id=d2.id and (d2.filetype='red_immask' or d2.filetype='coadd_head_scamp') """

    if verbose > 0:
        print("# Executing query to obtain red_immask images (based on their use in a previous multiepoch attempt)")
        if verbose == 1:
            print(f"# sql = " + ' '.join([d.strip() for d in query.split('\n')]))
        if verbose > 1:
            print(f"# sql = {query:s}")
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

#
#   Form list of entries from OPM_USED that should be removed.
#
    UsedList=[]
    for row in curDB:
        rowd = dict(zip(desc, row))
        UsedList.append([rowd['task_id'],rowd['desfile_id']])

    print("Found {:d} records that should be removed".format(len(UsedList)))

    return UsedList


######################################################################################
if (__name__ == "__main__"):

    parser = argparse.ArgumentParser(description="Application to \"fix\" provenance that is Define list-based inputs to run (serially) many executions of coadd_nwgint")

    parser.add_argument('--imglist', action='store', type=str, default=None, required=True,
                        help='List of image files to be processed')
    parser.add_argument('--headlist', action='store', type=str, default=None, required=False,
                        help='List of header files to be processed (optional)')
    parser.add_argument('--outlist', action='store', type=str, default=None, required=False,
                        help='List of resulting output image files')
    parser.add_argument('-u', '--updateDB',  action='store_true', default=False, 
                        help='Allow insert/commit of new data to DB.')

    # Option to implement complex criteria for mask bits (and weights) to be set
#   parser.add_argument('--var_badpix', action='store', type=str, default=None, required=False,
#                       help='YAML description of alternate criteria to enable mask bits')

#    parser.add_argument('--zpcol', action='store', type=int, default=None, required=False,
#                       help='Column in imglist that contain MAG_ZERO (to be placed in output header).')

#    parser.add_argument('-A', '--pfw_attempt_id', action='store', type=str, default=None, required=False,
#                       help='Processing attempt used to discover inputs.')

    parser.add_argument('-s', '--section', action='store', type=str, default=None,
                        help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema', action='store', type=str, default=None,
                        help='DB schema (do not include \'.\').')
    parser.add_argument('-v', '--verbose', action='store', type=int, default=0,
                        help='Verbosity (defualt:0; currently values up to 4)')

#    Not sure that this needs pass through args....
#    args,unknown_args = parser.parse_known_args()

    args = parser.parse_args()
    if (args.verbose > 0):
        print("Args: {:}".format(args))

#
#   Handle simple args (verbose, Schema, PFW_ATTEMPT_ID)
#
    verbose = args.verbose

    if args.Schema is None:
        dbSchema = ""
    else:
        dbSchema = f"{args.Schema}."

########################################################
#
#   Setup a DB connection
#
    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = despydb.desdbi.DesDbi(desdmfile, args.section, retry=True)

#
#   Get list of input images.
#
#   This is a hacked version of file_to_list (that assigns extra columns to a dictionary)
#   Necessary because args.imglist is now also providing extra information.
#
    ProvDict = {}
    ImgDict={}
    try:
        fimg = open(args.imglist, 'r')
    except:
        raise IOError("File not found.  Missing input list {:s} ".format(args.imglist))
        exit(1)

    for line in fimg:
        line = line.strip()
        columns = line.split()
        # just the filename
        fnamepath = columns[0].split("/")
        fname=fnamepath[-1]
        if ((fname[-3:] == ".gz")or(fname[-3:] == ".fz")):
            fname=fname[:-3]
#            print("Compressed ",fname)
        expccd='{:d}_c{:02d}'.format(int(columns[1]),int(columns[2]))
        ProvDict[expccd]={}
        ProvDict[expccd]['img']={}
        ProvDict[expccd]['expnum']=int(columns[1])
        ProvDict[expccd]['ccdnum']=int(columns[2])
        ProvDict[expccd]['img']['fname']=fname
        ImgDict[fname]=expccd
    fimg.close()

#
#   Optional read a list of WCS headers (assumed to be ordered same as other lists)
#
    HeadDict={}
    if (args.headlist is None):
        print('No list of header files give... will not update WCS')
        HeadDict=None
    else:
        try:
            fhead = open(args.headlist, 'r')
        except:
            print('Failure reading headlist file names from {:s}'.format(args.headlist))
            exit(1)

        for line in fhead:
            line = line.strip()
            columns = line.split()
            fnamepath = columns[0].split("/")
            fname=fnamepath[-1]
            expccd='{:d}_c{:02d}'.format(int(columns[1]),int(columns[2]))
            if (expccd in ProvDict):
                ProvDict[expccd]['head']={}
                ProvDict[expccd]['head']['fname']=fname
                HeadDict[fname]=expccd
            else:
                print("Warning: No entry for {:s} in provenance dictionary (no input image)".format(expccd))
        fhead.close()

#
#   Get list of output images.
#
    try:
        fout = open(args.outlist, 'r')
    except:
        print('Failure reading outlist file names from {:s}'.format(args.outlist))
        exit(1)

    OutDict={}
    for line in fout:
        line = line.strip()
        columns = line.split()
        fnamepath = columns[0].split("/")
        fname=fnamepath[-1]
        expccd='{:d}_c{:02d}'.format(int(columns[1]),int(columns[2]))
        if (expccd in ProvDict):
            ProvDict[expccd]['out']={}
            ProvDict[expccd]['out']['fname']=fname
            OutDict[fname]=expccd
        else:
            print("Warning: No entry for {:s} in provenance dictionary (no input image)".format(expccd))
    fout.close()

#
#   Query DB to pull DESFILE info needed for provenance
#
    ProvDict=get_file_ids(OutDict,ProvDict,'out',dbh,dbSchema,verbose=verbose)
    ProvDict=get_file_ids(ImgDict,ProvDict,'img',dbh,dbSchema,AreCompressed=True,verbose=verbose)
    if (HeadDict is not None):
        ProvDict=get_file_ids(HeadDict,ProvDict,'head',dbh,dbSchema,verbose=verbose)
#
#   Remove misleading provenance
#
#   RAG notes this is currently fixed by not declaring images as used when running...
#
#    UsedList=get_used_recs(ProvDict,dbh,dbSchema,verbose=verbose)
#    print(len(UsedList))

#
#   Add OPM_WAS_DERIVED_FROM_ENTRIES
#
    New_WDF=[] 
    for expccd in ProvDict:
        PRec=ProvDict[expccd]
        New_WDF.append([PRec['out']['id'],PRec['img']['id']])
        if (verbose > 2):
            print(" {:d} <-- {:d}   ({:s} <- {:s}) ".format(PRec['out']['id'],PRec['img']['id'],PRec['out']['fname'],PRec['img']['fname']))
        if ('head' in PRec):
            New_WDF.append([PRec['out']['id'],PRec['head']['id']])
            if (verbose > 2):
                print(" {:d} <-- {:d}   ({:s} <- {:s}) ".format(PRec['out']['id'],PRec['head']['id'],PRec['out']['fname'],PRec['head']['fname']))

    if (verbose > 0):
        print("Preparing to update OPM_WAS_DERIVED_FROM with {:d} records.".format(len(New_WDF)))
    if (args.updateDB):
        dbh.insert_many('OPM_WAS_DERIVED_FROM', ['CHILD_DESFILE_ID','PARENT_DESFILE_ID'], New_WDF)
        dbh.commit()
        if (verbose > 0):
            print("Insert/commit complete.")
    else:
        print("Warning: --updateDB must be chose for insert/commit to occur.")

    dbh.close()

    exit(0)

 
