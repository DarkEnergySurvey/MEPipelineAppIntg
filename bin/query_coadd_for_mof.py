#! /usr/bin/env python
# $Id: query_coadd_img_for_nullwgt.py 44569 2016-11-08 07:21:46Z rgruendl $
# $Rev:: 44569                            $:  # Revision of last commit.
# $LastChangedBy:: rgruendl               $:  # Author of last commit.
# $LastCha
"""
Query code to obtain images inputs for the COADD/multiepoch pipeline.  
"""

verbose=0

######################################################################################


if __name__ == "__main__":

    import argparse
    import os
    import despydb.desdbi
#    import stat
    import time
    import re
    import sys
    from despymisc.miscutils import fwsplit 
    import intgutils.queryutils as queryutils
    import mepipelineappintg.coadd_query as me
    import mepipelineappintg.mepochmisc as mepochmisc
    import json

    svnid="$Id: query_coadd_img_for_nullwgt.py 44569 2016-11-08 07:21:46Z rgruendl $"

    parser = argparse.ArgumentParser(description='Query code to obtain image inputs for COADD/multiepoch pipelines.')
    parser.add_argument('--me_proctag',  action='store', type=str, required=True, help='Multi-Epoch Processing Tag from which to draw MEDs inputs')
    parser.add_argument('--se_proctag',  action='store', type=str, required=False, help='Single-Epoch Processing Tag from which to draw PSF Model inputs')
    parser.add_argument('-t', '--tile',     action='store', type=str, required=True, help='COADD tile name for which to asssemble inputs')
    parser.add_argument('-o', '--outfile',  action='store', type=str, required=True, help='Output list to be returned for the framework')
    parser.add_argument('--bandlist',   action='store', type=str, default='g,r,i,z,Y', help='Comma separated list of bands to be COADDed (Default="g,r,i,z,Y").')
    parser.add_argument('--archive',  action='store', type=str, default='desar2home', help='Archive site where data are being drawn from')
    parser.add_argument('--meds',     action='store_true', default=False, help='Flag for code to return MEDs files')
    parser.add_argument('--psfmodel', action='store_true', default=False, help='Flag for code to return PSF models associated with MEDs files')
    parser.add_argument('--meds_list',     action='store', default=None, help='Filename with list of returned MEDs files')
    parser.add_argument('--psfmodel_list', action='store', default=None, help='Filename with list returned PSF models associated with MEDs files')
    parser.add_argument('-s', '--section', action='store', type=str, default=None,   help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',  action='store', type=str, default=None,   help='DB schema (do not include \'.\').')
    parser.add_argument('-v', '--verbose', action='store', type=int, default=0, help='Verbosity (defualt:0; currently values up to 4)')
    args = parser.parse_args()
    if args.verbose:
        print "Args: ",args

#
#   Handle simple args (verbose, Schema, magbase, bandlist)
#
    verbose=args.verbose

    if args.Schema is None:
        dbSchema=""
    else:
        dbSchema="%s." % args.Schema

    ArchiveSite=args.archive
    print(" Archive site will be constrained to {:s}".format(ArchiveSite))

#    MagBase=args.magbase
    BandList=fwsplit(args.bandlist)
    print(" Proceeding with BAND constraint to include {:s}-band images".format(','.join([d.strip() for d in BandList])))

    if not args.meds and not args.psfmodel:
        print("Must choose either --meds or --psfmodel")
        exit("Aborting")
    if args.meds and args.psfmodel:
        print("Must choose ONLY ONE of the following --meds or --psfmodel... (i.e. not both)")
        exit("Aborting")

#   Finished rationalizing input
########################################################
#
#   Setup a DB connection
#
    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = despydb.desdbi.DesDbi(desdmfile,args.section,retry=True)

    t0=time.time()
    if args.meds:
        MED_Dict=me.query_meds_psfmodels('meds',args.tile,args.me_proctag,args.se_proctag,BandList,ArchiveSite,dbh,dbSchema,verbose)
        print("    MED Dict size: {:d}".format(len(MED_Dict)))
    if args.psfmodel:
        PSF_Dict=me.query_meds_psfmodels('psfmodel',args.tile,args.me_proctag,args.se_proctag,BandList,ArchiveSite,dbh,dbSchema,verbose)
        print("    PSF Model Dict size: {:d}".format(len(PSF_Dict)))
    print("    Execution Time: {:.2f}".format(time.time()-t0))

    # Write simple lists of returned files
    if args.psfmodel_list and args.psfmodel_list:
        mepochmisc.write_textlist(dbh,PSF_Dict,args.psfmodel_list, fields=['ngmixid','fullname'],verb=args.verbose)
    if args.meds_list and args.meds:
        mepochmisc.write_textlist(dbh,MED_Dict,args.meds_list, fields=['fullname'],verb=args.verbose)
#
#   Close DB connection?
#
    dbh.close()
#
#   Convert the ImgDict to an LLD (list of list of dictionaries)
#   While doing the assembly get a count of number of Imgs per band
#
    OutDict={}
    BandCnt={}
    for band in BandList:
        BandCnt[band]=0

    if args.meds:
        for MED_Img in MED_Dict:
            OutDict[MED_Img]={}
            OutDict[MED_Img]['meds']=MED_Dict[MED_Img]
            BandCnt[MED_Dict[MED_Img]['band']]=BandCnt[MED_Dict[MED_Img]['band']]+1
        filetypes=['meds']
        mdatatypes={'meds':['filename','compression','band']}
    else:
        for PSF_Model in PSF_Dict:
            OutDict[PSF_Model]={}
            OutDict[PSF_Model]['psfmodel']=PSF_Dict[PSF_Model]
            BandCnt[PSF_Dict[PSF_Model]['band']]=BandCnt[PSF_Dict[PSF_Model]['band']]+1
        filetypes=['psfmodel']
        mdatatypes={'psfmodel':['filename','compression','expnum','ccdnum','band']}

    Img_LLD=me.ImgDict_to_LLD(OutDict,filetypes,mdatatypes,verbose)

#
#   If a high level of verbosity is present print the results.
#
    if verbose > 3:
        print("Query results for COADD (SWarp) inputs (LLD format).")
        for Img in Img_LLD:
            print Img
#
#   Here a function call is needed to take the Img_LLD and write results to the output file.
#
    Img_lines=queryutils.convert_multiple_files_to_lines(Img_LLD,filetypes)
    queryutils.output_lines(args.outfile,Img_lines)

#
#   Provide a quick summary of the number of images found for COADD
#
    if verbose > 0:
        print(" ")
        print("Summary results for COADD image imputs")
        for band in BandList:
            if args.meds:
                print("  Identified {:5d} MEDs files for {:s}-band".format(BandCnt[band],band))
            if args.psfmodel:
                print("  Identified {:5d} PSF Model files for {:s}-band".format(BandCnt[band],band))

#
#   Check that all bands that make up the detection image have at least one entry
#
    AllBandsOK=True
    for band in BandList:
        if band not in BandCnt:
            print("ERROR: no images present for {:s}-band (detection band constraint requires at least 1)".format(band))
            AllBandsOK=False
        else:
            if BandCnt[band] < 1:
                print("ERROR: no images present for {:s}-band (detection band constraint requires at least 1)".format(band))
                AllBandsOK=False
#
#   If not all bands are present Abort and throw non-zero exit.
#
    if not AllBandsOK:
        exit("Aborting!")

#   Close up shop. 

    exit()
