#! /usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastCha
"""
Query code to obtain catalogs and headfiles for input to the astrometric 
refinement step in the COADD/multiepoch pipeline.  This version is for the
case where it is desired to use the cat_scamp_full and the associated .head 
files for the astrometric refinement.
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
    
    svnid="$Id$"

    parser = argparse.ArgumentParser(description='Query code to obtain inputs for the astrometric refinement step in COADD/multiepoch pipelines.')
    parser.add_argument('--cattype',       action='store', type=str, required=True, help='Type of catalog query (default=CAT_FINALCUT) or alternatively SCAMPCAT scampcat/head')
    parser.add_argument('-p', '--proctag', action='store', type=str, required=True, help='Processing Tag from which to draw COADD inputs')
    parser.add_argument('-t', '--tile',    action='store', type=str, required=True, help='COADD tile name for which to asssemble inputs')
    parser.add_argument('-o', '--outfile', action='store', type=str, required=True, help='Output list to be returned for the framework')
    parser.add_argument('--bandlist',      action='store', type=str, default='g,r,i,z,Y', help='Comma separated list of bands to be COADDed (Default="g,r,i,z,Y").')
    parser.add_argument('-s', '--section', action='store', type=str, default=None,   help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',  action='store', type=str, default=None,   help='DB schema (do not include \'.\').')
    parser.add_argument('-v', '--verbose', action='store', type=int, default=0, help='Verbosity (defualt:0; currently values up to 3)')
    args = parser.parse_args()
    if (args.verbose):
        print "Args: ",args

    verbose=args.verbose

    if (args.Schema is None):
        dbSchema=""
    else:
        dbSchema="%s." % (args.Schema)

    cattype=args.cattype.upper()
    if (cattype not in ['CAT_FINALCUT', 'SCAMPCAT']):
        print "--cattype must be either 'cat_finalcut' or 'scampcat'"
        print "Aborting!!!"
        exit(1)

    BandList=fwsplit(args.bandlist)
    print(" Proceeding with BAND constraint to include {:s}-band images".format(','.join([d.strip() for d in BandList])))

#
#   Setup a DB connection
#
    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = despydb.desdbi.DesDbi(desdmfile,args.section,retry=True)
#    cur = dbh.cursor()

    t0=time.time()
    CatDict={}

    if (cattype == 'SCAMPCAT'):
        CatDict=me.query_astref_scampcat(CatDict,args.tile,args.proctag,dbh,dbSchema,BandList,verbose)
    else:
        CatDict=me.query_astref_catfinalcut(CatDict,args.tile,args.proctag,dbh,dbSchema,BandList,verbose)

    print " "
    print "CATs Acquired by Query using edges for tile=%s" % (args.tile)
    print "    Execution Time: %.2f" % (time.time()-t0)
    print "    CAT Dict size: ",len(CatDict)
#
#   Close DB connection?
#
    dbh.close()
#
#   If a high level of verbosity is present print the results.
#
    if (verbose > 2):
        print " "
#        CatList=[]
#        for Cat in CatDict:
#            CatList.append(Cat)
#        CatList=sorted(CatList)
#        for Cat in CatList:
        for Cat in CatDict:
            if (cattype == 'SCAMPCAT'):
                print(" {:d} {:s} {:s} ".format(
                    CatDict[Cat]['expnum'],
                    CatDict[Cat]['catfile'],
                    CatDict[Cat]['headfile']))
            else:
                print(" {:d} {:2d} {:s}  ".format(
                    CatDict[Cat]['expnum'],
                    CatDict[Cat]['ccdnum'],
                    CatDict[Cat]['catfile']))

#
#   Convert the CatDict to an LLD (list of list of dictionaries)
#
    if (cattype == 'SCAMPCAT'):
        filetypes=['catfile','headfile']
    else:
        filetypes=['catfile']
    mdatatypes=['expnum','band','ccdnum']
    CAT_LLD=me.CatDict_to_LLD(CatDict,filetypes,mdatatypes,verbose)
#
#   Here a function call is needed to take the CAT_LLD and write results to the output file.
#
    CAT_lines=queryutils.convert_multiple_files_to_lines(CAT_LLD,filetypes)
    queryutils.output_lines(args.outfile,CAT_lines)

#   Close up shop. 

    exit(0)
