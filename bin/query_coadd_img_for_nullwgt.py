#! /usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
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
    
    svnid="$Id$"

    parser = argparse.ArgumentParser(description='Query code to obtain image inputs for COADD/multiepoch pipelines.')
    parser.add_argument('-p', '--proctag',  action='store', type=str, required=True, help='Processing Tag from which to draw COADD inputs')
    parser.add_argument('-t', '--tile',     action='store', type=str, required=True, help='COADD tile name for which to asssemble inputs')
    parser.add_argument('-o', '--outfile',  action='store', type=str, required=True, help='Output list to be returned for the framework')
    parser.add_argument('--zeropoint',  action='store', type=str, default='ZEROPOINT', help='ZEROPOINT table to use in queries. (Default=ZEROPOINT, "NONE" results in all ZP fixed at magbase)')
    parser.add_argument('--zsource',    action='store', type=str, default=None, help='SOURCE constraint on ZEROPOINT table to use in queries. (Default=None)')
    parser.add_argument('--zversion',   action='store', type=str, default=None, help='VERSION constraint on ZEROPOINT table to use in queries. (Default=None)')
    parser.add_argument('--zflag',      action='store', type=str, default=None, help='FLAG constraint on ZEROPOINT table to use in queries. (Default=None)')
    parser.add_argument('--blacklist',  action='store', type=str, default='BLACKLIST', help='BLACKLIST table to use in queries. (Default=BLACKLIST, "NONE", results in no blacklist constraint')
    parser.add_argument('--bandlist',   action='store', type=str, default='g,r,i,z,Y', help='Comma separated list of bands to be COADDed (Default="g,r,i,z,Y").')
    parser.add_argument('--magbase',  action='store', type=float, default=30.0, help='Fiducial/reference magnitude for COADD (default=30.0)')
    parser.add_argument('--archive',  action='store', type=str, default='desar2home', help='Archive site where data are being drawn from')
    parser.add_argument('-s', '--section', action='store', type=str, default=None,   help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',  action='store', type=str, default=None,   help='DB schema (do not include \'.\').')
    parser.add_argument('-v', '--verbose', action='store', type=int, default=0, help='Verbosity (defualt:0; currently values up to 2)')
    args = parser.parse_args()
    if (args.verbose):
        print "Args: ",args

#
#   Handle simple args (verbose, Schema, magbase, bandlist)
#
    verbose=args.verbose

    if (args.Schema is None):
        dbSchema=""
    else:
        dbSchema="%s." % (args.Schema)

    ArchiveSite=args.archive
    print(" Archive site will be constrained to {:s}".format(ArchiveSite))

    MagBase=args.magbase
    BandList=fwsplit(args.bandlist)
    print(" Proceeding with BAND constraint to include {:s}-band images".format(','.join([d.strip() for d in BandList])))

#
#   Specify ZEROPOINT table for use
#
    if (args.zeropoint.upper() == "NONE"):
        ZptInfo=None;
        print(" Proceeding with no ZEROPOINT table specified (mag_zero will be passed as {:8.3f}".format(MagBase))
    else:
        ZptInfo={}
        if (len(args.zeropoint.split('.')) > 1):
            ZptInfo['table']=args.zeropoint
        else:
            ZptInfo['table']='%s%s' % (dbSchema,args.zeropoint)
        print(" Proceeding with constraints using {:s} for ZEROPOINT constraints.".format(ZptInfo['table']))
#
#       Since a zeropoint table is being used... require that SOURCE and VERSION are present
#
        if ((args.zsource is None)or(args.zversion is None)):
            print(" As --zeropoint constraint is active:")
            if (args.zsource is None):
                print("   --zsource {SOURCE} must be provided")
            if (args.zversion is None):
                print("   --zversion {VERSION} must be provided")
            print(" Aborting!")
            exit(1)

#
#   Constraint on ZEROPOINT based on SOURCE
#
    if (args.zsource is not None):
        if (ZptInfo is None):
            print(" No ZEROPOINT table specified. Constaint on {ZEROPOINT}.SOURCE ignored.")
        else:
            ZptInfo['source']=args.zsource
            print("   Adding constraint on ZEROPOINT using SOURCE='{:s}'.".format(ZptInfo['source']))
    else:
        if (ZptInfo is not None):
            print("   Skipping constraint on ZEROPOINT using SOURCE")

#
#   Constraint on ZEROPOINT based on VERSION
#
    if (args.zversion is not None):
        if (ZptInfo is None):
            print(" No ZEROPOINT table specified. Constaint on {ZEROPOINT}.VERSION ignored.")
        else:
            ZptInfo['version']=args.zversion
            print("   Adding constraint on ZEROPOINT using VERSION='{:s}'.".format(ZptInfo['version']))
    else:
        if (ZptInfo is not None):
            print("   Skipping constraint on ZEROPOINT using VERSION")

#
#   Constraint on ZEROPOINT based on FLAGS
#
    if (args.zflag is not None):
        if (ZptInfo is None):
            print(" No ZEROPOINT table specified. Constaint on {ZEROPOINT}.FLAG ignored.")
        else:
            ZptInfo['flag']=args.zflag
            print("   Adding constraint on ZEROPOINT using FLAG='{:s}'.".format(ZptInfo['flag']))
    else:
        if (ZptInfo is not None):
            print("   Skipping constraint on ZEROPOINT using FLAG")

#
#   Specify BLACKLIST table for use
#
    if (args.blacklist.upper() == "NONE"):
        BlacklistInfo=None;
        print(" Proceeding with no BLACKLIST table specified.".format(MagBase))
    else:
        BlacklistInfo={}
        if (len(args.blacklist.split('.')) > 1):
            BlacklistInfo['table']=args.blacklist
        else:
            BlacklistInfo['table']='%s%s' % (dbSchema,args.blacklist)
        print(" Proceeding with constraints using {:s} for BLACKLIST constraint.".format(BlacklistInfo['table']))

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
#    cur = dbh.cursor()

    t0=time.time()
    ImgDict={}
    ImgDict=me.query_coadd_img_by_edges(ImgDict,args.tile,args.proctag,BandList,ArchiveSite,dbh,dbSchema,verbose)
    print "Images Acquired by Query using edges for tile=%s" % (args.tile)
    print "    Execution Time: %.2f" % (time.time()-t0)
    print "    Img Dict size: ",len(ImgDict)

    if (ZptInfo is not None):
        ImgDict=me.query_zeropoint(ImgDict,ZptInfo,dbh,dbSchema,verbose)
        print "ZeroPoint query run " 
        print "    Execution Time: %.2f" % (time.time()-t0)
        print "    Img Dict size: ",len(ImgDict)

    if (BlacklistInfo is not None):
        ImgDict=me.query_blacklist(ImgDict,BlacklistInfo,dbh,dbSchema,verbose)
        print "Blacklist query run " 
        print "    Execution Time: %.2f" % (time.time()-t0)
        print "    Img Dict size: ",len(ImgDict)
#
#   Convert zeropoint (mag_zero) into a fluxscale.
#
    for Img in ImgDict:
        if ('mag_zero' in ImgDict[Img]):
            ImgDict[Img]['fluxscale']=10.**(0.4*(MagBase-ImgDict[Img]['mag_zero']))
        else:
            ImgDict[Img]['mag_zero']=MagBase
            ImgDict[Img]['fluxscale']=1.0


    BkgDict=me.query_bkg_img(ImgDict,ArchiveSite,dbh,dbSchema,verbose)
    print " Bkg image query run"
    print "    Execution Time: %.2f" % (time.time()-t0)
    print "    Bkg Dict size: ",len(BkgDict)

    SegDict=me.query_segmap(ImgDict,ArchiveSite,dbh,dbSchema,verbose)
    print " Segmentation Map query run"
    print "    Execution Time: %.2f" % (time.time()-t0)
    print "    Seg Dict size: ",len(SegDict)

    CatDict=me.query_catfinalcut(ImgDict,ArchiveSite,dbh,dbSchema,verbose)
    print " Catalog query run"
    print "    Execution Time: %.2f" % (time.time()-t0)
    print "    Cat Dict size: ",len(CatDict)
#
#   Close DB connection?
#
    dbh.close()
#
#   If a high level of verbosity is present print the query results.
#
    if (verbose > 2):
        print("Query results for COADD (SWarp) inputs.")
        for Img in ImgDict:
            print(" {:8d} {:2d} {:5s} {:6.3f} {:s}".format(
                ImgDict[Img]['expnum'],
                ImgDict[Img]['ccdnum'],
                ImgDict[Img]['band'],
                ImgDict[Img]['fluxscale'],
                ImgDict[Img]['filename']))

    if (verbose > 0):
        print(" ")
        print("Summary results for COADD image imputs")
        for band in BandList:
            band_cnt=len([ImgDict[Img]['ccdnum'] for Img in ImgDict  if(ImgDict[Img]['band']==band)])
            print("  Identified {:5d} images for {:s}-band".format(band_cnt,band))

#
#   Convert the ImgDict to an LLD (list of list of dictionaries)
#
    OutDict={}
    for Img in ImgDict:
        if ((Img in BkgDict)and(Img in SegDict)and(Img in CatDict)):
            OutDict[Img]={}
            OutDict[Img]['red']=ImgDict[Img]
            OutDict[Img]['bkg']=BkgDict[Img]
            OutDict[Img]['seg']=SegDict[Img]
            OutDict[Img]['cat']=CatDict[Img]

    filetypes=['red','bkg','seg','cat']
    mdatatypes={'red':['filename','compression','expnum','ccdnum','band','mag_zero','fluxscale'],
                'bkg':['filename','compression','expnum','ccdnum','band'],
                'seg':['filename','compression','expnum','ccdnum','band'],
                'cat':['filename','compression','expnum','ccdnum','band','mag_zero']}
    Img_LLD=me.ImgDict_to_LLD(OutDict,filetypes,mdatatypes,verbose)

#
#   If a high level of verbosity is present print the results.
#
    if (verbose > 3):
        print("Query results for COADD (SWarp) inputs (LLD format).")
        for Img in Img_LLD:
            print Img
#
#   Here a function call is needed to take the Img_LLD and write results to the output file.
#
    Img_lines=queryutils.convert_multiple_files_to_lines(Img_LLD,filetypes)
    queryutils.output_lines(args.outfile,Img_lines)

#   Close up shop. 

    exit()
