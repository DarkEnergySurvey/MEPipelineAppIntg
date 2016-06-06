#! /usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastCha
"""
A small set of tests that exercise a set of queries that nominally provide 
inputs for the COADD pipeline

"""

verbose=0

######################################################################################

if __name__ == "__main__":

    import argparse
    import os
    import despydb.desdbi
    import stat
    import time
    import re
    import sys
    import mepipelineappintg.coadd_query as me
    
    svnid="$Id$"

    parser = argparse.ArgumentParser(description='Small scale tests of COADD pipeline queries')
    parser.add_argument('-p', '--proctag',  action='store', type=str, required=True, help='Processing Tag from which to draw COADD inputs')
    parser.add_argument('-t', '--tile',     action='store', type=str, required=True, help='COADD tile name for which to asssemble inputs')
    parser.add_argument('--catquery',    action='store_true', default=False, help='Exercise Astrorefine input CAT query')
    parser.add_argument('--edgequery',   action='store_true', default=False, help='Exercise Edge-based IMG query')
    parser.add_argument('--extentquery', action='store_true', default=False, help='Exercise Extent-based IMG query')
    parser.add_argument('--compIMG',     action='store_true', default=False, help='Compare results from edge and extent IMG queries')
    parser.add_argument('--zeropoint',   action='store', type=str, default='ZEROPOINT', help='ZEROPOINT table to use in queries. (Default=ZEROPOINT, "NONE" results in all ZP fixed at magbase)')
    parser.add_argument('--zsource',     action='store', type=str, default=None, help='SOURCE constraint on ZEROPOINT table to use in queries. (Default=None)')
    parser.add_argument('--zversion',    action='store', type=str, default=None, help='VERSION constraint on ZEROPOINT table to use in queries. (Default=None)')
    parser.add_argument('--zflag',       action='store', type=str, default=None, help='FLAG constraint on ZEROPOINT table to use in queries. (Default=None)')
    parser.add_argument('--blacklist',   action='store', type=str, default='BLACKLIST', help='BLACKLIST table to use in queries. (Default=BLACKLIST, "NONE", results in no blacklist constraint')
    parser.add_argument('--magbase',     action='store', type=float, default=30.0, help='Fiducial/reference magnitude for COADD (default=30.0)')

    parser.add_argument('-s', '--section', action='store', type=str, default=None,   help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',  action='store', type=str, default=None,   help='DB schema (do not include \'.\').')
    parser.add_argument('-v', '--verbose', action='store', type=int, default=0, help='Verbosity (defualt:0; currently values up to 2)')
    args = parser.parse_args()
    if (args.verbose):
        print "Args: ",args

    verbose=args.verbose

    if (args.Schema is None):
        dbSchema=""
    else:
        dbSchema="%s." % (args.Schema)
#
#   If no specific test is specified then test all.
#
    TestAll=False
    if ((not(args.extentquery))and(not(args.edgequery))and(not(args.catquery))):
        TestAll=True

    MagBase=args.magbase

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

#
#   Setup a DB connection
#
    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = despydb.desdbi.DesDbi(desdmfile,args.section)
    cur = dbh.cursor()

    if (args.tile == "TESTBED"):
        tilelist=['DES0311-504','DES0309-5205', 'DES0307-5040', 'DES0306-5123','DES2359+0001','DES0001-4957','DES2357-5040']
    else:
        tilelist=[]
        tilelist.append(args.tile)

####################
    for tile in tilelist:
 
        if ((args.extentquery)or(TestAll)):
            t0=time.time()
            MeImgDict={}
            MeImgDict=me.query_coadd_img_by_extent(MeImgDict,tile,args.proctag,cur,dbSchema,verbose)
            print "Img Acquired by Query using extents for tile=%s" % (tile)
            print "    Execution Time: %.2f" % (time.time()-t0)
            print "    ImgDict (by extent) size: ",len(MeImgDict)

        if ((args.catquery)or(TestAll)):
            t0=time.time()
            CatDict={}
            CatDict=me.query_astref_scampcat(CatDict,tile,args.proctag,cur,dbSchema,verbose)
            print "CAT Acquired by Query using edges for tile=%s" % (tile)
            print "    Execution Time: %.2f" % (time.time()-t0)
            print "    CAT Dict size: ",len(CatDict)
#
            if (verbose > 1):
                for Cat in CatDict:
                    print(" {:d} {:s} {:s} ".format(
                        CatDict[Cat]['expnum'],
                        CatDict[Cat]['catfile'],
                        CatDict[Cat]['headfile']))

#
#           Test conversion of CatDict to an LLD (list of list of dictionaries)
#
            filetypes=['catfile','headfile']
            mdatatypes=['expnum','band']
            CAT_LLD=me.CatDict_to_LLD(CatDict,filetypes,mdatatypes,verbose)
 
            if (verbose > 1):
                for sublist in CAT_LLD:
                    print sublist


        if ((args.edgequery)or(TestAll)):
            t0=time.time()
            ImgDict={}
            ImgDict=me.query_coadd_img_by_edges(ImgDict,tile,args.proctag,ZptInfo,BlacklistInfo,cur,dbSchema,verbose)
            print "Img Acquired by Query using edges for tile=%s" % (tile)
            print "    Execution Time: %.2f" % (time.time()-t0)
            print "    ImgDict size: ",len(ImgDict)
#
#           Convert zeropoint (mag_zero) into a fluxscale.
#
            for Img in ImgDict:
                if ('mag_zero' in ImgDict[Img]):
                    ImgDict[Img]['fluxscale']=10.**(0.4*(MagBase-ImgDict[Img]['mag_zero']))
                else:
                    ImgDict[Img]['fluxscale']=1.0


        if ((args.compIMG)or(TestAll)):
            for Img in ImgDict:
                if (Img in MeImgDict):
                    MeMatch="Matched"
                else:
                    MeMatch="Missing"

                print(" {:s} {:s} ".format(MeMatch,ImgDict[Img]['filename']))

            for Img in MeImgDict:
                if (Img not in ImgDict):
                    print(" Extra?  {:s}".format(Img))


    dbh.close()
