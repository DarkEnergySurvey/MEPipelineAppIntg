#! /usr/bin/env python3
# $Id: query_coadd_meds_standalone.py 46438 2018-01-04 20:38:17Z rgruendl $
# $Rev:: 46438                            $:  # Revision of last commit.
# $LastChangedBy:: rgruendl               $:  # Author of last commit.
# $LastCha
"""
Query code to obtain inputs for standalone MEDS generation.
Operates on the assumption that a previous execution of the multiepoch/COADD pipeline
has already executed.
"""

verbose = 0

######################################################################################

def main():

    import argparse
    import os
    import despydb.desdbi
    import time
    from despymisc.miscutils import fwsplit
    import intgutils.queryutils as queryutils
    import mepipelineappintg.coadd_query as me
    import mepipelineappintg.mepochmisc as mepochmisc

    svnid = "$Id: query_coadd_meds_standalone.py 46438 2018-01-04 20:38:17Z rgruendl $"

    parser = argparse.ArgumentParser(description='Query code to obtain inputs for standalone MEDS generation.')

    parser.add_argument('--me_proctag', action='store', type=str, required=True,
                        help='Multi-Epoch Processing Tag from which to draw MEDs inputs')
    #    parser.add_argument('--se_proctag',  action='store', type=str, required=False, help='Single-Epoch Processing Tag from which to draw PSF Model inputs')
    parser.add_argument('-t', '--tile', action='store', type=str, required=True,
                        help='COADD tile name for which to asssemble inputs')
    parser.add_argument('-o', '--outfile', action='store', type=str, required=True,
                        help='Output list to be returned for the framework')
    parser.add_argument('--zeropoint', action='store', type=str, default='ZEROPOINT',
                        help='ZEROPOINT table to use in queries. (Default=ZEROPOINT, "NONE" results in all ZP fixed at magbase)')
    parser.add_argument('--zsource', action='store', type=str, default=None,
                        help='SOURCE constraint on ZEROPOINT table to use in queries. (Default=None)')
    parser.add_argument('--zversion', action='store', type=str, default=None,
                        help='VERSION constraint on ZEROPOINT table to use in queries. (Default=None)')
    parser.add_argument('--zflag', action='store', type=str, default=None,
                        help='FLAG constraint on ZEROPOINT table to use in queries. (Default=None)')
    #    parser.add_argument('--blacklist',  action='store', type=str, default='BLACKLIST', help='BLACKLIST table to use in queries. (Default=BLACKLIST, "NONE", results in no blacklist constraint')
    parser.add_argument('--bandlist', action='store', type=str, default='g,r,i,z,Y',
                        help='Comma separated list of bands to be COADDed (Default="g,r,i,z,Y").')
    parser.add_argument('--detbands', action='store', type=str, default='r,i,z',
                        help='Comma separated list of bands that must have at least one image present (Default="r,i,z").')
    parser.add_argument('--magbase', action='store', type=float, default=30.0,
                        help='Fiducial/reference magnitude for COADD (default=30.0)')
    parser.add_argument('--zpt2', action='store', type=str, default=None,
                        help='ZEROPOINT table to use secondary ZPT queries. (Default=None)')
    parser.add_argument('--z2source', action='store', type=str, default=None,
                        help='SOURCE constraint on secondary ZPT queries. (Default=None)')
    parser.add_argument('--z2version', action='store', type=str, default=None,
                        help='VERSION constraint on secondary ZPT queries. (Default=None)')
    parser.add_argument('--z2flag', action='store', type=str, default=None,
                        help='FLAG constraint on secondary ZPT queries. (Default=None)')
    parser.add_argument('--archive', action='store', type=str, default='desar2home',
                        help='Archive site where data are being drawn from')
    parser.add_argument('--imglist', action='store', type=str, default=None,
                        help='Optional output of a txt-file listing showing expnum, ccdnum, band, zeropoint')
    parser.add_argument('--ima_list', action='store', default=None,
                        help='Filename with list of returned IMG list')
    parser.add_argument('--bkg_list', action='store', default=None,
                        help='Filename with list of returned BKG list')
    parser.add_argument('-s', '--section', action='store', type=str, default=None,
                        help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema', action='store', type=str, default=None,
                        help='DB schema (do not include \'.\').')
    parser.add_argument('-v', '--verbose', action='store', type=int, default=0,
                        help='Verbosity (defualt:0; currently values up to 4)')
    args = parser.parse_args()
    if args.verbose:
        print("Args: ", args)

    #
    #   Handle simple args (verbose, Schema, magbase, bandlist)
    #
    verbose = args.verbose

    if args.Schema is None:
        dbSchema = ""
    else:
        dbSchema = f"{args.Schema}."

    ArchiveSite = args.archive
    print(f" Archive site will be constrained to {ArchiveSite:s}")

    MagBase = args.magbase
    BandList = fwsplit(args.bandlist)
    print(f" Proceeding with BAND constraint to include {','.join([d.strip() for d in BandList]):s}-band images")

    if args.detbands.upper() == "NONE":
        DetBandList = []
        print(" Proceeding WITHOUT constraint that detection bands must have at least one image")
    else:
        DetBandList = fwsplit(args.detbands)
        print(f" Proceeding with constraint that all detection bands ({','.join([d.strip() for d in DetBandList]):s}) must have at least one image")

    #
    #   Specify ZEROPOINT table for use
    #
    if args.zeropoint.upper() == "NONE":
        ZptInfo = None
        print(f" Proceeding with no ZEROPOINT table specified (mag_zero will be passed as {MagBase:8.3f}")
    else:
        ZptInfo = {}
        if len(args.zeropoint.split('.')) > 1:
            ZptInfo['table'] = args.zeropoint
        else:
            ZptInfo['table'] = f'{dbSchema}{args.zeropoint}'
        print(f" Proceeding with constraints using {ZptInfo['table']:s} for ZEROPOINT constraints.")
        #
        #       Since a zeropoint table is being used... require that SOURCE and VERSION are present
        #
        if args.zsource is None or args.zversion is None:
            print(" As --zeropoint constraint is active:")
            if args.zsource is None:
                print("   --zsource {SOURCE} must be provided")
            if args.zversion is None:
                print("   --zversion {VERSION} must be provided")
            print(" Aborting!")
            exit(1)

    #
    #   Constraint on ZEROPOINT based on SOURCE
    #
    if args.zsource is not None:
        if ZptInfo is None:
            print(" No ZEROPOINT table specified. Constraint on {ZEROPOINT}.SOURCE ignored.")
        else:
            ZptInfo['source'] = args.zsource
            print(f"   Adding constraint on ZEROPOINT using SOURCE='{ZptInfo['source']:s}'.")
    else:
        if ZptInfo is not None:
            print("   Skipping constraint on ZEROPOINT using SOURCE")

    #
    #   Constraint on ZEROPOINT based on VERSION
    #
    if args.zversion is not None:
        if ZptInfo is None:
            print(" No ZEROPOINT table specified. Constraint on {ZEROPOINT}.VERSION ignored.")
        else:
            ZptInfo['version'] = args.zversion
            print(f"   Adding constraint on ZEROPOINT using VERSION='{ZptInfo['version']:s}'.")
    else:
        if ZptInfo is not None:
            print("   Skipping constraint on ZEROPOINT using VERSION")

    #
    #   Constraint on ZEROPOINT based on FLAGS
    #
    if args.zflag is not None:
        if ZptInfo is None:
            print(" No ZEROPOINT table specified. Constraint on {ZEROPOINT}.FLAG ignored.")
        else:
            ZptInfo['flag'] = args.zflag
            print(f"   Adding constraint on ZEROPOINT using FLAG<{ZptInfo['flag']:s}.")
    else:
        if ZptInfo is not None:
            print("   Skipping constraint on ZEROPOINT using FLAG")

    #
    #   Secondary ZEROPOINT capablity/constraint
    #
    if args.zpt2 is None:
        ZptSecondary = None
        print(" No secondary ZPT query requested.")
    else:
        ZptSecondary = {}
        if len(args.zpt2.split('.')) > 1:
            ZptSecondary['table'] = args.zpt2
        else:
            ZptSecondary['table'] = f'{dbSchema}{args.zpt2}'
        print(f" Proceeding with constraints for secondary  using {ZptInfo['table']:s} for ZEROPOINT constraints.")
        #
        #       Since a zeropoint table is being used... require that SOURCE and VERSION are present
        #
        if args.z2source is None or args.z2version is None:
            print(" As --zpt2 constraint is active:")
            if args.z2source is None:
                print("   --z2source {SOURCE} must be provided")
            if args.z2version is None:
                print("   --z2version {VERSION} must be provided")
            print(" Aborting!")
            exit(1)
        #
        #       Constraint on secondary ZPT based on SOURCE
        #
        if args.z2source is not None:
            ZptSecondary['source'] = args.z2source
            print(f"   Adding constraint on Secondary ZPT using SOURCE='{ZptSecondary['source']:s}'.")
        #
        #       Constraint on secondary ZPT based on VERSION
        #
        if args.z2version is not None:
            ZptSecondary['version'] = args.z2version
            print(f"   Adding constraint on Secondary ZPT using VERSION='{ZptSecondary['version']:s}'.")
        #
        #       Constraint on secondary ZPT based on FLAGS
        #
        if args.zflag is not None:
            ZptSecondary['flag'] = args.z2flag
            print(f"   Adding constraint on Secondary ZPT using FLAG<{ZptSecondary['flag']:s}.")
    ##
    ##   Specify BLACKLIST table for use
    ##
    #    if (args.blacklist.upper() == "NONE"):
    #        BlacklistInfo=None;
    #        print(" Proceeding with no BLACKLIST table specified.".format(MagBase))
    #    else:
    #        BlacklistInfo={}
    #        if (len(args.blacklist.split('.')) > 1):
    #            BlacklistInfo['table']=args.blacklist
    #        else:
    #            BlacklistInfo['table']='%s%s' % (dbSchema,args.blacklist)
    #        print(" Proceeding with constraints using {:s} for BLACKLIST constraint.".format(BlacklistInfo['table']))

    ##
    ##   Specify Fiat TABLE (table that declares image to tile correspondence).
    ##
    #    if (len(args.fiat_table.split('.')) > 1):
    #        FiatTable=args.fiat_table
    #    else:
    #        FiatTable='%s%s' % (dbSchema,args.fiat_table)
    #    if (not(args.brute_force)):
    #        print(" Proceeding with constraints using {:s} to tie Image to Tiles.".format(FiatTable))
    #    else:
    #        print(" Will perform a brute force query to tie Image to Tiles.")

    #   Finished rationalizing input
    ########################################################
    #
    #   Setup a DB connection
    #
    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = despydb.desdbi.DesDbi(desdmfile, args.section, retry=True)
    #    cur = dbh.cursor()

    t0 = time.time()
    #
    #   Obtain the attempt ID for a given COADD tile from specific PROCTAG
    #
    attemptID = mepochmisc.find_tile_attempt(args.tile, args.me_proctag, dbh, dbSchema,
                                             Timing=True, verbose=verbose)
    if attemptID is None:
        print(f"Failed to identify an attempt for TILE={args.tile:s} for PROCTAG={args.me_proctag:s}.")
        print("Aborting")
        exit(1)
    else:
        if verbose > 0:
            print(f"Identified attempt as PFW_ATTEMPT_ID={attemptID:d}")
    #
    #   Now obtain the input images and their associated .head files by matching to the nullwgt images from the COADD attempt
    #
    print(f"Acquiring single-epoch images based on previous attempt for tile={args.tile:s}")
    ImgDict = {}
    ImgDict = me.query_coadd_img_from_attempt(ImgDict, attemptID, BandList, ArchiveSite, dbh,
                                              dbSchema, verbose)
    print(f"    Execution Time: {time.time() - t0:.2f}")
    print(f"    Img Dict size: {len(ImgDict):d}")

    ImgDict = me.query_zeropoint(ImgDict, ZptInfo, ZptSecondary, dbh, dbSchema, verbose)
    print("ZeroPoint query run ")
    print(f"    Execution Time: {time.time() - t0:.2f}")
    print(f"    Img Dict size: {len(ImgDict):d}")

    #    if (BlacklistInfo is not None):
    #        ImgDict=me.query_blacklist(ImgDict,BlacklistInfo,dbh,dbSchema,verbose)
    #        print "Blacklist query run "
    #        print "    Execution Time: %.2f" % (time.time()-t0)
    #        print "    Img Dict size: ",len(ImgDict)

    #
    #   Convert zeropoint (mag_zero) into a fluxscale.
    #
    for Img in ImgDict:
        if 'mag_zero' in ImgDict[Img]:
            ImgDict[Img]['fluxscale'] = 10. ** (0.4 * (MagBase - ImgDict[Img]['mag_zero']))
        else:
            ImgDict[Img]['mag_zero'] = MagBase
            ImgDict[Img]['fluxscale'] = 1.0

    HeadDict = me.query_headfile_from_attempt(ImgDict, attemptID, ArchiveSite, dbh, dbSchema, verbose)
    print(" Head file query run")
    print(f"    Execution Time: {time.time() - t0:.2f}")
    print(f"    Head Dict size: {len(HeadDict):d} ")

    BkgDict = me.query_bkg_img(ImgDict, ArchiveSite, dbh, dbSchema, verbose)
    print(" Bkg image query run")
    print(f"    Execution Time: {time.time() - t0:.2f}")
    print(f"    Bkg Dict size: {len(BkgDict):d}")

    SegDict = me.query_segmap(ImgDict, ArchiveSite, dbh, dbSchema, verbose)
    print(" Segmentation Map query run")
    print(f"    Execution Time: {time.time() - t0:.2f}")
    print(f"    Seg Dict size: {len(SegDict):d}")

    #
    #   Close DB connection?
    #
    #    dbh.close()
    #
    #   If a high level of verbosity is present print the query results.
    #
    if verbose > 2:
        print("Query results for COADD (SWarp) inputs, prior to mixing among catalogs")
        for Img in ImgDict:
            print(f" {ImgDict[Img]['expnum']:8d} {ImgDict[Img]['ccdnum']:2d} {ImgDict[Img]['band']:5s} {ImgDict[Img]['fluxscale']:6.3f} {ImgDict[Img]['filename']:s}")

    #
    #   Convert the ImgDict to an LLD (list of list of dictionaries)
    #   While doing the assembly get a count of number of Imgs per band
    #
    OutDict = {}
    BandCnt = {}
    for band in BandList:
        BandCnt[band] = 0
    for Img in ImgDict:
        if Img in BkgDict and Img in SegDict and Img in HeadDict:
            OutDict[Img] = {'red': ImgDict[Img],
                            'bkg': BkgDict[Img],
                            'seg': SegDict[Img],
                            'head': HeadDict[Img]}
            BandCnt[ImgDict[Img]['band']] = BandCnt[ImgDict[Img]['band']] + 1

    filetypes = ['red', 'bkg', 'seg', 'head']
    mdatatypes = {'red':['filename', 'compression', 'expnum', 'ccdnum', 'band', 'mag_zero', 'fluxscale'],
                  'bkg':['filename', 'compression', 'expnum', 'ccdnum', 'band'],
                  'seg':['filename', 'compression', 'expnum', 'ccdnum', 'band'],
                  'head':['filename', 'compression', 'expnum', 'ccdnum', 'band']}
    Img_LLD = me.ImgDict_to_LLD(OutDict, filetypes, mdatatypes, verbose)

    #
    #   If a high level of verbosity is present print the results.
    #
    if verbose > 3:
        print("Query results for COADD (SWarp) inputs (LLD format).")
        for Img in Img_LLD:
            print(Img)
    #
    #   Here a function call is needed to take the Img_LLD and write results to the output file.
    #
    Img_lines = queryutils.convert_multiple_files_to_lines(Img_LLD, filetypes)
    queryutils.output_lines(args.outfile, Img_lines)

    #
    #   Provide a quick summary of the number of images found for COADD
    #
    if verbose > 0:
        print(" ")
        print("Summary results for COADD image imputs")
        for band in BandList:
            print(f"  Identified {BandCnt[band]:5d} images for {band:s}-band")
    #
    #   Secondary (optional) output of a list of images found by the query.
    #
    if args.imglist is not None:
        imgfile = open(args.imglist, 'w')
        for Img in ImgDict:
            wrec = False
            if Img in BkgDict and Img in SegDict and Img in HeadDict:
                wrec = True
            if wrec:
                imgfile.write(f" {ImgDict[Img]['expnum']:8d} {ImgDict[Img]['ccdnum']:2d} {ImgDict[Img]['band']:5s} {ImgDict[Img]['mag_zero']:8.5f}\n")
        imgfile.close()

    #
    #   Check that all bands that make up the detection image have at least one entry
    #
    DetBandsOK = True
    for band in DetBandList:
        if band not in BandCnt:
            print(f"ERROR: no images present for {band:s}-band (detection band constraint requires at least 1)")
            DetBandsOK = False
        else:
            if BandCnt[band] < 1:
                print(f"ERROR: no images present for {band:s}-band (detection band constraint requires at least 1)")
                DetBandsOK = False
    #
    #   If not all bands are present Abort and throw non-zero exit.
    #
    if not DetBandsOK:
        print("Aborting!")
        exit(1)

    #   Close up shop.


    # Optional print a list of the location of the inputs
    if args.ima_list:
        mepochmisc.write_textlist(dbh, ImgDict, args.ima_list, fields=['fullname', 'band', 'mag_zero'], verb=args.verbose)

    if args.bkg_list:
        mepochmisc.write_textlist(dbh, BkgDict, args.bkg_list, fields=['fullname', 'band'], verb=args.verbose)

    exit()


if __name__ == "__main__":
    main()
