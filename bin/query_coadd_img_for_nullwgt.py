#! /usr/bin/env python3
# $Id: query_coadd_img_for_nullwgt.py 48356 2019-03-07 16:26:23Z rgruendl $
# $Rev:: 48356                            $:  # Revision of last commit.
# $LastChangedBy:: rgruendl               $:  # Author of last commit.
# $LastCha
"""
Query code to obtain images inputs for the COADD/multiepoch pipeline.
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

    svnid = "$Id: query_coadd_img_for_nullwgt.py 48356 2019-03-07 16:26:23Z rgruendl $"

    parser = argparse.ArgumentParser(description='Query code to obtain image inputs for COADD/multiepoch pipelines.')
    parser.add_argument('-p', '--proctag', action='store', type=str, required=True,
                        help='Processing Tag from which to draw COADD inputs')
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
    parser.add_argument('--blacklist', action='store', type=str, default='BLACKLIST',
                        help='BLACKLIST table to use in queries. (Default=BLACKLIST, "NONE", results in no blacklist constraint')
    parser.add_argument('--bandlist', action='store', type=str, default='g,r,i,z,Y',
                        help='Comma separated list of bands to be COADDed (Default="g,r,i,z,Y").')
    parser.add_argument('--detbands', action='store', type=str, default='r,i,z',
                        help='Comma separated list of bands that must have at least one image present (Default="r,i,z").')
    parser.add_argument('--fiat_table', action='store', type=str, default='Y3A1_IMAGE_TO_TILE',
                        help='Optional table that contains a direct correspondence between image (FILENAME) and tile (TILENAME). (Default=Y3A1_IMAGE_TO_TILE)')
    parser.add_argument('--brute_force', action='store_true', default=False, help='Redirects query to obtain images by making a brute force comparison between IMAGE table and COADDTILE_GEOM (Default=False)')
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
    parser.add_argument('--no_MEDs', action='store_true', default=False,
                        help='Suppress inclusion of BKGD, SEGMAP, PSF model  products')
    parser.add_argument('--imglist', action='store', type=str, default=None,
                        help='Optional output of a txt-file listing showing expnum, ccdnum, band, zeropoint')
    parser.add_argument('--ima_list', action='store', default=None,
                        help='Filename for optional list of returned RED_IMMASK images')
    parser.add_argument('--seg_list', action='store', default=None,
                        help='Filename for optional list of returned SEGMAP images')
    parser.add_argument('--bkg_list', action='store', default=None,
                        help='Filename for optional list of returned BKG images')
    parser.add_argument('--psf_list', action='store', default=None,
                        help='Filename for optional list of returned PSF models')
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
        dbSchema =f"{args.Schema}."

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
    #
    #   Specify BLACKLIST table for use
    #
    if args.blacklist.upper() == "NONE":
        BlacklistInfo = None
        print(" Proceeding with no BLACKLIST table specified.")
    else:
        BlacklistInfo = {}
        if len(args.blacklist.split('.')) > 1:
            BlacklistInfo['table'] = args.blacklist
        else:
            BlacklistInfo['table'] = f'{dbSchema}{args.blacklist}'
        print(f" Proceeding with constraints using {BlacklistInfo['table']:s} for BLACKLIST constraint.")

    #
    #   Specify Fiat TABLE (table that declares image to tile correspondence).
    #
    if len(args.fiat_table.split('.')) > 1:
        FiatTable = args.fiat_table
    else:
        FiatTable = f'{dbSchema}{args.fiat_table}'
    if not args.brute_force:
        print(f" Proceeding with constraints using {FiatTable:s} to tie Image to Tiles.")
    else:
        print(" Will perform a brute force query to tie Image to Tiles.")

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
    ImgDict = {}
    if args.brute_force:
        print(f"Images Acquired by Brute Force Query using edges for tile={args.tile}")
        ImgDict = me.query_coadd_img_by_edges(ImgDict, args.tile, args.proctag, BandList, ArchiveSite,
                                              dbh, dbSchema, verbose)
    else:
        print(f"Images Acquired by Pre-computed relationship between Images and Tile for tile={args.tile}")
        ImgDict = me.query_coadd_img_by_fiat(ImgDict, args.tile, args.proctag, BandList, ArchiveSite,
                                             FiatTable, dbh, dbSchema, verbose)

    print(f"    Execution Time: {time.time() - t0:.2f}")
    print("    Img Dict size: ", len(ImgDict))

    if ZptInfo is not None:
        ImgDict = me.query_zeropoint(ImgDict, ZptInfo, ZptSecondary, dbh, dbSchema, verbose)
        print("ZeroPoint query run ")
        print(f"    Execution Time: {time.time() - t0:.2f}")
        print("    Img Dict size: ", len(ImgDict))

    if BlacklistInfo is not None:
        ImgDict = me.query_blacklist(ImgDict, BlacklistInfo, dbh, dbSchema, verbose)
        print("Blacklist query run ")
        print(f"    Execution Time: {time.time() - t0:.2f}")
        print("    Img Dict size: ", len(ImgDict))
    #
    #   Convert zeropoint (mag_zero) into a fluxscale.
    #
    for Img in ImgDict:
        if 'mag_zero' in ImgDict[Img]:
            ImgDict[Img]['fluxscale'] = 10. ** (0.4 * (MagBase - ImgDict[Img]['mag_zero']))
        else:
            ImgDict[Img]['mag_zero'] = MagBase
            ImgDict[Img]['fluxscale'] = 1.0

    if not args.no_MEDs:
        BkgDict = me.query_bkg_img(ImgDict, ArchiveSite, dbh, dbSchema, verbose)
        print(" Bkg image query run")
        print(f"    Execution Time: {time.time() - t0:.2f}")
        print("    Bkg Dict size: ", len(BkgDict))

    #if not args.no_MEDs:
        SegDict = me.query_segmap(ImgDict, ArchiveSite, dbh, dbSchema, verbose)
        print(" Segmentation Map query run")
        print(f"    Execution Time: {time.time() - t0:.2f}")
        print("    Seg Dict size: ", len(SegDict))

    #if  not args.no_MEDs:
        PsfDict = me.query_psfmodel(ImgDict, ArchiveSite, dbh, dbSchema, verbose)
        print(" Segmentation Map query run")
        print(f"    Execution Time: {time.time() - t0:.2f}")
        print("    PSF Dict size: ", len(PsfDict))


    CatDict = me.query_catfinalcut(ImgDict, ArchiveSite, dbh, dbSchema, verbose)
    print(" Catalog query run")
    print(f"    Execution Time: {time.time() - t0:.2f}")
    print("    Cat Dict size: ", len(CatDict))


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
        if args.no_MEDs:
            if Img in CatDict:
                OutDict[Img] = {'red': ImgDict[Img],
                                'cat': CatDict[Img]}
                BandCnt[ImgDict[Img]['band']] = BandCnt[ImgDict[Img]['band']] + 1
        else:
            if Img in BkgDict and Img in SegDict and Img in CatDict and Img in PsfDict:
                OutDict[Img] = {'red': ImgDict[Img],
                                'bkg': BkgDict[Img],
                                'seg': SegDict[Img],
                                'psf': PsfDict[Img],
                                'cat': CatDict[Img]}
                BandCnt[ImgDict[Img]['band']] = BandCnt[ImgDict[Img]['band']] + 1

    if args.no_MEDs:
        filetypes = ['red', 'cat']
        mdatatypes = {'red': ['filename', 'compression', 'expnum', 'ccdnum', 'band', 'mag_zero', 'fluxscale'],
                      'cat': ['filename', 'compression', 'expnum', 'ccdnum', 'band', 'mag_zero']}
    else:
        filetypes = ['red', 'bkg', 'seg', 'psf', 'cat']
        mdatatypes = {'red':['filename', 'compression', 'expnum', 'ccdnum', 'band', 'mag_zero', 'fluxscale'],
                      'bkg':['filename', 'compression', 'expnum', 'ccdnum', 'band'],
                      'seg':['filename', 'compression', 'expnum', 'ccdnum', 'band'],
                      'psf':['filename', 'compression', 'expnum', 'ccdnum', 'band'],
                      'cat':['filename', 'compression', 'expnum', 'ccdnum', 'band', 'mag_zero']}
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
            if args.no_MEDs:
                if Img in CatDict:
                    wrec = True
            else:
                if Img in BkgDict and Img in SegDict and Img in CatDict:
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
        mepochmisc.write_textlist(dbh, ImgDict, args.ima_list, fields=['fullname', 'band', 'mag_zero'],
                                  verb=args.verbose)

    if args.bkg_list:
        if not args.no_MEDs:
            mepochmisc.write_textlist(dbh, BkgDict, args.bkg_list, fields=['fullname', 'band'],
                                      verb=args.verbose)
        else:
            print(f"Option --no_MEDs precludes search for BKG images.  Skipping write for --bkg_list {args.bkg_list:s}")

    if args.seg_list:
        if not args.no_MEDs:
            mepochmisc.write_textlist(dbh, SegDict, args.seg_list, fields=['fullname', 'band'],
                                      verb=args.verbose)
        else:
            print(f"Option --no_MEDs precludes search for SEGMAP images.  Skipping write for --seg_list {args.seg_list:s}")

    if args.psf_list:
        if not args.no_MEDs:
            mepochmisc.write_textlist(dbh, PsfDict, args.psf_list, fields=['fullname', 'band'],
                                      verb=args.verbose)
        else:
            print(f"Option --no_MEDs precludes search for PSF models.  Skipping write for --psf_list {args.psf_list:s}")

    exit()


if __name__ == "__main__":
    main()
