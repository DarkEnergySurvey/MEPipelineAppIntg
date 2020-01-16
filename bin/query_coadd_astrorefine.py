#! /usr/bin/env python3
# $Id: query_coadd_astrorefine.py 43836 2016-08-25 20:15:59Z rgruendl $
# $Rev:: 43836                            $:  # Revision of last commit.
# $LastChangedBy:: rgruendl               $:  # Author of last commit.
# $LastCha
"""
    Query code to obtain catalogs and headfiles for input to the astrometric
    refinement step in the COADD/multiepoch pipeline.  This version is for the
    case where it is desired to use the cat_scamp_full and the associated .head
    files for the astrometric refinement.
"""

verbose = 0

######################################################################################

if __name__ == "__main__":

    import argparse
    import os
    import despydb.desdbi
    import time
    from despymisc.miscutils import fwsplit
    import intgutils.queryutils as queryutils
    import mepipelineappintg.coadd_query as me

    svnid = "$Id: query_coadd_astrorefine.py 43836 2016-08-25 20:15:59Z rgruendl $"

    parser = argparse.ArgumentParser(description='Query code to obtain inputs for the astrometric refinement step in COADD/multiepoch pipelines.')
    parser.add_argument('--cattype', action='store', type=str, required=True,
                        help='Type of catalog query (default=CAT_FINALCUT) or alternatively SCAMPCAT scampcat/head')
    parser.add_argument('-p', '--proctag', action='store', type=str, required=True,
                        help='Processing Tag from which to draw COADD inputs')
    parser.add_argument('-t', '--tile', action='store', type=str, required=True,
                        help='COADD tile name for which to asssemble inputs')
    parser.add_argument('-o', '--outfile', action='store', type=str, required=True,
                        help='Output list to be returned for the framework')
    parser.add_argument('--bandlist', action='store', type=str, default='g,r,i,z,Y',
                        help='Comma separated list of bands to be COADDed (Default="g,r,i,z,Y").')
    parser.add_argument('--fiat_table', action='store', type=str, default='Y3A1_IMAGE_TO_TILE',
                        help='Optional table that contains a direct correspondence between image (FILENAME) and tile (TILENAME). (Default=Y3A1_IMAGE_TO_TILE)')
    parser.add_argument('--brute_force', action='store_true', default=False,
                        help='Redirects query to obtain images by making a brute force comparison between IMAGE table and COADDTILE_GEOM (Default=False)')
    parser.add_argument('-s', '--section', action='store', type=str, default=None,
                        help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema', action='store', type=str, default=None,
                        help='DB schema (do not include \'.\').')
    parser.add_argument('-v', '--verbose', action='store', type=int, default=0,
                        help='Verbosity (defualt:0; currently values up to 3)')
    args = parser.parse_args()
    if args.verbose:
        print("Args: ", args)

    verbose = args.verbose

    if args.Schema is None:
        dbSchema = ""
    else:
        dbSchema = f"{args.Schema}."

    cattype = args.cattype.upper()
    if cattype not in ['CAT_FINALCUT', 'SCAMPCAT']:
        print("--cattype must be either 'cat_finalcut' or 'scampcat'")
        print("Aborting!!!")
        exit(1)

    BandList = fwsplit(args.bandlist)
    print(f" Proceeding with BAND constraint to include {','.join([d.strip() for d in BandList]):s}-band images")

    #
    #   Specify Fiat TABLE (table that declares image to tile correspondence).
    #
    if len(args.fiat_table.split('.')) > 1:
        FiatTable = args.fiat_table
    else:
        FiatTable = f'{dbSchema}{args.fiat_table}'
    if not args.brute_force:
        print(f" Proceeding with constraints using {FiatTable:s} to tie Images (and hence catalogs) to Tiles.")
    else:
        print(" Will perform a brute force query to tie Catalogs to Tiles.")


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
    CatDict = {}

    if cattype == 'SCAMPCAT':
        if args.brute_force:
            CatDict = me.query_astref_scampcat(CatDict, args.tile, args.proctag, dbh, dbSchema,
                                               BandList, verbose)
        else:
            CatDict = me.query_astref_scampcat_by_fiat(CatDict, args.tile, args.proctag, dbh,
                                                       dbSchema, BandList, FiatTable, verbose)
    else:
        if args.brute_force:
            CatDict = me.query_astref_catfinalcut(CatDict, args.tile, args.proctag, dbh, dbSchema,
                                                  BandList, verbose)
        else:
            CatDict = me.query_astref_catfinalcut_by_fiat(CatDict, args.tile, args.proctag, dbh,
                                                          dbSchema, BandList, FiatTable, verbose)

    print(" ")
    print(f"CATs Acquired by Query using edges for tile={args.tile}")
    print(f"    Execution Time: {time.time() - t0:.2f}")
    print("    CAT Dict size: ", len(CatDict))
    #
    #   Close DB connection?
    #
    dbh.close()
    #
    #   If a high level of verbosity is present print the results.
    #
    if verbose > 2:
        print(" ")
        #        CatList=[]
        #        for Cat in CatDict:
        #            CatList.append(Cat)
        #        CatList=sorted(CatList)
        #        for Cat in CatList:
        for Cat in CatDict:
            if cattype == 'SCAMPCAT':
                print(f" {CatDict[Cat]['expnum']:d} {CatDict[Cat]['catfile']:s} {CatDict[Cat]['headfile']:s} ")
            else:
                print(f" {CatDict[Cat]['expnum']:d} {CatDict[Cat]['ccdnum']:2d} {CatDict[Cat]['catfile']:s}  ")

    #
    #   Convert the CatDict to an LLD (list of list of dictionaries)
    #
    if cattype == 'SCAMPCAT':
        filetypes = ['catfile', 'headfile']
    else:
        filetypes = ['catfile']
    mdatatypes = ['expnum', 'band', 'ccdnum']
    CAT_LLD = me.CatDict_to_LLD(CatDict, filetypes, mdatatypes, verbose)
    #
    #   Here a function call is needed to take the CAT_LLD and write results to the output file.
    #
    CAT_lines = queryutils.convert_multiple_files_to_lines(CAT_LLD, filetypes)
    queryutils.output_lines(args.outfile, CAT_lines)

    #   Close up shop.

    exit(0)
