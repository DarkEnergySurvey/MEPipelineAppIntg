#! /usr/bin/env python3

"""
Query GAIA_DR2 to obtain stars on (or near) a coadd tile
"""

verbose = 0

######################################################################################

if __name__ == "__main__":

    import argparse
    import os
    import despydb.desdbi
#    import time
#    import yaml
    import mepipelineappintg.cat_query as cq
    import fitsio


    parser = argparse.ArgumentParser(description='Query code to obtain image inputs for COADD/multiepoch pipelines.')
    parser.add_argument('--tilename', action='store', type=str, default=None,
                        help='Tilename of interest')
    parser.add_argument('--extend', action='store', type=float, default=0.0,
                        help='Amount to extend tile boundary (default=0.0).  Units depend on --method. Negative values will shrink but are not strictly controlled.')
    parser.add_argument('--method', action='store', type=str, default='fixed',
                        help='Method to used with --extend. Either "fractional" (expand by a factor) or "fixed" (default) number of arcminutes')
    parser.add_argument('-o', '--output', action='store', type=str, required=True,
                        help='Output FITS table to be written')
    parser.add_argument('-s', '--section', action='store', type=str, default='db-dessci',
                        help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema', action='store', type=str, default='des_admin',
                        help='DB schema (do not include \'.\').')
    parser.add_argument('-v', '--verbose', action='store', type=int, default=0,
                        help='Verbosity (defualt:0; currently values up to 2)')
    args = parser.parse_args()
    if args.verbose:
        print("Args: ", args)

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
    #    cur = dbh.cursor()

    tileDict=cq.query_Tile_edges(args.tilename,dbh,dbSchema,verbose=args.verbose)

    if (args.extend != 0.0):
        tileDict=cq.expand_range(tileDict,extend=args.extend,method=args.method,verbose=args.verbose)

#
#   Since I apparently am  not consistent crowbar the structure for bounds of a tile to that used in 
#   bounding an RA/DEC search.
#
    radec_box={}
    for tile in tileDict:
        if (tileDict[tile]['crossra0']=="Y"):
            radec_box['crossra0']=True
        else:
            radec_box['crossra0']=False
        radec_box['ra1']=tileDict[tile]['racmin']
        radec_box['ra2']=tileDict[tile]['racmax']
        radec_box['dec1']=tileDict[tile]['deccmin']
        radec_box['dec2']=tileDict[tile]['deccmax']

    GCol=['ra','dec','phot_g_mean_mag']
    GCat,GHead=cq.get_cat_radec_range(radec_box,dbh,dbSchema=dbSchema,table='GAIA_DR2',cols=GCol,verbose=args.verbose)

    if (args.method == "fixed"):
        hopt=[{'name':'TILENAME','value':args.tilename},
              {'name':'EXPAND',  'value':args.extend, 'comment':'arcminutes'},
              {'name':'METHOD',  'value':args.method},
              {'name':'CATALOG', 'value':'GAIA_DR2'}]
    else:
        hopt=[{'name':'TILENAME','value':args.tilename},
              {'name':'EXPAND',  'value':args.extend*100., 'comment':'percentage'},
              {'name':'METHOD',  'value':args.method},
              {'name':'CATALOG', 'value':'GAIA_DR2'}]
            
   # Write a fits file with the record array
    fitsio.write(args.output, GCat, header=hopt, extname='GAIA_OBJECT', clobber=True)
    print("# Wrote GAIA objects to: {ftab:s}".format(ftab=args.output))


    exit(0)
