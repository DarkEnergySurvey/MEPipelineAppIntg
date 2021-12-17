#! /usr/bin/env python3

"""
Query PIFF_HSM_MODEL_QA to obtain data relevant for the inputs to a specific tile and write as a FITS table
"""

verbose = 0

######################################################################################

if __name__ == "__main__":

    import argparse
    import os
    import despydb.desdbi
#    import time
#    import yaml
    import mepipelineappintg.meds_query as mq
    import mepipelineappintg.piff_qa_query as pq


    parser = argparse.ArgumentParser(description='Query code to obtain image inputs for COADD/multiepoch pipelines.')
    parser.add_argument('--me_proctag', action='store', type=str, default=None,
                        help='Multi-Epoch Processing Tag where coadd tile was constructed.')
    parser.add_argument('--tilename', action='store', type=str, default=None,
                        help='Tilename of interest')
    parser.add_argument('-A', '--pfw_attempt_id', action='store', type=str, default=None, required=False,
                        help='Processing attempt ID: alternate method to discover coadd tile inputs.')
#    parser.add_argument('--bandlist', action='store', type=str, default='g,r,i,z,Y',
#                        help='Comma separated list of bands to be COADDed (Default="g,r,i,z,Y").')
    parser.add_argument('--piff_tag', action='store', type=str, default=None, required=True,
                        help='Proctag TAG containing PIFF afterburner products (PIFF models)')
    parser.add_argument('-o', '--output', action='store', type=str, required=True,
                        help='Output FITS table to be written')
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
    #   Handle simple args (verbose, Schema, PFW_ATTEMPT_ID)
    #
    verbose = args.verbose

    if args.Schema is None:
        dbSchema = ""
    else:
        dbSchema = f"{args.Schema}."

    #
    #   Check whether a PFW_ATTEMPT_ID was provided (or if one needs to be determined from --tilename --me_proctag)
    #
    if (args.me_proctag is None)or(args.tilename is None):
        if (args.pfw_attempt_id is None):
            print("Must provide either a PFW_ATTEMPT_ID (-A) or PROCTAG and TILENAME (--me_proctag --tilename)")
            print("Aborting!")
            exit(1)

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

    if (args.pfw_attempt_id is None):
        IntID = mq.query_attempt_from_tag_tile(args.me_proctag,args.tilename,dbh,dbSchema,verbose)
        if (IntID is None):
            print("Failed to obtain a PFW_ATTEMPT_ID so will not be able to identify a run to base inputs on")
            print("Aborting")
            exit(1)
        PFWattemptID=str(IntID)
    else:
        PFWattemptID = args.pfw_attempt_id


    AOK = pq.get_piff_qa(args.output, args.piff_tag, PFWattemptID, dbh, dbSchema, verbose=verbose)

    print(f"# Exit status: {AOK}")
