# $Id: mepochmisc.py 48316 2019-03-01 20:00:27Z rgruendl $
# $Rev:: 48316                            $:  # Revision of last commit.
# $LastChangedBy:: rgruendl               $:  # Author of last commit.

import os
import time
from despydb import desdbi

######################################################################################
def get_tile_info(indict):
    # indict must have submit_des_services, submit_des_db_section, tilename

    dbh = desdbi.DesDbi(indict['submit_des_services'], indict['submit_des_db_section'])

    sql = f"select id as tileid, ra_cent, dec_cent, pixelscale, naxis1, naxis2, uramin, uramax, udecmin, udecmax, crossra0 from coaddtile_geom where tilename={dbh.get_named_bind_string('tilename')}"

    curs = dbh.cursor()
    curs.execute(sql, {'tilename': indict['tilename']})
    desc = [d[0].lower() for d in curs.description]

    d = dict(zip(desc, curs.fetchone()))

    return d


######################################################################################
def write_textlist(dbh, dict_input, outfile, archive_name='desar2home', sel_band=None,
                   fields=['fullname', 'band', 'expnum'], verb=None):
    """ Write a simple ascii list from a dictionary """

    # Get root archive like: /archive_data/desarchive
    root_archive = get_root_archive(dbh, archive_name=archive_name, verb=verb)

    of = open(outfile, 'w')
    #
    #    for val in dict_input.values():
    #
    #   RAG:  change below should order output list (for the case where inputs dicts share a common set of top level keys
    #
    for key in sorted(dict_input.keys()):
        val = dict_input[key]

        # if we want a specific band, only select the val[band] == sel_band
        if sel_band is None or val['band'] == sel_band:
            for field in fields:
                if field == 'fullname':
                    if val['compression'] is None:
                        val['compression'] = ''
                    val['fullname'] = os.path.join(root_archive, val['path'], val['filename'] + val['compression'])
                if field == 'pexpnum':
                    val['pexpnum'] = f"D{val['expnum']:08d}"
                if field == 'ngmixid':
                    val['ngmixid'] = f"D{val['expnum']:08d}-{val['ccdnum']:02d}"
                of.write(f"{val[field]} ")
            of.write("\n")
    if verb:
        print(f"Wrote file: {outfile}")



######################################################################################
def get_root_archive(dbh, archive_name='desar2home', verb=None):
    """ Get the root-archive fron the database"""
    cur = dbh.cursor()
    # root_archive
    query = f"SELECT root FROM ops_archive WHERE name='{archive_name}'"
    if verb:
        print(f"Getting the archive root name for section: {archive_name}")
        print(f"Will execute the SQL query:\n********\n** {query}\n********")
    cur.execute(query)
    root_archive = cur.fetchone()[0]
    if verb:
        print(f"root_archive: {root_archive}")
    return root_archive


######################################################################################
def find_tile_attempt(TileName, ProcTag, dbh, dbSchema, releasePrefix=None, Timing=False, verbose=0):
    """ Query code to obtain COADD tile PFW_ATTEMPT_ID after constraining
        that results are part of a specific PROCTAG.

        Inputs:
            TileName:  Tilename to be search for
            ProcTag:   Proctag name containing set to be worked on
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            releasePrefix: Prefix string (including _'s) to identify a specific set of tables
                           (Useful when working from releases in DESSCI).  None --> will substitute a null string.
            Timing:    Causes internal timing to report results.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            AttemptID: Resulting AttemptID
    """

    if releasePrefix is None:
        relPrefix = ""
    else:
        relPrefix = releasePrefix

    t0 = time.time()
    query = f"""SELECT
            distinct t.pfw_attempt_id as pfw_attempt_id
        FROM {dbSchema:s}{relPrefix:s}proctag t, {dbSchema:s}{relPrefix:s}catalog c
        WHERE t.tag='{ProcTag:s}'
            and t.pfw_attempt_id=c.pfw_attempt_id
            and c.filetype='coadd_cat'
            and c.tilename='{TileName:s}'
        """

    if verbose > 0:
        if verbose == 1:
            QueryLines = query.split('\n')
            QueryOneLine = 'sql = '
            for line in QueryLines:
                QueryOneLine = QueryOneLine + " " + line.strip()
            print(f"{QueryOneLine:s}")
        if verbose > 1:
            print(f"{query:s}")
    #
    #   Establish a DB connection
    #
    curDB = dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    attval = None
    for row in curDB:
        rowd = dict(zip(desc, row))
        if attval is None:
            attval = rowd['pfw_attempt_id']
        else:
            print(f"Found more than one attempt for tile={TileName:s} attval={attval:d} vs {rowd['pfw_attempt_id']:d} ")
            attval = rowd['pfw_attempt_id']

    #
    #   This is a backup attempt that is meant to handle cases where no coadd_catalogs were output (eg. Mangle standalone)
    #
    if attval is None:
        print("First attempt to find PFW_ATTEMPT_ID failed... switching to use miscfile")

        query = f"""SELECT
                distinct t.pfw_attempt_id as pfw_attempt_id
            FROM {dbSchema:s}{relPrefix:s}proctag t, {dbSchema:s}{relPrefix:s}miscfile m
            WHERE t.tag='{ProcTag:s}'
                and t.pfw_attempt_id=m.pfw_attempt_id
                and m.tilename='{TileName:s}'
            """

        if verbose > 0:
            if verbose == 1:
                QueryLines = query.split('\n')
                QueryOneLine = 'sql = '
                for line in QueryLines:
                    QueryOneLine = QueryOneLine + " " + line.strip()
                print(f"{QueryOneLine:s}")
            if verbose > 1:
                print(f"{query:s}")
#
        curDB.execute(query)
        desc = [d[0].lower() for d in curDB.description]

        for row in curDB:
            rowd = dict(zip(desc, row))
            if attval is None:
                attval = rowd['pfw_attempt_id']
            else:
                print(f"Found more than one attempt for tile={TileName:s} attval={attval:d} vs {rowd['pfw_attempt_id']:d} ")
                attval = rowd['pfw_attempt_id']

    if Timing:
        t1 = time.time()
        print(f" Query to find attempt execution time: {t1 - t0:.2f}")
    curDB.close()

    return attval

######################################################################################
def read_target_path(tpath_file, verbose=0):
    """ Read file that specifies relative paths for filetypes used on target machines"""

    tpath_Dict={}

    f1=open(tpath_file,'r')
    if (verbose > 0):
        print("File open: {:s}".format(tpath_file))
    for line in f1:
        line=line.strip()
        columns=line.split(':')
        if (columns[0].strip(" ")[0] != "#"):
            tpath_Dict[columns[0].strip(" ")]=columns[1].strip(" ")
    f1.close()
    if (verbose > 0):
        print("Read paths for {:d} filetype".format(len(tpath_Dict)))

    return tpath_Dict

######################################################################################
def update_fullname(Dict,tpath):
    """Update/create a 'fullname' entry that uses an updated tpath rather than archive path"""
    for Img in Dict:
        if (Dict[Img]['compression'] is None):
            Dict[Img]['compression']=''
        Dict[Img]['fullname']=tpath+'/'+Dict[Img]['filename']+Dict[Img]['compression']
    return Dict
