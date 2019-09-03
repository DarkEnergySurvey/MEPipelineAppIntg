#!/usr/bin/env python
# $Id: mepochmisc.py 48316 2019-03-01 20:00:27Z rgruendl $
# $Rev:: 48316                            $:  # Revision of last commit.
# $LastChangedBy:: rgruendl               $:  # Author of last commit.

from despydb import desdbi
import os
import time

######################################################################################
def get_tile_info(indict):
    # indict must have submit_des_services, submit_des_db_section, tilename

    dbh = desdbi.DesDbi(indict['submit_des_services'], indict['submit_des_db_section'])

    sql = 'select id as tileid, ra_cent, dec_cent, pixelscale, naxis1, naxis2, uramin, uramax, udecmin, udecmax, crossra0 from coaddtile_geom where tilename=%s' % dbh.get_named_bind_string('tilename')

    curs = dbh.cursor()
    curs.execute(sql, {'tilename': indict['tilename']})
    desc = [d[0].lower() for d in curs.description]

    d = dict(zip(desc, curs.fetchone()))

    return d


######################################################################################
def write_textlist(dbh,dict_input, outfile, archive_name='desar2home', fields=['fullname','band','expnum'], verb=None):

    """ Write a simple ascii list from a dictionary """

    # Get root archive like: /archive_data/desarchive
    root_archive = get_root_archive(dbh, archive_name=archive_name, verb=verb)

    of = open(outfile,'w')
#
#    for val in dict_input.values():
#
#   RAG:  change below should order output list (for the case where inputs dicts share a common set of top level keys
#   If we switch to python3.x then the line below should become "for key in sorted(dict_input.keys()):"
#
    for key in sorted(dict_input.iterkeys()):
        val=dict_input[key]

        for field in fields:
            if field == 'fullname':
                if val['compression'] is None: val['compression'] = ''
                val['fullname'] = os.path.join(root_archive, val['path'],val['filename']+val['compression'])
            if field == 'pexpnum':
                val['pexpnum'] = "D%08d" % val['expnum']
            if field == 'ngmixid':
                val['ngmixid'] = "D%08d-%02d" % (val['expnum'],val['ccdnum'])
            of.write("%s "% val[field])
        of.write("\n")
    if verb: print "Wrote file: %s" % outfile

    return


######################################################################################
def get_root_archive(dbh, archive_name='desar2home', verb=None):
    """ Get the root-archive fron the database"""
    cur = dbh.cursor()
    # root_archive
    query = "SELECT root FROM ops_archive WHERE name='%s'" % archive_name
    if verb:
        print "Getting the archive root name for section: %s" % archive_name
        print "Will execute the SQL query:\n********\n** %s\n********" % query
    cur.execute(query)
    root_archive = cur.fetchone()[0]
    if verb: print "root_archive: %s" % root_archive
    return root_archive


######################################################################################
def find_tile_attempt(TileName,ProcTag,dbh,dbSchema,releasePrefix=None,Timing=False,verbose=0):
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

    if (releasePrefix is None):
        relPrefix=""
    else:
        relPrefix=releasePrefix

    t0=time.time()
    query="""SELECT
            distinct t.pfw_attempt_id as pfw_attempt_id
        FROM {schema:s}{rpref:s}proctag t, {schema:s}{rpref:s}catalog c
        WHERE t.tag='{ptag:s}' 
            and t.pfw_attempt_id=c.pfw_attempt_id
            and c.filetype='coadd_cat'
            and c.tilename='{tname:s}'
        """.format(
            schema=dbSchema,ptag=ProcTag,tname=TileName,rpref=relPrefix)

    if (verbose > 0):
        if (verbose == 1):
            QueryLines=query.split('\n')
            QueryOneLine='sql = '
            for line in QueryLines:
                QueryOneLine=QueryOneLine+" "+line.strip()
            print("{:s}".format(QueryOneLine))
        if (verbose > 1):
            print("{:s}".format(query))
#
#   Establish a DB connection
#
    curDB = dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    attval=None
    for row in curDB:
        rowd = dict(zip(desc, row))
        if (attval is None):
            attval=rowd['pfw_attempt_id']
        else:
            print("Found more than one attempt for tile={:s} attval={:ld} vs {:ld} ".format(TileName,attval,rowd['pfw_attempt_id']))
            attval=rowd['pfw_attempt_id']

#
#   This is a backup attempt that is meant to handle cases where no coadd_catalogs were output (eg. Mangle standalone)
#
    if (attval is None):
        print("First attempt to find PFW_ATTEMPT_ID failed... switching to use miscfile")

        query="""SELECT
                distinct t.pfw_attempt_id as pfw_attempt_id
            FROM {schema:s}{rpref:s}proctag t, {schema:s}{rpref:s}miscfile m
            WHERE t.tag='{ptag:s}' 
                and t.pfw_attempt_id=m.pfw_attempt_id
                and m.tilename='{tname:s}'
            """.format(
                schema=dbSchema,ptag=ProcTag,tname=TileName,rpref=relPrefix)

        if (verbose > 0):
            if (verbose == 1):
                QueryLines=query.split('\n')
                QueryOneLine='sql = '
                for line in QueryLines:
                    QueryOneLine=QueryOneLine+" "+line.strip()
                print("{:s}".format(QueryOneLine))
            if (verbose > 1):
                print("{:s}".format(query))
#
        curDB.execute(query)
        desc = [d[0].lower() for d in curDB.description]

        for row in curDB:
            rowd = dict(zip(desc, row))
            if (attval is None):
                attval=rowd['pfw_attempt_id']
            else:
                print("Found more than one attempt for tile={:s} attval={:ld} vs {:ld} ".format(TileName,attval,rowd['pfw_attempt_id']))
                attval=rowd['pfw_attempt_id']

    if (Timing):
        t1=time.time()
        print(" Query to find attempt execution time: {:.2f}".format(t1-t0))
    curDB.close()

    return attval


