#!/usr/bin/env python

from despydb import desdbi
import os

def get_tile_info(indict):
    # indict must have submit_des_services, submit_des_db_section, tilename

    dbh = desdbi.DesDbi(indict['submit_des_services'], indict['submit_des_db_section'])

    sql = 'select id as tileid, ra_cent, dec_cent, pixelscale, naxis1, naxis2, uramin, uramax, udecmin, udecmax, crossra0 from coaddtile_geom where tilename=%s' % dbh.get_named_bind_string('tilename')

    curs = dbh.cursor()
    curs.execute(sql, {'tilename': indict['tilename']})
    desc = [d[0].lower() for d in curs.description]

    d = dict(zip(desc, curs.fetchone()))

    return d



def write_textlist(dbh,dict_input, outfile, archive_name='desar2home', fields=['fullname','band','expnum'], verb=None):

    """ Write a simple ascii list from a dictionary """

    # Get root archive like: /archive_data/desarchive
    root_archive = get_root_archive(dbh, archive_name=archive_name, verb=verb)

    of = open(outfile,'w')
    for val in dict_input.values():

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
