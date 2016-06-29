#!/usr/bin/env python

from despydb import desdbi

def get_tile_info(indict):
    # indict must have submit_des_services, submit_des_db_section, tilename

    dbh = desdbi.DesDbi(indict['submit_des_services'], indict['submit_des_db_section'])

    sql = 'select id as tileid, ra_cent, dec_cent, pixelscale, naxis1, naxis2, uramin, uramax, udecmin, udecmax, crossra0 from coaddtile_geom where tilename=%s' % dbh.get_named_bind_string('tilename')

    curs = dbh.cursor()
    curs.execute(sql, {'tilename': indict['tilename']})
    desc = [d[0].lower() for d in curs.description]

    d = dict(zip(desc, curs.fetchone()))

    return d
