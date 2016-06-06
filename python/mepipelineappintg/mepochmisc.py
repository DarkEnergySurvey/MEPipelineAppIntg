#!/usr/bin/env python

from despydb import desdbi

def get_tile_info(indict):
    # indict must have submit_des_services, submit_des_db_section, tilename

    dbh = desdbi.DesDbi(indict['submit_des_services'], indict['submit_des_db_section'])

    #sql = 'select id as tileid, to_char(ra_cent) as ra_cent, to_char(dec_cent) as dec_cent, pixelscale, naxis1, naxis2 from felipe.coaddtile_geom where tilename=%s' % dbh.get_named_bind_string('tilename')
    sql = 'select id as tileid, ra_cent, dec_cent, pixelscale, naxis1, naxis2 from felipe.coaddtile_geom where tilename=%s' % dbh.get_named_bind_string('tilename')

    curs = dbh.cursor()
    curs.execute(sql, {'tilename': indict['tilename']})
    desc = [d[0].lower() for d in curs.description]

    d = dict(zip(desc, curs.fetchone()))

    # since zerpoint version varies so much right now for multiepoch testbed...until have better way to choose zp version
    if indict['tilename'] in ['DES0002+0001','DES0002+0043','DES0511-5457','DES0516-5457','DES2356+0001','DES2356+0043','DES2359+0001','DES2359+0043','DES2354+0001','DES2354+0043']: 
        d['zversion'] = 'Y1A1_gruendlhack'
    elif indict['tilename'] in ['DES0459-5622','DES0508-5540','DES0509-5622','DES0511-5705','DES0513-5540','DES0514-5622','DES0517-5705','DES0518-5540','DES0520-5622','DES2246-4457','DES2247-4414']:
        d['zversion'] = 'SVA1_gruendlhack'
    else:
        d['zversion'] = 'SVA1_gruendlhack'

    return d
