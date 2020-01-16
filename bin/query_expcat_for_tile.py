#!/usr/bin/env python3
# $Id: query_expcat_for_tile.py 34655 2015-03-06 19:40:56Z mgower $
# $Rev:: 34655                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2015-03-06 13:40:56 #$:  # Date of last commit.

""" Specialized query to find catalogs """

import argparse
import intgutils.queryutils as queryutils
import despydb.desdbi as desdbi

def parse_command_line():
    parser = argparse.ArgumentParser(description='Submit a run to the processing framework')
    parser.add_argument('--tile', action='store', required=True)
    parser.add_argument('--qoutfile', action='store', required=True)
    parser.add_argument('--qouttype', action='store')
    parser.add_argument('--runtag', action='store')
    parser.add_argument('--ccdlist', action='store')
    parser.add_argument('--exptag', action='store')
    parser.add_argument('--exptime', action='store')
    parser.add_argument('--des_services', action='store')
    parser.add_argument('--section', action='store')
    parser.add_argument('--schema', action='store', default='')
    parser.add_argument('--red2tile-schema', action='store', required=True)   # deprecated once red2tile is moved to prodbeta

    args = vars(parser.parse_args())   # convert dict
    return args


def create_sql_query(dbh, args):

    select_list = []
    from_list = []
    where_cond_list = []
    params = {}


    # inner query
    select_list.append('rt.*')
    select_list.append('w.reqnum')
    select_list.append('w.unitname')
    select_list.append('w.attnum')
    select_list.append('i.ccdnum')
    select_list.append('i.expnum')

    from_list.append(f"{args['red2tile_schema']}red2tile rt")
    from_list.append(f"{args['schema']}image i")
    from_list.append(f"{args['schema']}wgb w")

    where_cond_list.append('rt.image_filename=i.filename')
    where_cond_list.append('w.filename=i.filename')


    where_cond_list.append(f"rt.tile={dbh.get_named_bind_string('tile')}")
    params['tile'] = args['tile']
    if args['runtag'] is not None:
        from_list.append(f"{args['schema']}ops_proctag ptag")
        where_cond_list.append('w.reqnum=ptag.reqnum')
        where_cond_list.append('w.unitname=ptag.unitname')
        where_cond_list.append('w.attnum=ptag.attnum')
        where_cond_list.append(f"ptag.tag = {dbh.get_named_bind_string('runtag')}")
        params['runtag'] = args['runtag']

    isql = 'with info as (select /*+ materialize */ ' + ','.join(select_list)
    isql += ' from ' + ','.join(from_list)
    isql += ' where ' + ' and '.join(where_cond_list)
    isql += ')'

    print("inner query = ", isql)

    # main query
    sql = isql
    sql += " select distinct c.expnum,c.ccdnum,c.filename from catalog c,info, wgb where c.expnum=info.expnum and wgb.reqnum=info.reqnum and wgb.unitname=info.unitname and wgb.attnum=info.attnum and c.filename=wgb.filename and c.filetype='cat_finalcut'"


    return (sql, params)


def run_query(dbh, sql, params):
    """ Execute the query to get catalog information """

    print(sql)
    print(params)

    curs = dbh.cursor()
    curs.execute(sql, params)
    desc = [d[0].lower() for d in curs.description]

    filelist = []
    for row in curs:
        rowdict = dict(zip(desc, row))
        filelist.append(rowdict)

    return filelist




def main():
    args = parse_command_line()
    dbh = desdbi.DesDbi(args['des_services'], args['section'])
    (sql, params) = create_sql_query(dbh, args)
    filelist = run_query(dbh, sql, params)
    print(len(filelist))
    lines = queryutils.convert_single_files_to_lines(filelist)
    queryutils.output_lines(args['qoutfile'], lines, outtype=args['qouttype'])

if __name__ == '__main__':
    main()
