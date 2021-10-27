# $Id: coadd_query.py 48310 2019-03-01 16:24:53Z rgruendl $
# $Rev:: 48310                            $:  # Revision of last commit.
# $LastChangedBy:: rgruendl               $:  # Author of last commit.
# $LastCha
"""
A set of routines for obtaining tile boundaries (an RA,Dec box), 
manipulate those box boundaries,
and then extracting catalog data (organized with RA, Dec) based on those boundaries.
"""

import time
import pandas as pd
import numpy as np

########################################################################
def query_Tile_edges(Tile,dbh,dbSchema='DES_ADMIN.',table='Y6A1_COADDTILE_GEOM',verbose=0):
    """ Pull tile edges from a COADDTILE_GEOM release table:

        Tile:       Name of Tile to poll for information.
        dbh:        Database connection to be used
        dbSchema:   DB schema (default='DES_ADMIN')
        table:      Name of coadd tile geometry table (default='Y6A1_COADDTILE_GEOM')
        verbose:    Sets level of verbosity." 

        Return:     Dict with Tile for key containing information
    """
    

    QUERY = """select tilename,racmin,racmax,deccmin,deccmax,crossra0 from {schema:s}{tbl:s} where tilename='{tname:s}'""".format(schema=dbSchema,tbl=table,tname=Tile)

    if (verbose>0):
        print("# Will query: ")
        print(QUERY)

    curDB = dbh.cursor()
    curDB.execute(QUERY)
    desc = [d[0].lower() for d in curDB.description]

    tile_data={}
    for row in curDB:
        rowd = dict(zip(desc, row))
        TName = rowd['tilename']
        tile_data[TName]=rowd

    if (len(tile_data) < 1):
        raise ValueError("ERROR: Query to {rel:s}_COADDTILE_GEOM returned no entries corrseponding to {tname:s}".format(
            rel=release,tname=Tile))
    else:
        print("# Sucessfull query for {schema:s}{tbl:s}".format(schema=dbSchema,tbl=table))

    return tile_data


#########################################################################
def expand_range(tDict,extend=0.0,method='fixed',verbose=0):
    """ Take an RA/Dec range described in the format from DES COADDTILE_GEOM and expand it

        tDict[tile] = Tile Dict with elements/keys: racmin,racmax,deccmin,deccmax,crossra0
        extend      = (float) amount to extend bounds of each "tile"
        method      = (str) fractional --> expand by the current extent mutliplied by the value of extend
                            fixed      --> expand by a fixed number of arcminutes in each direction
                      Note negative values will shrink the extent but currently are NOT carefully checked....
        verbose     = Verbose=0 is silent, greater values give increased amount of commentary.
       
        Returns:      Altered tile dictionary. 
    """

#
#   Note this allows for dict to contain more than one tile (operate on each)
#
    for tile in tDict:

        if (method == 'fractional'):
            print("Expansion method: fractional ({:.2f} percent)".format(100.*extend))
            if (tDict[tile]['crossra0'] == 'Y'):
                dra=tDict[tile]['racmax']-(tDict[tile]['racmin']-360.0)
            else:
                dra=tDict[tile]['racmax']-tDict[tile]['racmin']
            ddec=tDict[tile]['deccmax']-tDict[tile]['deccmin']

            rmin=tDict[tile]['racmin']-(extend*dra)
            rmax=tDict[tile]['racmax']+(extend*dra)
            dmin=tDict[tile]['deccmin']-(extend*ddec)
            dmax=tDict[tile]['deccmax']+(extend*ddec)

        elif (method == 'fixed'):
            print("Expansion method: fixed ({:.1f} arcminutes)".format(extend))
            deccen=0.5*(tDict[tile]['deccmax']+tDict[tile]['deccmin'])
            cosdec=np.cos(deccen*np.pi/180.)
            dra=extend/60.0/cosdec
            ddec=extend/60.0

            rmin=tDict[tile]['racmin']-dra
            rmax=tDict[tile]['racmax']+dra
            dmin=tDict[tile]['deccmin']-ddec
            dmax=tDict[tile]['deccmax']+ddec

        else:
            print("Warning! Unrecognized method: '{:s}'.  No changes made to search range.".format(method))
            rmin=tDict[tile]['racmin']
            rmax=tDict[tile]['racmax']
            dmin=tDict[tile]['deccmin']
            dmax=tDict[tile]['deccmax']

#
#       Now some simple checking
#
        if (verbose > 0):
            print("Expanded: {:s} {:s}  RA: {:9.5f} << {:9.5f} -- {:9.5f} >> {:9.5f}   Dec: {:9.5f} << {:9.5f} -- {:9.5f} >> {:9.5f} ".format(
                tile,tDict[tile]['crossra0'],
                rmin,tDict[tile]['racmin'],tDict[tile]['racmax'],rmax,
                dmin,tDict[tile]['deccmin'],tDict[tile]['deccmax'],dmax))
        
#
#       In the unlikely even that the North/South Celestial Pole was crossed just truncate it at the pole (do not deal with the unholy case that really occurred)
#       
        if (dmin<-90.):
            dmin=-90.0        
            if (verbose > 0):
                print("Ammended: {:s} {:s}  RA: {:9.5f} << {:9.5f} -- {:9.5f} >> {:9.5f}   Dec: {:9.5f} << {:9.5f} -- {:9.5f} >> {:9.5f} ".format(
                    tile,tDict[tile]['crossra0'],
                    rmin,tDict[tile]['racmin'],tDict[tile]['racmax'],rmax,
                    dmin,tDict[tile]['deccmin'],tDict[tile]['deccmax'],dmax))
        if (dmax>90.):
            dmax=90.0        
            if (verbose > 0):
                print("Ammended: {:s} {:s}  RA: {:9.5f} << {:9.5f} -- {:9.5f} >> {:9.5f}   Dec: {:9.5f} << {:9.5f} -- {:9.5f} >> {:9.5f} ".format(
                    tile,tDict[tile]['crossra0'],
                    rmin,tDict[tile]['racmin'],tDict[tile]['racmax'],rmax,
                    dmin,tDict[tile]['deccmin'],tDict[tile]['deccmax'],dmax))

#
#       Check to make sure that if RA range crosses the RA=0/24 boundary is handled consistent with COADDTILE_GEOM/IMAGE table structure
#       
        if (tDict[tile]['crossra0'] == 'N'):
            if (rmin < 0.):
                rmin=rmin+360.0 
                tDict[tile]['crossra0']='Y'
                if (verbose > 0):
                    print("Ammended: {:s} {:s}  RA: {:9.5f} << {:9.5f} -- {:9.5f} >> {:9.5f}   Dec: {:9.5f} << {:9.5f} -- {:9.5f} >> {:9.5f} ".format(
                        tile,tDict[tile]['crossra0'],
                        rmin,tDict[tile]['racmin'],tDict[tile]['racmax'],rmax,
                        dmin,tDict[tile]['deccmin'],tDict[tile]['deccmax'],dmax))
            if (rmax > 360.):
                rmax=rmax-360.0 
                tDict[tile]['crossra0']='Y'
                if (verbose > 0):
                    print("Ammended: {:s} {:s}  RA: {:9.5f} << {:9.5f} -- {:9.5f} >> {:9.5f}   Dec: {:9.5f} << {:9.5f} -- {:9.5f} >> {:9.5f} ".format(
                        tile,tDict[tile]['crossra0'],
                        rmin,tDict[tile]['racmin'],tDict[tile]['racmax'],rmax,
                        dmin,tDict[tile]['deccmin'],tDict[tile]['deccmax'],dmax))
        
        tDict[tile]['racmin']=rmin
        tDict[tile]['racmax']=rmax
        tDict[tile]['deccmin']=dmin
        tDict[tile]['deccmax']=dmax

#   If there are more tiles lather/rinse/repeat

    return tDict


#########################################################################
def get_cat_radec_range(radec_box,dbh,dbSchema='des_admin.',table='GAIA_DR2',cols=['ra','dec','phot_g_mean_mag'],Timing=False,verbose=0):

    """ Pull Catalog Data in an RA/Dec range. Default is GAIA_DR2 objects (ra,dec,mag) in a region

    radec_box:  Dict w/ keys: ra1,ra2,dec1,dec2, and crossra0[bool] that describe box to search
    dbh:        Database connection to be used
    dbSchema:   DB schema (default='DES_ADMIN')
    table:      Catalog Table (must have RA,Dec) to query for objects (default='GAIA_DR2')
    cols:       List of columns to return (default is ['ra','dec','phot_g_mean_mag'])
    verbose:    Sets level of verbosity." 

    Return:     Dict of numpy arrays (one for each column), List of Columns
    
    """

    t0=time.time()
    if (radec_box['crossra0']):
#
#       Form Query for case where RA ranges crosses RA=0h (not very good at poles)
#
        query="""select {cname:s}
            from {schema:s}{tbl:s} 
            where (ra < {r2:.6f} or ra > {r1:.6f})
                and dec between {d1:.6f} and {d2:.6f}""".format(
        cname=",".join(cols),
        schema=dbSchema,
        tbl=table,
        r1=radec_box['ra1'],
        r2=radec_box['ra2'],
        d1=radec_box['dec1'],
        d2=radec_box['dec2'])
    else:
#
#       Form query for normal workhorse case
#
        query="""select {cname:s}
            from {schema:s}{tbl:s} 
            where ra between {r1:.6f} and {r2:.6f}
                and dec between {d1:.6f} and {d2:.6f}""".format(
        cname=",".join(cols),
        schema=dbSchema,
        tbl=table,
        r1=radec_box['ra1'],
        r2=radec_box['ra2'],
        d1=radec_box['dec1'],
        d2=radec_box['dec2'])
#
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
#   Establish a DB cursor
#
    curDB = dbh.cursor()

    prefetch=100000
    curDB.arraysize=int(prefetch)
    curDB.execute(query)
    header=[d[0].upper() for d in curDB.description]
    cat_data=pd.DataFrame(curDB.fetchall())

    CatDict={}
    if (cat_data.empty):
        print("# No values returned from query of {tval:s} ".format(tval="GAIA_DR2"))
        for val in header:
            CatDict[val]=np.array([])
    else:
        cat_data.columns=header
        for val in header:
            CatDict[val]=np.array(cat_data[val])
    curDB.close()

    if (verbose>0):
        print("# Number of objects found in {schema:s}{tbl:s} is {nval:d} ".format(
            schema=dbSchema,
            tbl=table,
            nval=CatDict[header[0]].size))
    if (Timing):
        t1=time.time()
        print(" Query execution time: {:.2f}".format(t1-t0))

    return CatDict,header


#########################################################################
def get_ALL_cat(dbh,dbSchema='DES_ADMIN.',table='GAIA_DR2',cols=['ra','dec','phot_g_mean_mag'],Timing=False,verbose=0):

    """ Pull the entirety of a Catalog.  Note this can easily overflow a machines memory for some tables.

    dbh:        Database connection to be used
    dbSchema:   DB schema (default='DES_ADMIN.')
    table:      Catalog Table to query for objects (default='GAIA_DR2')
    cols:       List of columns to return (default is ['ra','dec','phot_g_mean_mag'])
    verbose:    Sets level of verbosity." 

    Return:     Dict of numpy arrays (one for each column), 
                List of Columns

    """

    t0=time.time()
    query="select {cname:s} from {schema:s}{tbl:s} ".format(cname=",".join(cols),schema=dbSchema,tbl=table)
#
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
#   Establish a DB cursor
#
    curDB = dbh.cursor()
#    curDB.execute(query)
#    desc = [d[0].lower() for d in curDB.description]

    prefetch=100000
    curDB.arraysize=int(prefetch)
    curDB.execute(query)
#    header=[d[0].lower() for d in curDB.description]
    header=[d[0].upper() for d in curDB.description]
    cat_data=pd.DataFrame(curDB.fetchall())

    CatDict={}
    if (cat_data.empty):
        print("# No values returned from query of {tval:s} ".format(tval="GAIA_DR2"))
        for val in header:
            CatDict[val]=np.array([])
    else:
        cat_data.columns=header
        for val in header:
            CatDict[val]=np.array(cat_data[val])
    curDB.close()

    if (verbose>0):
        print("# Number of objects found in {schema:s}{tbl:s} is {nval:d} ".format(
            schema=dbSchema,
            tbl=table,
            nval=CatDict[header[0]].size))
    if (Timing):
        t1=time.time()
        print(" Query execution time: {:.2f}".format(t1-t0))

    return CatDict,header


