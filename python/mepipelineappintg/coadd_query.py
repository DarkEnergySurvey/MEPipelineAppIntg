#! /usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastCha
"""
A set of queries to obtain inputs for the COADD pipeline.
"""

import despydb.desdbi

######################################################################################
def query_coadd_geometry(TileDict,CoaddTile,dbh,dbSchema,verbose=0):
    """ Query code to obtain COADD tile geometry

        Inputs:
            TileDict:  Existing TileDict, new records are added (and possibly old records updated)
            CoaddTile: Name of COADD tile for search
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            TileDict:  Updated version of input TileDict
    """

#
#   Query to obtain geometric specification of a specific COADD tile.
#

    query="""SELECT 
        t.tilename as tilename,
        t.ra_cent as ra_cent, t.dec_cent as dec_cent,
        t.rac1 as rac1, t.rac2 as rac2, t.rac3 as rac3, t.rac4 as rac4,
        t.decc1 as decc1, t.decc2 as decc2, t.decc3 as decc3, t.decc4 as decc4,
        t.crossra0 as crossra0, 
        t.pixelscale as pixelscale, t.naxis1 as naxis1, t.naxis2 as naxis2
    FROM %scoaddtile_geom t
    WHERE t.tilename = '%s'
        """ % (dbSchema,CoaddTile)

    if (verbose > 0):
        if (verbose == 1):
            QueryLines=query.split('\n')
            QueryOneLine='sql = '
            for line in QueryLines:
                QueryOneLine=QueryOneLine+" "+line.strip()
            print QueryOneLine
        if (verbose > 1):
            print query

    curDB=dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        TileName=rowd['tilename']
        TileDict[TileName]=rowd
#
#       Fix any known problematic NoneTypes before they get in the way.
#
#        if (ImgDict[ImgName]['band'] is None):
#            ImgDict[ImgName]['band']='None'

    return TileDict


######################################################################################
def query_coadd_img_by_edges(ImgDict,CoaddTile,ProcTag,BandList,ArchiveSite,dbh,dbSchema,verbose=0):
    """ Query code to obtain image inputs for COADD (based on tile/img edges)
        Use an existing DB connection to execute a query for RED_IMMASK image
        products that overlap a spsecic COADD tile.
        Return a dictionary of Images along with with basic properties (expnum, 
        ccdnum, band, nite).

        Inputs:
            ImgDict:   Existing ImgDict, new records are added (and possibly old records updated)
            CoaddTile: Name of COADD tile for search
            ProcTag:   Processing Tag used to constrain pool of input images
            BandList:  List of bands (returned ImgDict list will be restricted to only these bands)
            ArchiveSite: Constraint that data/files exist within a specific archive
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            ImgDict:   Updated version of input ImgDict
    """
#
#   Pre-assemble constraint based on BandList
#
    BandConstraint=''
    if (len(BandList)>0):
        BandConstraint="and i.band in ('" + "','".join([d.strip() for d in BandList]) + "')"
#
#   Query to obtain images and associated metadata.  Note the current version may be
#   counter-intuitive and makes two references to image: "image i" and "image j".  
#   This is necessary for the workhorse portion of the query (image j) to make use 
#   of the indices (over RACMIN, RACMAX...).  The second instance (image i)
#   is necessary to obtain other infomration (e.g. band, expnum, ccdnum) in a way that 
#   does not confuse Oracle into ignoring the index and instead executing the query as a 
#   table scan.  Under our current database this may be fragile... also, experimentation
#   has shown that this cannot be fixed by simply forcing the index with a runtime 
#   directive (e.g. /* +index */
#
    
    query="""SELECT 
        fai.filename as filename,
        fai.compression as compression, 
        i.band as band,
        i.expnum as expnum,
        i.ccdnum as ccdnum,
        i.rac1 as rac1, i.rac2 as rac2, i.rac3 as rac3, i.rac4 as rac4,
        i.decc1 as decc1, i.decc2 as decc2, i.decc3 as decc3, i.decc4 as decc4
    FROM {schema:s}image i, {schema:s}file_archive_info fai, {schema:s}proctag t
    WHERE t.tag='{proctag:s}'
        and t.pfw_attempt_id=i.pfw_attempt_id
        and i.filetype='red_immask'
        and i.filename=fai.filename
        {bandconstraint:s}
        and fai.archive_name='{archive:s}' 
        and exists (
            select 1
            from {schema:s}image j, {schema:s}coaddtile_geom ct
            WHERE ct.tilename='{tile:s}'
                AND j.filename=i.filename
                AND ((ct.crossra0='N'
                        AND ((j.racmin between ct.racmin and ct.racmax)OR(j.racmax between ct.racmin and ct.racmax))
                        AND ((j.deccmin between ct.deccmin and ct.deccmax)OR(j.deccmax between ct.deccmin and ct.deccmax))
                    )OR(ct.crossra0='Y'
                        AND ((j.racmin between ct.racmin and 360.)OR(j.racmin between 0.0 and ct.racmax)
                            OR(j.racmax between ct.racmin and 360.)OR(j.racmax between 0.0 and ct.racmax))
                        AND ((j.deccmin between ct.deccmin and ct.deccmax)OR(j.deccmax between ct.deccmin and ct.deccmax))
                    ))
            )
        """.format(
            schema=dbSchema,
            proctag=ProcTag,
            tile=CoaddTile,
            bandconstraint=BandConstraint,
            archive=ArchiveSite)

#        {bandconstraint:s}
#            bandconstraint=BandConstraint,
    query="""SELECT 
        fai.filename as filename,
        fai.compression as compression,
        i.band as band,
        i.expnum as expnum,
        i.ccdnum as ccdnum,
        i.rac1 as rac1, i.rac2 as rac2, i.rac3 as rac3, i.rac4 as rac4,
        i.decc1 as decc1, i.decc2 as decc2, i.decc3 as decc3, i.decc4 as decc4
    FROM {schema:s}image i, {schema:s}file_archive_info fai, {schema:s}proctag t, {schema:s}coaddtile_geom ct
    WHERE t.tag='{proctag:s}'
        and t.pfw_attempt_id=i.pfw_attempt_id
        and i.filetype='red_immask'
        and i.filename=fai.filename
        and fai.archive_name='{archive:s}' 
        and ct.tilename = '{tile:s}'
        and ((ct.crossra0='N'
                AND ((i.racmin between ct.racmin and ct.racmax)OR(i.racmax between ct.racmin and ct.racmax))
                AND ((i.deccmin between ct.deccmin and ct.deccmax)OR(i.deccmax between ct.deccmin and ct.deccmax))
            )OR(ct.crossra0='Y'
                AND ((i.racmin between ct.racmin and 360.)OR(i.racmin between 0.0 and ct.racmax)
                    OR(i.racmax between ct.racmin and 360.)OR(i.racmax between 0.0 and ct.racmax))
                AND ((i.deccmin between ct.deccmin and ct.deccmax)OR(i.deccmax between ct.deccmin and ct.deccmax))
            ))""".format(
            schema=dbSchema,
            proctag=ProcTag,
            tile=CoaddTile,
            archive=ArchiveSite)

    if (verbose > 0):
        print("# Executing query to obtain red_immask images (based on their edges/boundaries)")
        if (verbose == 1):
            print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
        if (verbose > 1):
            print("# sql = {:s}".format(query))
    curDB=dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
#        ImgName=rowd['filename']
#        ImgDict[ImgName]=rowd
        if (rowd['band'] in BandList):
            ImgName=rowd['filename']
            ImgDict[ImgName]=rowd
#            ImgList.append([ImgName])
        else:
            if (verbose > 1):
                print(" Post query constraint removed {:s}-band image: {:s} ".format(rowd['band'],rowd['filename']))

    return ImgDict



######################################################################################
def query_zeropoint(ImgDict,ZptInfo,dbh,dbSchema,verbose=0):
    """ Query code to obtain zeropoints for a set of images in existing ImgDict.
        Use an existing DB connection to execute a query to obtain ZEROPOINTs 
        for an existing set of images.  If images in the input list are not 
        returned (i.e. do not have a zeropoint) they are removed from the dictionary 
        that is returned.

        Inputs:
            ImgDict:    Existing ImgDict, (returned dictionary will remove records that have no zeropoint)
            ZptInfo:    Dictionary containing information about Zeropoint Constraint 
                        (NoneType yields no constraint)
                            ZptInfo['table']:   provides table to use 
                            ZptInfo['source']:  Zpt source constraint
                            ZptInfo['version']: Zpt version constraint
                            ZptInfo['flag']:    Zpt flag constraint
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            ImgDict:   Updated version of input ImgDict
    """
#
#   Pre-assemble portions of query that pertain to ZEROPOINT (no constraint will output mag_zero=30 for all exposures)
#
    ZptData=''
    ZptTable=''
    ZptConstraint=''
    if (ZptInfo is not None):
        ZptData='z.mag_zero as mag_zero,'
        ZptTable=', %s z' % ZptInfo['table']

        ZptSrcConstraint=''
        if ('source' in ZptInfo):
            ZptSrcConstraint="and z.source='%s'" % (ZptInfo['source'])

        ZptVerConstraint=''
        if ('version' in ZptInfo):
            ZptVerConstraint="and z.version='%s'" % (ZptInfo['version'])

        ZptFlagConstraint=''
        if ('flag' in ZptInfo):
            ZptFlagConstraint='and z.flag=0'

        ZptConstraint="""and z.imagename=i.filename %s %s %s""" % (ZptSrcConstraint,ZptVerConstraint,ZptFlagConstraint)
#
#   Prepare GTT_FILENAME table with list of possible inputs 
#
    ImgList=[]
    for ImgName in ImgDict:
        ImgList.append([ImgName])

    # Make sure the GTT_FILENAME table is empty
    curDB=dbh.cursor()
    curDB.execute('delete from GTT_FILENAME')
    # load img ids into opm_filename_gtt table
    print("# Loading GTT_FILENAME table for secondary queries with entries for {:d} images".format(len(ImgList)))
    dbh.insert_many('GTT_FILENAME',['FILENAME'],ImgList)
#
#   Query to obtain zeropoints 
#
    query="""SELECT 
        gtt.filename as filename,
        {zptdata:s}
        i.expnum as expnum,
        i.ccdnum as ccdnum
    FROM {schema:s}image i, gtt_filename gtt{zpttable:s}
    WHERE i.filename=gtt.filename
       {zptconstraint:s}
        """.format(
            zptdata=ZptData,
            schema=dbSchema,
            zpttable=ZptTable,
            zptconstraint=ZptConstraint)

    if (verbose > 0):
        print("# Executing query to obtain red_bkg images corresponding to the red_immasked images")
        if (verbose == 1):
            print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
        if (verbose > 1):
            print("# sql = {:s}".format(query))
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

#
#   New image dictionary is formed so that images that do not return in this query are not returned

    NewImgDict={}
    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName=rowd['filename']
        if (ImgName in ImgDict):
            NewImgDict[ImgName]=ImgDict[ImgName]
            if ('mag_zero' in rowd):
                NewImgDict[ImgName]['mag_zero']=rowd['mag_zero']
#            NewImgList.append([ImgName])
        else:
            if (verbose > 2):
                print(" No matching record? in query for zeropoint for (ImgName={:s} ".format(ImgName))

    ImgDict=NewImgDict 
#    ImgList=NewImgList
    return ImgDict



######################################################################################
def query_blacklist(ImgDict,BlacklistInfo,dbh,dbSchema,verbose=0):
    """ Query code to obtain zeropoints (and to remove blacklisted images) from an existing ImgDict 
        Use an existing DB connection to execute a query to obtain ZEROPOINTs (and to remove blacklisted expsoures)
        for an existing set of images.  If images in the input list are not returned (i.e. do not have a zeropoin
        or have been blacklisted then they are removed from the dictionary that is returned.

        Inputs:
            ImgDict:    Existing ImgDict, new records are added (and possibly old records updated)
            Blacklistnfo: Dictionary containing information about Blacklist Constraint 
                          (NoneType yields no constraint)
                            BlacklistInfo['table'] provides table to use
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            ImgDict:   Updated version of input ImgDict
    """
#
#   Pre-assemble portions of query that pertain to the BLACKLIST (no constraint)
#
#    BlacklistTable=''
    BlacklistConstraint=''
    if (BlacklistInfo is not None):
        BlacklistConstraint="""and not exists (select bl.reason from %s bl where bl.expnum=i.expnum and bl.ccdnum=i.ccdnum)""" % BlacklistInfo['table']

#
#   Prepare GTT_FILENAME table with list of possible inputs 
#
    ImgList=[]
    for ImgName in ImgDict:
        ImgList.append([ImgName])

    # Make sure the GTT_FILENAME table is empty
    curDB=dbh.cursor()
    curDB.execute('delete from GTT_FILENAME')
    # load img ids into opm_filename_gtt table
    print("# Loading GTT_FILENAME table for secondary queries with entries for {:d} images".format(len(ImgList)))
    dbh.insert_many('GTT_FILENAME',['FILENAME'],ImgList)
#
#   Query to obtain zeropoints 
#
    query="""SELECT 
        gtt.filename as filename,
        i.expnum as expnum,
        i.ccdnum as ccdnum
    FROM {schema:s}image i, gtt_filename gtt
    WHERE i.filename=gtt.filename
       {blackconstraint:s}
        """.format(
            schema=dbSchema,
            blackconstraint=BlacklistConstraint)

    if (verbose > 0):
        print("# Executing query to remove blacklisted images")
        if (verbose == 1):
            print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
        if (verbose > 1):
            print("# sql = {:s}".format(query))
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

#
#   New image dictionary is formed so that images that do not return in this query are not returned
#
    NewImgDict={}
    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName=rowd['filename']
        if (ImgName in ImgDict):
            NewImgDict[ImgName]=ImgDict[ImgName]

    if (verbose > 0):
        print("# Blacklist removed {:d} of {:d} images.".format((len(ImgDict)-len(NewImgDict)),len(ImgDict)))

    ImgDict=NewImgDict 

    return ImgDict



######################################################################################
def query_bkg_img(ImgDict,ArchiveSite,dbh,dbSchema,verbose=0):
    """ Query code to obtain BKG images associated with a set of red_immask images.
        Use an existing DB connection to execute a query to obtain RED_BKG images
        for an existing set of images.  

        Inputs:
            ImgDict:    Existing ImgDict
            ArchiveSite: 
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            BkgDict:   Ouput dictionary of RED_BKG images
    """
#
#   Prepare GTT_FILENAME table with list of possible inputs 
#
    ImgList=[]
    for ImgName in ImgDict:
        ImgList.append([ImgName])

#   Setup DB cursor
    curDB=dbh.cursor()
#   Make sure teh GTT_FILENAME table is empty
    curDB.execute('delete from GTT_FILENAME')
#   Load img ids into opm_filename_gtt table
    print("# Loading GTT_FILENAME table for secondary queries with entries for {:d} images".format(len(ImgList)))
    dbh.insert_many('GTT_FILENAME',['FILENAME'],ImgList)

#
#   Obtain associated bkgd image (red_bkg).
#
    query="""SELECT 
        i.filename as redfile,
        fai.filename as filename,
        fai.compression as compression,
        k.band as band,
        k.expnum as expnum,
        k.ccdnum as ccdnum
    FROM {schema:s}image i, {schema:s}image k, {schema:s}file_archive_info fai, GTT_FILENAME gtt
    WHERE i.filename=gtt.filename
        and i.pfw_attempt_id=k.pfw_attempt_id
        and k.filetype='red_bkg'
        and i.ccdnum=k.ccdnum
        and k.filename=fai.filename
        and fai.archive_name='{archive:s}'
    """.format(schema=dbSchema,
        archive=ArchiveSite)

    if (verbose > 0):
        print("# Executing query to obtain red_bkg images corresponding to the red_immasked images")
        if (verbose == 1):
            print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
        if (verbose > 1):
            print("# sql = {:s}".format(query))
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    BkgDict={}
    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName=rowd['redfile']
#        BkgName=rowd['filename']
        BkgDict[ImgName]=rowd
#        if ('mag_zero' in ImgDict[ImgName]):
#            BkgDict[ImgName]['mag_zero']=ImgDict[ImgName]['mag_zero']

#            ImgDict[ImgName]['skyfilename']=rowd['skyfilename']
#            ImgDict[ImgName]['skycompress']=rowd['skycompress']
#        else:
#            if (verbose > 2):
#                print(" No matching record? in query for skyfilename for (ImgName={:s} ".format(ImgName))

    return BkgDict



######################################################################################
def query_segmap(ImgDict,ArchiveSite,dbh,dbSchema,verbose=0):
    """ Query code to obtain Segmentation Map Images (red_segmap) associated with a set of red_immask images.
        Use an existing DB connection to execute a query to obtain RED_SEGMAP images
        for an existing set of images.  

        Inputs:
            ImgDict:    Existing ImgDict
            ArchiveSite: Archive_name 
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            SegDict:   Ouput dictionary of RED_BKG images
    """
#
#   Prepare GTT_FILENAME table with list of possible inputs 
#
    ImgList=[]
    for ImgName in ImgDict:
        ImgList.append([ImgName])

#   Setup DB cursor
    curDB=dbh.cursor()
#   Make sure teh GTT_FILENAME table is empty
    curDB.execute('delete from GTT_FILENAME')
#   load img ids into opm_filename_gtt table
    print("# Loading GTT_FILENAME table for secondary queries with entries for {:d} images".format(len(ImgList)))
    dbh.insert_many('GTT_FILENAME',['FILENAME'],ImgList)
#
#   Obtain associated segmentation map image (red_segmap).
#
    query="""SELECT 
        i.filename as redfile,
        fai.filename as filename,
        fai.compression as compression,
        m.band as band,
        m.expnum as expnum,
        m.ccdnum as ccdnum
    FROM {schema:s}image i, {schema:s}miscfile m, {schema:s}file_archive_info fai, GTT_FILENAME gtt
    WHERE i.filename=gtt.filename
        and i.pfw_attempt_id=m.pfw_attempt_id
        and m.filetype='red_segmap'
        and i.ccdnum=m.ccdnum
        and m.filename=fai.filename
        and fai.archive_name='{archive:s}'
    """.format(schema=dbSchema,archive=ArchiveSite)

    if (verbose > 0):
        print("# Executing query to obtain segmentation map images corresponding to the red_immasked images")
        if (verbose == 1):
            print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
        if (verbose > 1):
            print("# sql = {:s}".format(query))
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    SegDict={}
    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName=rowd['redfile']
        SegDict[ImgName]=rowd
#        if ('mag_zero' in ImgDict[ImgName]):
#            SegDict[ImgName]['mag_zero']=ImgDict[ImgName]['mag_zero']

    return SegDict



######################################################################################
def query_catfinalcut(ImgDict,ArchiveSite,dbh,dbSchema,verbose=0):
    """ Query code to obtain CAT_FINALCUT catalogs associated with a set of red_immask images.
        Use an existing DB connection to execute a query to obtain RED_SEGMAP images
        for an existing set of images.  

        Inputs:
            ImgDict:    Existing ImgDict
            ArchiveSite: Archive_name 
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            CatDict:   Ouput dictionary of CAT_FINALCUT catalogs (only those associated with images in ImgDict)
    """
#
#   Prepare GTT_FILENAME table with list of possible inputs 
#
    ImgList=[]
    for ImgName in ImgDict:
        ImgList.append([ImgName])

#   Setup DB cursor
    curDB=dbh.cursor()
#   Make sure teh GTT_FILENAME table is empty
    curDB.execute('delete from GTT_FILENAME')
#   load img ids into opm_filename_gtt table
    print("# Loading GTT_FILENAME table for secondary queries with entries for {:d} images".format(len(ImgList)))
    dbh.insert_many('GTT_FILENAME',['FILENAME'],ImgList)
#
#   Obtain associated catlogs (cat_finalcut).
#
    query="""SELECT 
        i.filename as redfile,
        fai.filename as filename,
        fai.compression as compression,
        c.band as band,
        c.expnum as expnum,
        c.ccdnum as ccdnum
    FROM {schema:s}image i, {schema:s}catalog c, {schema:s}file_archive_info fai, GTT_FILENAME gtt
    WHERE i.filename=gtt.filename
        and i.pfw_attempt_id=c.pfw_attempt_id
        and c.filetype='cat_finalcut'
        and i.ccdnum=c.ccdnum
        and c.filename=fai.filename
        and fai.archive_name='{archive:s}'
    """.format(schema=dbSchema,archive=ArchiveSite)

    if (verbose > 0):
        print("# Executing query to obtain cat_finalcut catalogs corresponding to the red_immasked images")
        if (verbose == 1):
            print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
        if (verbose > 1):
            print("# sql = {:s}".format(query))
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    CatDict={}
    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName=rowd['redfile']
        CatDict[ImgName]=rowd
#        if ('mag_zero' in ImgDict[ImgName]):
#            CatDict[ImgName]['mag_zero']=ImgDict[ImgName]['mag_zero']

    return CatDict




######################################################################################
def query_coadd_img_by_extent(ImgDict,CoaddTile,ProcTag,dbh,dbSchema,BandList,verbose=0):
    """ Query code to obtain image inputs for COADD (based on centers and extents of Tile/Imgs).
        Note this is an alternate version (and is currently non-performant).

        Use an existing DB connection to execute query(s) for RED_IMMASK image
        products that overlap a spsecic COADD tile.
        Return a dictionary of Images along with with basic properties (expnum, 
        ccdnum, band, nite).

        Inputs:
            ImgDict:   Existing ImgDict, new records are added (and possibly old records updated)
            CoaddTile: Name of COADD tile for search
            ProcTag:   Processing Tag used to constrain pool of input images
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            BandList:  List of bands (returned ImgDict list will be restricted to only these bands)
            verbose:   Integer setting level of verbosity when running.

        Returns:
            ImgDict:   Updated version of input ImgDict
    """


#
#   Pre-assemble constraint based on BandList
#
    BandConstraint=''
    if (len(BandList)>0):
        BandConstraint="and band in ('" + "','".join([d.strip() for d in BandList]) + "')"

#
#   Prepare queries used to find images that correspond to a tile (based on their extents)
#

    query1="""select crossra0 from {schema:s}coaddtile_geom where tilename='{tile:s}'""".format(schema=dbSchema,tile=CoaddTile)

    query2a="""with ima as
    (SELECT /*+ materialize */
         FILENAME, FILETYPE, CROSSRA0, PFW_ATTEMPT_ID, BAND, CCDNUM,
         RA_CENT, DEC_CENT,
         (case when image.CROSSRA0='Y' THEN abs(image.RACMAX - (image.RACMIN-360)) ELSE abs(image.RACMAX - image.RACMIN) END) as RA_SIZE_CCD,
         abs(image.DECCMAX - image.DECCMIN) as DEC_SIZE_CCD
         FROM {schema:s}image where filetype='red_immask' {bandconstraint:s})
    SELECT
     ima.FILENAME as FILENAME,
     ima.RA_CENT,ima.DEC_CENT,
     ima.BAND as BAND,
     tile.RA_CENT as tra_cent,
     tile.DEC_CENT as tdec_cent,
     ima.RA_SIZE_CCD,ima.DEC_SIZE_CCD
    FROM
     ima, {schema:s}proctag, {schema:s}coaddtile_geom tile
    WHERE
     ima.PFW_ATTEMPT_ID = proctag.PFW_ATTEMPT_ID AND   
     proctag.TAG = '{proctag:s}' AND
     tile.tilename = '{tile:s}' AND
     (ABS(ima.RA_CENT  -  tile.RA_CENT)  < (0.5*tile.RA_SIZE  + 0.5*ima.RA_SIZE_CCD)) AND
     (ABS(ima.DEC_CENT -  tile.DEC_CENT) < (0.5*tile.DEC_SIZE + 0.5*ima.DEC_SIZE_CCD))
     order by ima.RA_CENT
        """.format(schema=dbSchema,bandconstraint=BandConstraint,proctag=ProcTag,tile=CoaddTile)


    query2b="""with ima as
    (SELECT /*+ materialize */
         i.FILENAME, i.FILETYPE,   
         i.CROSSRA0, i.PFW_ATTEMPT_ID,
         i.BAND, i.CCDNUM, i.DEC_CENT,
         (case when i.RA_CENT > 180. THEN i.RA_CENT-360. ELSE i.RA_CENT END) as RA_CENT,
         (case when i.CROSSRA0='Y' THEN abs(i.RACMAX - (i.RACMIN-360)) ELSE abs(i.RACMAX - i.RACMIN) END) as RA_SIZE_CCD,
         abs(i.DECCMAX - i.DECCMIN) as DEC_SIZE_CCD
         FROM {schema:s}image i where i.filetype='red_immask' {bandconstraint:s}),
    tile as (SELECT /*+ materialize */
         t.tilename, t.RA_SIZE, t.DEC_SIZE, t.DEC_CENT,
         (case when (t.CROSSRA0='Y' and t.RA_CENT> 180) THEN t.RA_CENT-360. ELSE t.RA_CENT END) as RA_CENT
         FROM {schema:s}coaddtile_geom t )
    SELECT
         ima.FILENAME,
         ima.RA_CENT,ima.DEC_CENT,
         ima.BAND,
         ima.RA_SIZE_CCD,ima.DEC_SIZE_CCD
    FROM
         ima, {schema:s}proctag t, tile
    WHERE
         ima.PFW_ATTEMPT_ID = t.PFW_ATTEMPT_ID AND   
         t.TAG = '{proctag:s}' AND
         tile.tilename = '{tile:s}' AND
         (ABS(ima.RA_CENT  -  tile.RA_CENT)  < (0.5*tile.RA_SIZE  + 0.5*ima.RA_SIZE_CCD)) AND
         (ABS(ima.DEC_CENT -  tile.DEC_CENT) < (0.5*tile.DEC_SIZE + 0.5*ima.DEC_SIZE_CCD))
     order by ima.BAND
        """.format(schema=dbSchema,bandconstraint=BandConstraint,proctag=ProcTag,tile=CoaddTile)

#
#   First check whether the requested tile crosses RA0
#   Then depending on the result execute query2a or query1b
#
    if (verbose > 0):
        print("# Executing query to determine CROSSRA0 condition for COADDTILE_GEOM.TILE={:s}".format(CoaddTile))
        if (verbose == 1):
            print("# sql = {:s} ".format(" ".join([d.strip() for d in query1.split('\n')])))
        if (verbose > 1):
            print("# sql = {:s}".format(query1))
    curDB=dbh.cursor()
    curDB.execute(query1)
    for row in curDB:
        crossravalue=row[0]


    if (crossravalue == "Y"):
        print("# Executing query (condition CROSSRA0=Y) to obtain red_immask images (based on their extents)")
        if (verbose > 0):
            if (verbose == 1):
                print("# sql = {:s} ".format(" ".join([d.strip() for d in query2b.split('\n')])))
            if (verbose > 1):
                print("# sql = {:s}".format(query2b))
        curDB.execute(query2b)
    else:
        print("# Executing query (condition CROSSRA0=N) to obtain red_immask images (based on their extents)")
        if (verbose > 0):
            if (verbose == 1):
                print("# sql = {:s} ".format(" ".join([d.strip() for d in query2a.split('\n')])))
            if (verbose > 1):
                print("# sql = {:s}".format(query2a))
        curDB.execute(query2a)

    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName=rowd['filename']
        ImgDict[ImgName]=rowd
#
#       Fix any known problematic NoneTypes before they get in the way.
#
#        if (ImgDict[ImgName]['band'] is None):
#            ImgDict[ImgName]['band']='None'
#        if (ImgDict[ImgName]['compression'] is None):
#            ImgDict[ImgName]['compression']=''
#        if (ImgDict[ImgName]['object'] is None):
#            ImgDict[ImgName]['object']='None'
#        if (ImgDict[ImgName]['program'] is None):
#            ImgDict[ImgName]['program']='None'

#    for ImgName in ImgDict:
#        print ImgDict[ImgName]

    return ImgDict


######################################################################################
def query_astref_scampcat(CatDict,CoaddTile,ProcTag,dbh,dbSchema,BandList,verbose=0):
    """ Query code to obtain inputs for COADD Astrorefine step.
        Use an existing DB connection to execute a query for CAT_SCAMP_FULL and 
        HEAD_SCAMP_FULL products from exposures that overlap a specific COADD tile.  
        Return a dictionary of Catalogs and Head files along associated metadata
        (expnum,band,nite).

        Inputs:
            CatDict:   Existing CatDict, new records are added (and possibly old records updated)
            CoaddTile: Name of COADD tile for search
            ProcTag:   Processing Tag used to constrain pool of input images
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            BandList:  List of bands (returned ImgDict list will be restricted to only these bands)
            verbose:   Integer setting level of verbosity when running.

        Returns:
            CatDict:   Updated version of input CatDict
    """

#
#   Pre-assemble constraint based on BandList
#
    BandConstraint=''
    if (len(BandList)>0):
        BandConstraint="and c.band in ('" + "','".join([d.strip() for d in BandList]) + "')"

#
#   Form the query to obtain the cat_scamp_full files.
#
    query="""
        SELECT 
            c.filename as catfile,
            m.filename as headfile,
            m.expnum as expnum,
            m.band as band,
            listagg(d.ccdnum,',') within group (order by d.ccdnum) as ccdnum
        FROM
            {schema:s}miscfile m, {schema:s}catalog c, {schema:s}catalog d, {schema:s}proctag t
        WHERE t.tag='{proctag:s}'
            and t.pfw_attempt_id=c.pfw_attempt_id
            and c.filetype='cat_scamp_full'
            {bandconstraint:s}
            and d.pfw_attempt_id=c.pfw_attempt_id
            and d.filetype='cat_scamp'
            and c.pfw_attempt_id=m.pfw_attempt_id
            and m.filetype='head_scamp_full'
            and m.expnum=c.expnum
            and m.expnum=d.expnum
            and exists (
                SELECT 
                    1
                FROM
                    {schema:s}image i, {schema:s}file_archive_info fai, {schema:s}coaddtile_geom ct
                WHERE i.expnum=c.expnum
                    AND t.PFW_ATTEMPT_ID=i.PFW_ATTEMPT_ID
                    AND i.FILETYPE='red_immask'
                    AND i.FILENAME=fai.FILENAME
                    AND ct.tilename = '{tile:s}'
                    and ((
                            ct.crossra0='N'
                            and ((i.RACMIN between ct.racmin and ct.racmax)or(i.racmax between ct.racmin and ct.racmax))
                            and ((i.deccmin between ct.deccmin and ct.deccmax)or(i.deccmax between ct.deccmin and ct.deccmax))
                        )or(
                            ct.crossra0='Y'
                            and ((i.RACMIN between ct.racmin and 360.)or(i.racmin between 0.0 and ct.racmax)
                            or(i.racmax between ct.racmin and 360.)or(i.racmax between 0.0 and ct.racmax))
                            and ((i.deccmin between ct.deccmin and ct.deccmax)or(i.deccmax between ct.deccmin and ct.deccmax))
                    ))
            )
            group by c.filename,m.filename,m.expnum,m.band """.format(
                schema=dbSchema,
                proctag=ProcTag,
                bandconstraint=BandConstraint,
                tile=CoaddTile)

    if (verbose > 0):
        print("# Executing query to obtain CAT_SCAMP_FULL catalogs")
        if (verbose == 1):
            print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
        if (verbose > 1):
            print("# sql = {:s}".format(query))
    curDB=dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        expnum=rowd['expnum']
        CatDict[expnum]=rowd
#
#       Fix any known problematic NoneTypes before they get in the way.
#
#        if (CatDict[expnum]['band'] is None):
#            CatDict[expnum]['band']='None'

    return CatDict



######################################################################################
def query_astref_catfinalcut(CatDict,CoaddTile,ProcTag,dbh,dbSchema,BandList,verbose=0):
    """ Query code to obtain inputs for COADD Astrorefine step.
        Use an existing DB connection to execute a query for CAT_FINALCUT products
        from exposures that overlap a specific COADD tile.  Return a dictionary 
        of Catalogs along associated metadata (expnum,band,nite).

        Inputs:
            CatDict:   Existing CatDict, new records are added (and possibly old records updated)
            CoaddTile: Name of COADD tile for search
            ProcTag:   Processing Tag used to constrain pool of input images
            curDB:     Database connection to be used
            dbSchema:  Schema over which queries will occur.
            BandList:  List of bands (returned ImgDict list will be restricted to only these bands)
            verbose:   Integer setting level of verbosity when running.

        Returns:
            CatDict:   Updated version of input CatDict
    """

#
#   Pre-assemble constraint based on BandList
#
    BandConstraint=''
    if (len(BandList)>0):
        BandConstraint="and c.band in ('" + "','".join([d.strip() for d in BandList]) + "')"

#
#   Form the query to obtain the cat_finalcut files
#
    query="""
        SELECT 
            c.filename as catfile,
            c.expnum as expnum,
            c.ccdnum as ccdnum,
            c.band as band
        FROM
            {schema:s}catalog c, {schema:s}proctag t
        WHERE t.tag='{proctag:s}'
            and t.pfw_attempt_id=c.pfw_attempt_id
            and c.filetype='cat_finalcut'
            {bandconstraint:s}
            and exists (
                SELECT 
                    1
                FROM
                    {schema:s}image i, {schema:s}coaddtile_geom ct
                WHERE i.expnum=c.expnum
                    AND t.PFW_ATTEMPT_ID=i.PFW_ATTEMPT_ID
                    AND i.FILETYPE='red_immask'
                    AND ct.tilename = '{tile:s}'
                    and ((
                            ct.crossra0='N'
                            and ((i.RACMIN between ct.racmin and ct.racmax)or(i.racmax between ct.racmin and ct.racmax))
                            and ((i.deccmin between ct.deccmin and ct.deccmax)or(i.deccmax between ct.deccmin and ct.deccmax))
                        )or(
                            ct.crossra0='Y'
                            and ((i.RACMIN between ct.racmin and 360.)or(i.racmin between 0.0 and ct.racmax)
                            or(i.racmax between ct.racmin and 360.)or(i.racmax between 0.0 and ct.racmax))
                            and ((i.deccmin between ct.deccmin and ct.deccmax)or(i.deccmax between ct.deccmin and ct.deccmax))
                    ))
            )
            order by c.expnum,c.ccdnum """.format(
                schema=dbSchema,
                proctag=ProcTag,
                bandconstraint=BandConstraint,
                tile=CoaddTile)

    if (verbose > 0):
        print("# Executing query to obtain CAT_FINALCUT catalogs")
        if (verbose == 1):
            print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
        if (verbose > 1):
            print("# sql = {:s}".format(query))
    curDB=dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        catfile=rowd['catfile']
        CatDict[catfile]=rowd
#
#       Fix any known problematic NoneTypes before they get in the way.
#
#        if (CatDict[expnum]['band'] is None):
#            CatDict[expnum]['band']='None'

    return CatDict



######################################################################################
def ImgDict_to_LLD(ImgDict,filetypes,mdatatypes,verbose=0):
    """ Function to convert CatDict into appropriate list of list of dicts suitable for WCL"""

#
#   Note the current means for expressing this list does not have a means to have more than
#   one image/file type on a per record basis (this is partially because compression would need 
#   to be expressed for each image/file type.
#

    OutLLD=[]
    for Img in ImgDict:
        tmplist=[]
        for ftype in filetypes:
            tmpdict={}
            for mdata in mdatatypes[ftype]:
                if (mdata in ImgDict[Img][ftype]):
                    tmpdict[mdata]=ImgDict[Img][ftype][mdata]
                else:
                    if (verbose > 0):
                        print("Warning: missing metadata {:s} for image {:s}".format(mdata,ImgDict[Img][ftype]['filename']))
#            for mdata in mdatatypes:
#                if (mdata in ImgDict[Img]):
#                    tmpdict[mdata]=ImgDict[Img][mdata]
#            print tmpdict
#            tmplist.append(ImgDict[Img][ftype])
            tmplist.append(tmpdict)
        OutLLD.append(tmplist)

#    print OutLLD

    return OutLLD


######################################################################################
def CatDict_to_LLD(CatDict,filetypes,mdatatypes,verbose=0):
    """ Function to convert CatDict into appropriate list of list of dicts suitable for WCL"""

    OutLLD=[]
    for Cat in CatDict:
        tmplist=[]
        for ftype in filetypes:
            tmpdict={}
            tmpdict['filename']=CatDict[Cat][ftype]
            tmpdict['compression']=None
            for mdata in mdatatypes:
                if (mdata in CatDict[Cat]):
                    tmpdict[mdata]=CatDict[Cat][mdata]
            tmplist.append(tmpdict)
        OutLLD.append(tmplist)

    return OutLLD


