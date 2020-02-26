# $Id: coadd_query.py 48310 2019-03-01 16:24:53Z rgruendl $
# $Rev:: 48310                            $:  # Revision of last commit.
# $LastChangedBy:: rgruendl               $:  # Author of last commit.
# $LastCha
"""
A set of queries to obtain inputs for the COADD pipeline.
"""

# Added comment to test git
######################################################################################
def query_coadd_geometry(TileDict, CoaddTile, dbh, dbSchema, verbose=0):
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
    # Query to obtain geometric specification of a specific COADD tile.
    #

    query = f"""SELECT
        t.tilename as tilename,
        t.ra_cent as ra_cent, t.dec_cent as dec_cent,
        t.rac1 as rac1, t.rac2 as rac2, t.rac3 as rac3, t.rac4 as rac4,
        t.decc1 as decc1, t.decc2 as decc2, t.decc3 as decc3, t.decc4 as decc4,
        t.crossra0 as crossra0,
        t.pixelscale as pixelscale, t.naxis1 as naxis1, t.naxis2 as naxis2
        FROM {dbSchema}coaddtile_geom t
        WHERE t.tilename = '{CoaddTile}'
        """

    if verbose > 0:
        if verbose == 1:
            QueryLines = query.split('\n')
            QueryOneLine = 'sql = '
            for line in QueryLines:
                QueryOneLine = QueryOneLine + " " + line.strip()
            print(QueryOneLine)
        if verbose > 1:
            print(query)

    curDB = dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        TileName = rowd['tilename']
        TileDict[TileName] = rowd
    #
    #       Fix any known problematic NoneTypes before they get in the way.
    #
    #        if (ImgDict[ImgName]['band'] is None):
    #            ImgDict[ImgName]['band']='None'

    return TileDict


######################################################################################
def query_coadd_img_by_edges(ImgDict, CoaddTile, ProcTag, BandList, ArchiveSite, dbh, dbSchema, verbose=0):
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
    #BandConstraint = ''
    #if BandList:
    #    BandConstraint = "and i.band in ('" + "','".join([d.strip() for d in BandList]) + "')"

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

    query = f"""SELECT
        fai.filename as filename,
        fai.path as path,
        fai.compression as compression,
        i.band as band,
        i.expnum as expnum,
        i.ccdnum as ccdnum,
        i.rac1 as rac1, i.rac2 as rac2, i.rac3 as rac3, i.rac4 as rac4,
        i.decc1 as decc1, i.decc2 as decc2, i.decc3 as decc3, i.decc4 as decc4
        FROM {dbSchema:s}image i, {dbSchema:s}file_archive_info fai, {dbSchema:s}proctag t, {dbSchema:s}coaddtile_geom ct
        WHERE t.tag='{ProcTag:s}'
        and t.pfw_attempt_id=i.pfw_attempt_id
        and i.filetype='red_immask'
        and i.filename=fai.filename
        and fai.archive_name='{ArchiveSite:s}'
        and ct.tilename = '{CoaddTile:s}'
        and ((ct.crossra0='N'
        AND ((i.racmin between ct.racmin and ct.racmax)OR(i.racmax between ct.racmin and ct.racmax))
        AND ((i.deccmin between ct.deccmin and ct.deccmax)OR(i.deccmax between ct.deccmin and ct.deccmax))
        )OR(ct.crossra0='Y'
        AND ((i.racmin between ct.racmin and 360.)OR(i.racmin between 0.0 and ct.racmax)
        OR(i.racmax between ct.racmin and 360.)OR(i.racmax between 0.0 and ct.racmax))
        AND ((i.deccmin between ct.deccmin and ct.deccmax)OR(i.deccmax between ct.deccmin and ct.deccmax))
        ))"""

    if verbose > 0:
        print("# Executing query to obtain red_immask images (based on their edges/boundaries)")
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB = dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        #        ImgName=rowd['filename']
        #        ImgDict[ImgName]=rowd
        if rowd['band'] in BandList:
            ImgName = rowd['filename']
            ImgDict[ImgName] = rowd
            #            ImgList.append([ImgName])
        else:
            if verbose > 1:
                print(f" Post query constraint removed {rowd['band']:s}-band image: {rowd['filename']:s} ")

    return ImgDict


######################################################################################
def query_coadd_img_by_fiat(ImgDict, CoaddTile, ProcTag, BandList, ArchiveSite, FiatTable, dbh,
                            dbSchema, verbose=0):
    """ Query code to obtain image inputs for COADD (based on a pre-constructed
        mapping of filenames to tilenames).  THIS IS A MASSIVE CHEAT AND ASSSUMES
        THAT ALL WORK HAS BEEN DONE AHEAD OF TIME.  IT IS VERY SUCCEPTIBLE TO
        CHANGES IN THE INPUT LIST OF IMAGES AND/OR TILES>

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
    #BandConstraint = ''
    #if BandList:
    #    BandConstraint = "and i.band in ('" + "','".join([d.strip() for d in BandList]) + "')"
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

    query = f"""SELECT
        fai.filename as filename,
        fai.path as path,
        fai.compression as compression,
        i.band as band,
        i.expnum as expnum,
        i.ccdnum as ccdnum,
        i.rac1 as rac1, i.rac2 as rac2, i.rac3 as rac3, i.rac4 as rac4,
        i.decc1 as decc1, i.decc2 as decc2, i.decc3 as decc3, i.decc4 as decc4
        FROM {dbSchema:s}image i, {dbSchema:s}file_archive_info fai, {dbSchema:s}proctag t, {FiatTable:s} y
        WHERE t.tag='{ProcTag:s}'
        and t.pfw_attempt_id=i.pfw_attempt_id
        and i.filetype='red_immask'
        and i.filename=fai.filename
        and fai.archive_name='{ArchiveSite:s}'
        and i.filename=y.filename
        and y.tilename='{CoaddTile:s}'
        """

    if verbose > 0:
        print("# Executing query to obtain red_immask images (based on their edges/boundaries)")
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB = dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        #        ImgName=rowd['filename']
        #        ImgDict[ImgName]=rowd
        if rowd['band'] in BandList:
            ImgName = rowd['filename']
            ImgDict[ImgName] = rowd
            #            ImgList.append([ImgName])
        else:
            if verbose > 1:
                print(f" Post query constraint removed {rowd['band']:s}-band image: {rowd['filename']:s} ")

    return ImgDict



######################################################################################
def query_coadd_img_from_attempt(ImgDict, attemptID, BandList, ArchiveSite, dbh, dbSchema, verbose=0):
    """ Query code to obtain image inputs for COADD (based on a previous successful
        mulitepoch/COADD attempt.

        Use an existing DB connection to execute a query for RED_IMMASK image
        products that were found previously to overlap a specific COADD tile.
        Return a dictionary of Images along with with basic properties (expnum,
        ccdnum, band, nite).

        Inputs:
            ImgDict:   Existing ImgDict, new records are added (and possibly old records updated)
            attemptID: Exisitng (completed) attempt
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
    #BandConstraint = ''
    #if BandList:
    #    BandConstraint = "and i.band in ('" + "','".join([d.strip() for d in BandList]) + "')"
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

    query = f"""SELECT
        fai.filename as filename,
        fai.path as path,
        fai.compression as compression,
        i.band as band,
        i.expnum as expnum,
        i.ccdnum as ccdnum,
        i.rac1 as rac1, i.rac2 as rac2, i.rac3 as rac3, i.rac4 as rac4,
        i.decc1 as decc1, i.decc2 as decc2, i.decc3 as decc3, i.decc4 as decc4
        FROM {dbSchema:s}image i, {dbSchema:s}desfile d, {dbSchema:s}desfile d2, {dbSchema:s}opm_was_derived_from wdf, {dbSchema:s}file_archive_info fai
        WHERE d.pfw_attempt_id={attemptID:d}
        and d.filetype='coadd_nwgint'
        and d.id=wdf.child_desfile_id
        and wdf.parent_desfile_id=d2.id
        and d2.filetype='red_immask'
        and d2.filename=i.filename
        and d2.id=fai.desfile_id
        and fai.archive_name='{ArchiveSite:s}'
        """

    if verbose > 0:
        print("# Executing query to obtain red_immask images (based on their edges/boundaries)")
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB = dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        #        ImgName=rowd['filename']
        #        ImgDict[ImgName]=rowd
        if rowd['band'] in BandList:
            ImgName = rowd['filename']
            ImgDict[ImgName] = rowd
            #            ImgList.append([ImgName])
        else:
            if verbose > 1:
                print(f" Post query constraint removed {rowd['band']:s}-band image: {rowd['filename']:s} ")

    return ImgDict


######################################################################################
def query_zeropoint(ImgDict, ZptInfo, ZptSecondary, dbh, dbSchema, verbose=0):
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
            ZptSecondary: Dictionary containing information about Secondary Zeropoint Query/Constraint
                        (NoneType yields no seconady query/constraint)
                            ZptSecondary['table']:   provides table to use
                            ZptSecondary['source']:  Zpt source constraint
                            ZptSecondary['version']: Zpt version constraint
                            ZptSecondary['flag']:    Zpt flag constraint
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            ImgDict:   Updated version of input ImgDict
    """
    #
    #   Pre-assemble portions of query that pertain to ZEROPOINT (no constraint will output mag_zero=30 for all exposures)
    #
    ZptData = ''
    ZptTable = ''
    ZptConstraint = ''
    if ZptInfo is not None:
        ZptData = 'z.mag_zero as mag_zero,'
        ZptTable = f", {ZptInfo['table']} z"

        ZptSrcConstraint = ''
        if 'source' in ZptInfo:
            ZptSrcConstraint = f"and z.source='{ZptInfo['source']}'"

        ZptVerConstraint = ''
        if 'version' in ZptInfo:
            ZptVerConstraint = f"and z.version='{ZptInfo['version']}'"

        ZptFlagConstraint = ''
        if 'flag' in ZptInfo:
            ZptFlagConstraint = f"and z.flag<{ZptInfo['flag']:s}"

        ZptConstraint = f"""and z.imagename=i.filename and z.mag_zero>-100. {ZptSrcConstraint} {ZptVerConstraint} {ZptFlagConstraint}"""

    #
    #   Prepare GTT_FILENAME table with list of possible inputs
    #
    ImgList = []
    NewImgDict = {}
    for ImgName in ImgDict:
        if 'mag_zero' not in ImgDict[ImgName]:
            ImgList.append([ImgName])
        else:
            if ImgDict[ImgName]['mag_zero'] is None:
                ImgList.append([ImgName])
            else:
                NewImgDict[ImgName] = ImgDict[ImgName]

    # Make sure the GTT_FILENAME table is empty
    curDB = dbh.cursor()
    curDB.execute('delete from GTT_FILENAME')
    # load img ids into opm_filename_gtt table
    print(f"# Loading GTT_FILENAME table for secondary queries with entries for {len(ImgList):d} images")
    dbh.insert_many('GTT_FILENAME', ['FILENAME'], ImgList)
    #
    #   Query to obtain zeropoints
    #
    query = f"""SELECT
        gtt.filename as filename,
        {ZptData:s}
        i.expnum as expnum,
        i.ccdnum as ccdnum
        FROM {dbSchema:s}image i, gtt_filename gtt{ZptTable:s}
        WHERE i.filename=gtt.filename
        {ZptConstraint:s}
        """

    if verbose > 0:
        print("# Executing query to obtain ZEROPOINTs corresponding to the red_immasked images")
        if verbose == 1:
            print(f"# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    #
    #   New image dictionary is updated (so that images with zeropoints will be returned)

    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName = rowd['filename']
        NewImgDict[ImgName] = ImgDict[ImgName]
        NewImgDict[ImgName]['mag_zero'] = rowd['mag_zero']

    #
    #   Secondary zeropoint query
    #
    if ZptSecondary is not None:
        ZptData = 'z.mag_zero as mag_zero,'
        ZptSrcConstraint = ''
        if 'source' in ZptSecondary:
            ZptSrcConstraint = f"and z.source='{ZptSecondary['source']}'"

        ZptVerConstraint = ''
        if 'version' in ZptSecondary:
            ZptVerConstraint = f"and z.version='{ZptSecondary['version']}'"

        ZptFlagConstraint = ''
        if 'flag' in ZptSecondary:
            ZptFlagConstraint = f"and z.flag<{ZptSecondary['flag']:s}"
        ZptConstraint = f"""and z.imagename=i.filename and z.mag_zero>-100. {ZptSrcConstraint} {ZptVerConstraint} {ZptFlagConstraint}"""

        #
        #       Prepare GTT_FILENAME table with list of possible inputs
        #       Note this could be re-instated and only query over those images that do not yet have a zeropoint.
        #
        ImgList = []
        for ImgName in ImgDict:
            if ImgName not in NewImgDict:
                ImgList.append([ImgName])
        # Make sure the GTT_FILENAME table is empty
        curDB = dbh.cursor()
        curDB.execute('delete from GTT_FILENAME')
        # load img ids into opm_filename_gtt table
        print(f"# Loading GTT_FILENAME table for secondary queries with entries for {len(ImgList):d} images")
        dbh.insert_many('GTT_FILENAME', ['FILENAME'], ImgList)
        curDB.execute('select count(*) from gtt_filename')
        for row in curDB:
            print("GTT_FILENAME check found ", row, " rows.")
        #
        #       Query to obtain zeropoints
        #
        query = f"""SELECT
            gtt.filename as filename,
            {ZptData:s}
            i.expnum as expnum,
            i.ccdnum as ccdnum
            FROM {dbSchema:s}image i, gtt_filename gtt{ZptTable:s}
            WHERE i.filename=gtt.filename
            {ZptConstraint:s}
            """

        if verbose > 0:
            print("# Executing query to obtain SECONDARY ZPTs corresponding to the red_immasked images")
            if verbose == 1:
                print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
            else:
                print(f"# sql = {query:s}")
        curDB.execute(query)
        desc = [d[0].lower() for d in curDB.description]

        for row in curDB:
            rowd = dict(zip(desc, row))
            ImgName = rowd['filename']
            if ImgName in NewImgDict:
                print(f" Secondary ZPT query has identified a ZPT for and existing record.  Ignoring Seconday ZPT for (ImgName={ImgName:s} ")
            else:
                if ImgName in ImgDict:
                    NewImgDict[ImgName] = ImgDict[ImgName]
                    if 'mag_zero' in rowd:
                        NewImgDict[ImgName]['mag_zero'] = rowd['mag_zero']
                        #   NewImgList.append([ImgName])
                else:
                    if verbose > 2:
                        print(f" No matching record? in query for zeropoint for (ImgName={ImgName:s} ")

    ImgDict = NewImgDict

    return ImgDict


######################################################################################
def query_blacklist(ImgDict, BlacklistInfo, dbh, dbSchema, verbose=0):
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
    BlacklistConstraint = ''
    if BlacklistInfo is not None:
        BlacklistConstraint = f"""and not exists (select bl.reason from {BlacklistInfo['table']} bl where bl.expnum=i.expnum and bl.ccdnum=i.ccdnum)"""

    #
    #   Prepare GTT_FILENAME table with list of possible inputs
    #
    ImgList = []
    for ImgName in ImgDict:
        ImgList.append([ImgName])

    # Make sure the GTT_FILENAME table is empty
    curDB = dbh.cursor()
    curDB.execute('delete from GTT_FILENAME')
    # load img ids into opm_filename_gtt table
    print(f"# Loading GTT_FILENAME table for secondary queries with entries for {len(ImgList):d} images")
    dbh.insert_many('GTT_FILENAME', ['FILENAME'], ImgList)
    #
    #   Query to remove blacklisted exposures/images.
    #
    query = f"""SELECT
        gtt.filename as filename,
        i.expnum as expnum,
        i.ccdnum as ccdnum
        FROM {dbSchema:s}image i, gtt_filename gtt
        WHERE i.filename=gtt.filename
        {BlacklistConstraint:s}
        """

    if verbose > 0:
        print("# Executing query to remove blacklisted images")
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    #
    #   New image dictionary is formed so that images that do not return in this query are not returned
    #
    NewImgDict = {}
    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName = rowd['filename']
        if ImgName in ImgDict:
            NewImgDict[ImgName] = ImgDict[ImgName]

    if verbose > 0:
        print(f"# Blacklist removed {(len(ImgDict) - len(NewImgDict)):d} of {len(ImgDict):d} images.")

    ImgDict = NewImgDict

    return ImgDict



######################################################################################
def query_bkg_img(ImgDict, ArchiveSite, dbh, dbSchema, verbose=0):
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
    ImgList = []
    for ImgName in ImgDict:
        ImgList.append([ImgName])

    #   Setup DB cursor
    curDB = dbh.cursor()
    #   Make sure teh GTT_FILENAME table is empty
    curDB.execute('delete from GTT_FILENAME')
    #   Load img ids into opm_filename_gtt table
    print(f"# Loading GTT_FILENAME table for secondary queries with entries for {len(ImgList):d} images")
    dbh.insert_many('GTT_FILENAME', ['FILENAME'], ImgList)

    #
    #   Obtain associated bkgd image (red_bkg).
    #
    query = f"""SELECT
        i.filename as redfile,
        fai.filename as filename,
        fai.path as path,
        fai.compression as compression,
        k.band as band,
        k.expnum as expnum,
        k.ccdnum as ccdnum
        FROM {dbSchema:s}image i, {dbSchema:s}image k, {dbSchema:s}file_archive_info fai, GTT_FILENAME gtt
        WHERE i.filename=gtt.filename
        and i.pfw_attempt_id=k.pfw_attempt_id
        and k.filetype='red_bkg'
        and i.ccdnum=k.ccdnum
        and k.filename=fai.filename
        and fai.archive_name='{ArchiveSite:s}'
        """

    if verbose > 0:
        print("# Executing query to obtain red_bkg images corresponding to the red_immasked images")
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    BkgDict = {}
    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName = rowd['redfile']
        #        BkgName=rowd['filename']
        BkgDict[ImgName] = rowd
        #        if ('mag_zero' in ImgDict[ImgName]):
        #            BkgDict[ImgName]['mag_zero']=ImgDict[ImgName]['mag_zero']

        #            ImgDict[ImgName]['skyfilename']=rowd['skyfilename']
        #            ImgDict[ImgName]['skycompress']=rowd['skycompress']
        #        else:
        #            if (verbose > 2):
        #                print(" No matching record? in query for skyfilename for (ImgName={:s} ".format(ImgName))

        #    print ImgList
        #    print BkgDict
    return BkgDict



######################################################################################
def query_segmap(ImgDict, ArchiveSite, dbh, dbSchema, verbose=0):
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
    ImgList = []
    for ImgName in ImgDict:
        ImgList.append([ImgName])

    #   Setup DB cursor
    curDB = dbh.cursor()
    #   Make sure teh GTT_FILENAME table is empty
    curDB.execute('delete from GTT_FILENAME')
    #   load img ids into opm_filename_gtt table
    print(f"# Loading GTT_FILENAME table for secondary queries with entries for {len(ImgList):d} images")
    dbh.insert_many('GTT_FILENAME', ['FILENAME'], ImgList)
    #
    #   Obtain associated segmentation map image (red_segmap).
    #
    query = f"""SELECT
        i.filename as redfile,
        fai.filename as filename,
        fai.path as path,
        fai.compression as compression,
        m.band as band,
        m.expnum as expnum,
        m.ccdnum as ccdnum
        FROM {dbSchema:s}image i, {dbSchema:s}miscfile m, {dbSchema:s}file_archive_info fai, GTT_FILENAME gtt
        WHERE i.filename=gtt.filename
        and i.pfw_attempt_id=m.pfw_attempt_id
        and m.filetype='red_segmap'
        and i.ccdnum=m.ccdnum
        and m.filename=fai.filename
        and fai.archive_name='{ArchiveSite:s}'
        """

    if verbose > 0:
        print("# Executing query to obtain segmentation map images corresponding to the red_immasked images")
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    SegDict = {}
    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName = rowd['redfile']
        SegDict[ImgName] = rowd
        #        if ('mag_zero' in ImgDict[ImgName]):
        #            SegDict[ImgName]['mag_zero']=ImgDict[ImgName]['mag_zero']

    return SegDict



######################################################################################
def query_psfmodel(ImgDict, ArchiveSite, dbh, dbSchema, verbose=0):
    """ Query code to obtain PSF Models (psfex_model) associated with a set of red_immask images.
        Use an existing DB connection to execute a query to obtain PSFEX_MODEL files
        for an existing set of images.

        Inputs:
            ImgDict:    Existing ImgDict
            ArchiveSite: Archive_name
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            PsfDict:   Ouput dictionary of PSF Model files
    """
    #
    #   Prepare GTT_FILENAME table with list of possible inputs
    #
    ImgList = []
    for ImgName in ImgDict:
        ImgList.append([ImgName])

    #   Setup DB cursor
    curDB = dbh.cursor()
    #   Make sure teh GTT_FILENAME table is empty
    curDB.execute('delete from GTT_FILENAME')
    #   load img ids into opm_filename_gtt table
    print(f"# Loading GTT_FILENAME table for secondary queries with entries for {len(ImgList):d} images")
    dbh.insert_many('GTT_FILENAME', ['FILENAME'], ImgList)
    #
    #   Obtain associated PSF model (psfex_model).
    #
    query = f"""SELECT
        i.filename as redfile,
        fai.filename as filename,
        fai.path as path,
        fai.compression as compression,
        m.band as band,
        m.expnum as expnum,
        m.ccdnum as ccdnum
        FROM {dbSchema:s}image i, {dbSchema:s}miscfile m, {dbSchema:s}file_archive_info fai, GTT_FILENAME gtt
        WHERE i.filename=gtt.filename
        and i.pfw_attempt_id=m.pfw_attempt_id
        and m.filetype='psfex_model'
        and i.ccdnum=m.ccdnum
        and m.filename=fai.filename
        and fai.archive_name='{ArchiveSite:s}'
        """

    if verbose > 0:
        print("# Executing query to obtain PSFex models corresponding to the red_immasked images")
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    PsfDict = {}
    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName = rowd['redfile']
        PsfDict[ImgName] = rowd

    return PsfDict


######################################################################################
def query_PIFFmodel(ImgDict, ArchiveSite, dbh, dbSchema, PIFFtag, verbose=0):
    """ Query code to obtain PSF Models (PIFF) associated with a set of red_immask images.
        Use an existing DB connection to execute a query to obtain PIFF PSF model files
        for an existing set of images.

        Inputs:
            ImgDict:    Existing ImgDict
            ArchiveSite: Archive_name
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            PIFFtag:   Proctag that defines a specific afterburner PIFF run.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            PsfDict:   Ouput dictionary of PSF Model files
    """
        #
        #   Prepare GTT_FILENAME table with list of possible inputs
        #
    ImgList = []
    for ImgName in ImgDict:
        ImgList.append([ImgName])

        #   Setup DB cursor
    curDB = dbh.cursor()
        #   Make sure teh GTT_FILENAME table is empty
    curDB.execute("delete from GTT_FILENAME")
        #   load img ids into opm_filename_gtt table
    print(f"# Loading GTT_FILENAME table for secondary queries with entries for {len(ImgList):d} images")
    dbh.insert_many('GTT_FILENAME', ['FILENAME'], ImgList)
#
#   Obtain associated PSF model (psfex_model).
#
    query = f"""SELECT
        d2.filename as redfile,
        fai.filename as filename,
        fai.path as path,
        fai.compression as compression,
        m.band as band,
        m.expnum as expnum,
        m.ccdnum as ccdnum
        FROM {dbSchema:s}desfile d1, {dbSchema:s}desfile d2, {dbSchema:s}proctag t, {dbSchema:s}opm_was_derived_from wdf, {dbSchema:s}miscfile m, {dbSchema:s}file_archive_info fai, GTT_FILENAME gtt
        WHERE d2.filename=gtt.filename
        and d2.id=wdf.parent_desfile_id
        and wdf.child_desfile_id=d1.id
        and d1.filetype='piff_model'
        and d1.pfw_attempt_id=t.pfw_attempt_id
        and t.tag='{PIFFtag:s}'
        and d1.filename=m.filename
        and d1.id=fai.desfile_id
        and fai.archive_name='{ArchiveSite:s}'
        """

    if verbose > 0:
        print("# Executing query to obtain PIFF PSF models corresponding to the red_immasked images from PROCTAG.TAG={:s}".format(PIFFtag))
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    PsfDict = {}
    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName = rowd['redfile']
        PsfDict[ImgName] = rowd

    return PsfDict


######################################################################################
def query_headfile_from_attempt(ImgDict, attemptID, ArchiveSite, dbh, dbSchema, verbose=0):
    """ Query code to obtain headfiles from a previous multiepoch/COADD attempt
        that are associated with a set of red_immask images.
        Use an existing DB connection to execute a query to obtain coadd_head_scamp files
        for an existing set of images.

        Inputs:
            ImgDict:    Existing ImgDict
            attemptID:  Attempt ID from previous run that gives a specific set of head files
            ArchiveSite: Archive_name
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            HeadDict:   Ouput dictionary of head files.
    """
    #
    #   Prepare GTT_FILENAME table with list of possible inputs
    #
    ImgList = []
    for ImgName in ImgDict:
        ImgList.append([ImgName])

    #   Setup DB cursor
    curDB = dbh.cursor()
    #   Make sure teh GTT_FILENAME table is empty
    curDB.execute('delete from GTT_FILENAME')
    #   load img ids into opm_filename_gtt table
    print(f"# Loading GTT_FILENAME table for secondary queries with entries for {len(ImgList):d} images")
    dbh.insert_many('GTT_FILENAME', ['FILENAME'], ImgList)
    #
    #   Obtain associated segmentation map image (red_segmap).
    #
    query = f"""SELECT
        i.filename as redfile,
        fai.filename as filename,
        fai.path as path,
        fai.compression as compression,
        m.band as band,
        m.expnum as expnum,
        m.ccdnum as ccdnum
        FROM {dbSchema:s}image i, {dbSchema:s}miscfile m, {dbSchema:s}file_archive_info fai, GTT_FILENAME gtt
        WHERE i.filename=gtt.filename
        and m.pfw_attempt_id={attemptID:d}
        and m.filetype='coadd_head_scamp'
        and i.ccdnum=m.ccdnum
        and i.expnum=m.expnum
        and m.filename=fai.filename
        and fai.archive_name='{ArchiveSite:s}'
        """

    if verbose > 0:
        print("# Executing query to obtain head files corresponding to the red_immasked images")
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    HeadDict = {}
    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName = rowd['redfile']
        HeadDict[ImgName] = rowd

    return HeadDict


######################################################################################
def query_catfinalcut(ImgDict, ArchiveSite, dbh, dbSchema, verbose=0):
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
    ImgList = []
    for ImgName in ImgDict:
        ImgList.append([ImgName])

    #   Setup DB cursor
    curDB = dbh.cursor()
    #   Make sure teh GTT_FILENAME table is empty
    curDB.execute('delete from GTT_FILENAME')
    #   load img ids into opm_filename_gtt table
    print(f"# Loading GTT_FILENAME table for secondary queries with entries for {len(ImgList):d} images")
    dbh.insert_many('GTT_FILENAME', ['FILENAME'], ImgList)
    #
    #   Obtain associated catlogs (cat_finalcut).
    #
    query = f"""SELECT
        i.filename as redfile,
        fai.filename as filename,
        fai.path as path,
        fai.compression as compression,
        c.band as band,
        c.expnum as expnum,
        c.ccdnum as ccdnum
        FROM {dbSchema:s}image i, {dbSchema:s}catalog c, {dbSchema:s}file_archive_info fai, GTT_FILENAME gtt
        WHERE i.filename=gtt.filename
        and i.pfw_attempt_id=c.pfw_attempt_id
        and c.filetype='cat_finalcut'
        and i.ccdnum=c.ccdnum
        and c.filename=fai.filename
        and fai.archive_name='{ArchiveSite:s}'
        """

    if verbose > 0:
        print("# Executing query to obtain cat_finalcut catalogs corresponding to the red_immasked images")
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    CatDict = {}
    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName = rowd['redfile']
        CatDict[ImgName] = rowd
        if 'mag_zero' in ImgDict[ImgName]:
            CatDict[ImgName]['mag_zero'] = ImgDict[ImgName]['mag_zero']

    return CatDict


######################################################################################
def query_coadd_img_by_extent(ImgDict, CoaddTile, ProcTag, dbh, dbSchema, BandList, verbose=0):
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
    BandConstraint = ''
    if BandList:
        BandConstraint = "and band in ('" + "','".join([d.strip() for d in BandList]) + "')"

    #
    #   Prepare queries used to find images that correspond to a tile (based on their extents)
    #

    query1 = f"""select crossra0 from {dbSchema:s}coaddtile_geom where tilename='{CoaddTile:s}'"""

    query2a = f"""with ima as
        (SELECT /*+ materialize */
        FILENAME, FILETYPE, CROSSRA0, PFW_ATTEMPT_ID, BAND, CCDNUM,
        RA_CENT, DEC_CENT,
        (case when image.CROSSRA0='Y' THEN abs(image.RACMAX - (image.RACMIN-360)) ELSE abs(image.RACMAX - image.RACMIN) END) as RA_SIZE_CCD,
        abs(image.DECCMAX - image.DECCMIN) as DEC_SIZE_CCD
        FROM {dbSchema:s}image where filetype='red_immask' {BandConstraint:s})
        SELECT
        ima.FILENAME as FILENAME,
        ima.RA_CENT,ima.DEC_CENT,
        ima.BAND as BAND,
        tile.RA_CENT as tra_cent,
        tile.DEC_CENT as tdec_cent,
        ima.RA_SIZE_CCD,ima.DEC_SIZE_CCD
        FROM
        ima, {dbSchema:s}proctag, {dbSchema:s}coaddtile_geom tile
        WHERE
        ima.PFW_ATTEMPT_ID = proctag.PFW_ATTEMPT_ID AND
        proctag.TAG = '{ProcTag:s}' AND
        tile.tilename = '{CoaddTile:s}' AND
        (ABS(ima.RA_CENT  -  tile.RA_CENT)  < (0.5*tile.RA_SIZE  + 0.5*ima.RA_SIZE_CCD)) AND
        (ABS(ima.DEC_CENT -  tile.DEC_CENT) < (0.5*tile.DEC_SIZE + 0.5*ima.DEC_SIZE_CCD))
        order by ima.RA_CENT
        """


    query2b = f"""with ima as
        (SELECT /*+ materialize */
        i.FILENAME, i.FILETYPE,
        i.CROSSRA0, i.PFW_ATTEMPT_ID,
        i.BAND, i.CCDNUM, i.DEC_CENT,
        (case when i.RA_CENT > 180. THEN i.RA_CENT-360. ELSE i.RA_CENT END) as RA_CENT,
        (case when i.CROSSRA0='Y' THEN abs(i.RACMAX - (i.RACMIN-360)) ELSE abs(i.RACMAX - i.RACMIN) END) as RA_SIZE_CCD,
        abs(i.DECCMAX - i.DECCMIN) as DEC_SIZE_CCD
        FROM {dbSchema:s}image i where i.filetype='red_immask' {BandConstraint:s}),
        tile as (SELECT /*+ materialize */
        t.tilename, t.RA_SIZE, t.DEC_SIZE, t.DEC_CENT,
        (case when (t.CROSSRA0='Y' and t.RA_CENT> 180) THEN t.RA_CENT-360. ELSE t.RA_CENT END) as RA_CENT
        FROM {dbSchema:s}coaddtile_geom t )
        SELECT
        ima.FILENAME,
        ima.RA_CENT,ima.DEC_CENT,
        ima.BAND,
        ima.RA_SIZE_CCD,ima.DEC_SIZE_CCD
        FROM
        ima, {dbSchema:s}proctag t, tile
        WHERE
        ima.PFW_ATTEMPT_ID = t.PFW_ATTEMPT_ID AND
        t.TAG = '{ProcTag:s}' AND
        tile.tilename = '{CoaddTile:s}' AND
        (ABS(ima.RA_CENT  -  tile.RA_CENT)  < (0.5*tile.RA_SIZE  + 0.5*ima.RA_SIZE_CCD)) AND
        (ABS(ima.DEC_CENT -  tile.DEC_CENT) < (0.5*tile.DEC_SIZE + 0.5*ima.DEC_SIZE_CCD))
        order by ima.BAND
        """

    #
    #   First check whether the requested tile crosses RA0
    #   Then depending on the result execute query2a or query1b
    #
    if verbose > 0:
        print(f"# Executing query to determine CROSSRA0 condition for COADDTILE_GEOM.TILE={CoaddTile:s}")
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query1.split('\n')]))
        else:
            print(f"# sql = {query1:s}")
    curDB = dbh.cursor()
    curDB.execute(query1)
    for row in curDB:
        crossravalue = row[0]

    if crossravalue == "Y":
        print("# Executing query (condition CROSSRA0=Y) to obtain red_immask images (based on their extents)")
        if verbose > 0:
            if verbose == 1:
                print("# sql = " + " ".join([d.strip() for d in query2b.split('\n')]))
            else:
                print(f"# sql = {query2b:s}")
        curDB.execute(query2b)
    else:
        print("# Executing query (condition CROSSRA0=N) to obtain red_immask images (based on their extents)")
        if verbose > 0:
            if verbose == 1:
                print("# sql = " + " ".join([d.strip() for d in query2a.split('\n')]))
            else:
                print(f"# sql = {query2a:s}")
        curDB.execute(query2a)

    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName = rowd['filename']
        ImgDict[ImgName] = rowd
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
def query_astref_scampcat(CatDict, CoaddTile, ProcTag, dbh, dbSchema, BandList, verbose=0):
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
    BandConstraint = ''
    if BandList:
        BandConstraint = "and c.band in ('" + "','".join([d.strip() for d in BandList]) + "')"

    #
    #   Form the query to obtain the cat_scamp_full files.
    #
    query = f"""
        SELECT
        c.filename as catfile,
        m.filename as headfile,
        m.expnum as expnum,
        m.band as band,
        listagg(d.ccdnum,',') within group (order by d.ccdnum) as ccdnum
        FROM
        {dbSchema:s}miscfile m, {dbSchema:s}catalog c, {dbSchema:s}catalog d, {dbSchema:s}proctag t
        WHERE t.tag='{ProcTag:s}'
        and t.pfw_attempt_id=c.pfw_attempt_id
        and c.filetype='cat_scamp_full'
        {BandConstraint:s}
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
        {dbSchema:s}image i, {dbSchema:s}file_archive_info fai, {dbSchema:s}coaddtile_geom ct
        WHERE i.expnum=c.expnum
        AND t.PFW_ATTEMPT_ID=i.PFW_ATTEMPT_ID
        AND i.FILETYPE='red_immask'
        AND i.FILENAME=fai.FILENAME
        AND ct.tilename = '{CoaddTile:s}'
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
        group by c.filename,m.filename,m.expnum,m.band """

    if verbose > 0:
        print("# Executing query to obtain CAT_SCAMP_FULL catalogs")
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB = dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        expnum = rowd['expnum']
        CatDict[expnum] = rowd
    #
    #       Fix any known problematic NoneTypes before they get in the way.
    #
    #        if (CatDict[expnum]['band'] is None):
    #            CatDict[expnum]['band']='None'

    return CatDict


######################################################################################
def query_astref_scampcat_by_fiat(CatDict, CoaddTile, ProcTag, dbh, dbSchema, BandList, FiatTable, verbose=0):
    """ Query code to obtain inputs for COADD Astrorefine step.
        Use an existing DB connection to execute a query for CAT_SCAMP_FULL and
        HEAD_SCAMP_FULL products from exposures that overlap a specific COADD tile.
        Return a dictionary of Catalogs and Head files along associated metadata
        (expnum,band,nite).

        NOTE: This version uses A MASSIVE CHEAT (compared to query_astref_catfinalcut).  It uses
            a table with predefined correspondence between IMAGEs and TILEs.  It is very succeptible
            to changes in the input list that formed that predefined table.

        Inputs:
            CatDict:   Existing CatDict, new records are added (and possibly old records updated)
            CoaddTile: Name of COADD tile for search
            ProcTag:   Processing Tag used to constrain pool of input images
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            BandList:  List of bands (returned ImgDict list will be restricted to only these bands)
            FiatTable: Predefined table showing correspondence between IMAGEs and TILEs.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            CatDict:   Updated version of input CatDict
    """
    #
    #   Pre-assemble constraint based on BandList
    #
    BandConstraint = ''
    if BandList:
        BandConstraint = "and c.band in ('" + "','".join([d.strip() for d in BandList]) + "')"

    #
    #   Form the query to obtain the cat_scamp_full files.
    #
    query = f"""
        SELECT
        c.filename as catfile,
        m.filename as headfile,
        m.expnum as expnum,
        m.band as band,
        listagg(d.ccdnum,',') within group (order by d.ccdnum) as ccdnum
        FROM
        {dbSchema:s}miscfile m, {dbSchema:s}catalog c, {dbSchema:s}catalog d, {dbSchema:s}proctag t
        WHERE t.tag='{ProcTag:s}'
        and t.pfw_attempt_id=c.pfw_attempt_id
        and c.filetype='cat_scamp_full'
        {BandConstraint:s}
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
        {dbSchema:s}image i, {FiatTable:s} y
        WHERE i.expnum=c.expnum
        and t.pfw_attempt_id=i.pfw_attempt_id
        and i.filetype='red_immask'
        and i.filename=y.filename
        and y.tilename='{CoaddTile:s}'
        )
        group by c.filename,m.filename,m.expnum,m.band """

    if verbose > 0:
        print("# Executing query to obtain CAT_SCAMP_FULL catalogs")
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB = dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        expnum = rowd['expnum']
        CatDict[expnum] = rowd
    #
    #       Fix any known problematic NoneTypes before they get in the way.
    #
    #        if (CatDict[expnum]['band'] is None):
    #            CatDict[expnum]['band']='None'

    return CatDict


######################################################################################
def query_astref_catfinalcut(CatDict, CoaddTile, ProcTag, dbh, dbSchema, BandList, verbose=0):
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
    BandConstraint = ''
    if BandList:
        BandConstraint = "and c.band in ('" + "','".join([d.strip() for d in BandList]) + "')"

    #
    #   Form the query to obtain the cat_finalcut files
    #
    query = f"""
        SELECT
        c.filename as catfile,
        c.expnum as expnum,
        c.ccdnum as ccdnum,
        c.band as band
        FROM
        {dbSchema:s}catalog c, {dbSchema:s}proctag t
        WHERE t.tag='{ProcTag:s}'
        and t.pfw_attempt_id=c.pfw_attempt_id
        and c.filetype='cat_finalcut'
        {BandConstraint:s}
        and exists (
        SELECT
        1
        FROM
        {dbSchema:s}image i, {dbSchema:s}coaddtile_geom ct
        WHERE i.expnum=c.expnum
        AND t.PFW_ATTEMPT_ID=i.PFW_ATTEMPT_ID
        AND i.FILETYPE='red_immask'
        AND ct.tilename = '{CoaddTile:s}'
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
        order by c.expnum,c.ccdnum """

    if verbose > 0:
        print("# Executing query to obtain CAT_FINALCUT catalogs")
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB = dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        catfile = rowd['catfile']
        CatDict[catfile] = rowd
#
#       Fix any known problematic NoneTypes before they get in the way.
#
#        if (CatDict[expnum]['band'] is None):
#            CatDict[expnum]['band']='None'

    return CatDict


######################################################################################
def query_astref_catfinalcut_by_fiat(CatDict, CoaddTile, ProcTag, dbh, dbSchema, BandList, FiatTable,
                                     verbose=0):
    """ Query code to obtain inputs for COADD Astrorefine step.
        Use an existing DB connection to execute a query for CAT_FINALCUT products
        from exposures that overlap a specific COADD tile.  Return a dictionary
        of Catalogs along associated metadata (expnum,band,nite).

        NOTE: This version uses A MASSIVE CHEAT (compared to query_astref_catfinalcut).  It uses
            a table with predefined correspondence between IMAGEs and TILEs.  It is very succeptible
            to changes in the input list that formed that predefined table.

        Inputs:
            CatDict:   Existing CatDict, new records are added (and possibly old records updated)
            CoaddTile: Name of COADD tile for search
            ProcTag:   Processing Tag used to constrain pool of input images
            curDB:     Database connection to be used
            dbSchema:  Schema over which queries will occur.
            BandList:  List of bands (returned ImgDict list will be restricted to only these bands)
            FiatTable: Predefined table showing correspondence between IMAGEs and TILEs.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            CatDict:   Updated version of input CatDict
    """
    #
    #   Pre-assemble constraint based on BandList
    #
    BandConstraint = ''
    if BandList:
        BandConstraint = "and c.band in ('" + "','".join([d.strip() for d in BandList]) + "')"

    #
    #   Form the query to obtain the cat_finalcut files
    #
    query = f"""
        SELECT
        c.filename as catfile,
        c.expnum as expnum,
        c.ccdnum as ccdnum,
        c.band as band
        FROM
        {dbSchema:s}catalog c, {dbSchema:s}proctag t
        WHERE t.tag='{ProcTag:s}'
        and t.pfw_attempt_id=c.pfw_attempt_id
        and c.filetype='cat_finalcut'
        {BandConstraint:s}
        and exists (
        SELECT
        1
        FROM
        {dbSchema:s}image i, {FiatTable:s} y
        WHERE i.expnum=c.expnum
        and t.pfw_attempt_id=i.pfw_attempt_id
        and i.filetype='red_immask'
        and i.filename=y.filename
        and y.tilename='{CoaddTile:s}'
        )
        order by c.expnum,c.ccdnum """

    if verbose > 0:
        print("# Executing query to obtain CAT_FINALCUT catalogs")
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB = dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        catfile = rowd['catfile']
        CatDict[catfile] = rowd
    #
    #       Fix any known problematic NoneTypes before they get in the way.
    #
    #        if (CatDict[expnum]['band'] is None):
    #            CatDict[expnum]['band']='None'

    return CatDict


######################################################################################
def query_meds_psfmodels(QueryType, CoaddTile, CoaddProcTag, SE_ProcTag, COADD_ONLY, BandList,
                         ArchiveSite, dbh, dbSchema, verbose=0):
    """ Query code to obtain inputs for MOF/NGMIX (multi-epoch fitting and WL shape)
        Use an existing DB connection to execute a query for MEDs and associated
        single-epoch PSFex models.

        Inputs:
            QueryType: Either 'meds' or 'psfmodel' (
            CoaddTile: Name of COADD tile for search
            CoaddProcTag: Processing Tag used to constrain pool of input coadd runs
            SE_ProcTag:   Processing Tag used to constrain pool of input SE images/psf
            BandList:  List of bands (returned ImgDict list will be restricted to only these bands)
            ArchiveSite: Constraint that data/files exist within a specific archive
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            MedDict:   Dictionary of MEDs files from previous COADD pipeline run
            PSFDict:   Dictionary of PSF models from previous single-epoch pipeline run
    """
    #
    #   Pre-assemble constraint based on BandList (currently not used)
    #
    #BandConstraint = ''
    #if BandList:
    #    BandConstraint = "and i.band in ('" + "','".join([d.strip() for d in BandList]) + "')"
    #
    #   Query to get the MEDS files from a specific tile (also needed as a pre-query when getting PSF Models).
    #
    query = f"""SELECT fai.filename as filename,
        fai.path as path,
        fai.compression as compression,
        m.band as band,
        m.pfw_attempt_id as pfw_attempt_id
        FROM {dbSchema:s}proctag t, {dbSchema:s}miscfile m, {dbSchema:s}file_archive_info fai
        WHERE t.tag='{CoaddProcTag:s}'
        and t.pfw_attempt_id=m.pfw_attempt_id
        and m.tilename='{CoaddTile:s}'
        and m.filetype='coadd_meds'
        and fai.filename=m.filename
        and fai.archive_name='{ArchiveSite:s}'"""

    if verbose > 0:
        print("# Executing query to obtain red_immask images (based on their edges/boundaries)")
        if verbose == 1:
            print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
        else:
            print(f"# sql = {query:s}")
    curDB = dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    MedDict = {}
    for row in curDB:
        rowd = dict(zip(desc, row))
        if rowd['band'] in BandList:
            ImgName = rowd['filename']
            MedDict[ImgName] = rowd
        else:
            if verbose > 1:
                print(f" Post query constraint removed {rowd['band']:s}-band image: {rowd['filename']:s} ")

    if QueryType == 'psfmodel':
        #
        #       Get the PFW_ATTEMPT_ID for this tile (from the dictionary
        #
        AttIDList = []
        for MedImg in MedDict:
            AttIDList.append(MedDict[MedImg]['pfw_attempt_id'])
        uAttID = sorted(list(set(AttIDList)))
        if len(uAttID) > 1:
            print("WARNING: more than one attempt ID found when searching for MEDs files.")
            print("Using first ID")
        if not uAttID:
            print(f"ERROR: no MEDs files identified for tile='{CoaddTile:s}'")
            print("Aborting")
            exit(1)

        #
        #       Query for the PSF Model Files from the Single-Epoch runs.
        #
        if COADD_ONLY:
            query = f"""SELECT fai.filename as filename,
                fai.path as path,
                fai.compression as compression,
                -9999 as expnum,
                -9999 as ccdnum,
                m.band as band
                FROM {dbSchema:s}miscfile m, {dbSchema:s}file_archive_info fai
                WHERE m.pfw_attempt_id={uAttID[0]:d}
                and m.filetype='coadd_psfex_model'
                and m.filename=fai.filename
                and fai.archive_name='{ArchiveSite:s}'"""
        else:
            query = f"""SELECT fai.filename as filename,
                fai.path as path,
                fai.compression as compression,
                m.expnum as expnum,
                m.ccdnum as ccdnum,
                m.band as band
                FROM {dbSchema:s}proctag ts, {dbSchema:s}image i, {dbSchema:s}miscfile m, {dbSchema:s}file_archive_info fai
                WHERE i.pfw_attempt_id={uAttID[0]:d}
                and i.filetype='coadd_nwgint'
                and i.ccdnum=m.ccdnum
                and i.expnum=m.expnum
                and m.filetype='psfex_model'
                and m.pfw_attempt_id=ts.pfw_attempt_id
                and ts.tag='{SE_ProcTag}'
                and m.filename=fai.filename
                and fai.archive_name='{ArchiveSite:s}'"""

        if verbose > 0:
            print("# Executing query to obtain PSF models")
            if verbose == 1:
                print("# sql = " + " ".join([d.strip() for d in query.split('\n')]))
            else:
                print(f"# sql = {query:s}")
        curDB = dbh.cursor()
        curDB.execute(query)
        desc = [d[0].lower() for d in curDB.description]

        PSFDict = {}
        for row in curDB:
            rowd = dict(zip(desc, row))
            if rowd['band'] in BandList:
                ImgName = rowd['filename']
                PSFDict[ImgName] = rowd
            else:
                if verbose > 1:
                    print(f" Post query constraint removed {rowd['band']:s}-band image: {rowd['filename']:s} ")

    #
    #   Everything is now done... return the appropriate Dictionary
    #
    if QueryType == 'meds':
        ReturnDict = MedDict
    if QueryType == 'psfmodel':
        ReturnDict = PSFDict

    return ReturnDict


######################################################################################
def ImgDict_to_LLD(ImgDict, filetypes, mdatatypes, verbose=0):
    """ Function to convert CatDict into appropriate list of list of dicts suitable for WCL"""
    #
    #   Note the current means for expressing this list does not have a means to have more than
    #   one image/file type on a per record basis (this is partially because compression would need
    #   to be expressed for each image/file type.
    #

    OutLLD = []
    for Img in ImgDict:
        tmplist = []
        for ftype in filetypes:
            tmpdict = {}
            for mdata in mdatatypes[ftype]:
                if mdata in ImgDict[Img][ftype]:
                    tmpdict[mdata] = ImgDict[Img][ftype][mdata]
                else:
                    if verbose > 0:
                        print(f"Warning: missing metadata {mdata:s} for image {ImgDict[Img][ftype]['filename']:s}")
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
def CatDict_to_LLD(CatDict, filetypes, mdatatypes, verbose=0):
    """ Function to convert CatDict into appropriate list of list of dicts suitable for WCL
    """

    OutLLD = []
    for Cat in CatDict:
        tmplist = []
        for ftype in filetypes:
            tmpdict = {}
            tmpdict['filename'] = CatDict[Cat][ftype]
            tmpdict['compression'] = None
            for mdata in mdatatypes:
                if mdata in CatDict[Cat]:
                    tmpdict[mdata] = CatDict[Cat][mdata]
            tmplist.append(tmpdict)
        OutLLD.append(tmplist)

    return OutLLD
