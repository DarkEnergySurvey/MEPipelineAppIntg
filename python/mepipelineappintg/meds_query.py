# $Id: meds_query.py 48310 2019-03-01 16:24:53Z rgruendl $
# $Rev:: 48310                            $:  # Revision of last commit.
# $LastChangedBy:: rgruendl               $:  # Author of last commit.
# $LastCha
"""
A set of queries to obtain inputs for the COADD pipeline.
"""

######################################################################################
def query_imgs_from_attempt(attemptID, bands, dbh, dbSchema, verbose=0):
    """ Query code to obtain image inputs for COADD (based on a previous successful
        mulitepoch/COADD attempt.

        Use an existing DB connection to execute a query for RED_IMMASK image
        products that were found previously to overlap a specific COADD tile.
        Return a dictionary of Images along with with basic properties (expnum,
        ccdnum, band, nite).

        Inputs:
            attemptID: Exisitng (completed through first block) attempt
            bands:     List of bands (that should be included).
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            ImgDict,HeadDict:   Returns ImgDict and HeadDict
    """
#
#
    query = f"""SELECT
        i.filename as filename,
        fai.path as path,
        fai.compression as compression,
        m.filename as headfile,
        i.band as band,
        i.expnum as expnum,
        i.ccdnum as ccdnum,
        j.mag_zero as mag_zero
    FROM {dbSchema:s}image i, {dbSchema:s}image j, {dbSchema:s}desfile d, {dbSchema:s}desfile d2, {dbSchema:s}opm_was_derived_from wdf, {dbSchema:s}miscfile m, {dbSchema:s}file_archive_info fai
    WHERE d.pfw_attempt_id={attemptID:s}
        and d.filetype='coadd_nwgint'
        and d.id=wdf.child_desfile_id
        and wdf.parent_desfile_id=d2.id
        and d2.filetype='red_immask'
        and d2.filename=i.filename
        and d2.filename=fai.filename
        and d.pfw_attempt_id=m.pfw_attempt_id
        and m.filetype='coadd_head_scamp'
        and m.expnum=i.expnum
        and m.ccdnum=i.ccdnum
        and d.filename=j.filename
        """

    if verbose > 0:
        print("# Executing query to obtain red_immask images (based on their use in a previous multiepoch attempt)")
        if verbose == 1:
            print(f"# sql = " + ' '.join([d.strip() for d in query.split('\n')]))
        if verbose > 1:
            print(f"# sql = {query:s}")
    curDB = dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    ImgDict = {}
    HeadDict = {}
    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName = rowd['filename']
        Band = rowd['band']
        if (Band in bands):
            ImgDict[ImgName] = {}
            HeadDict[ImgName] = {}
            for key in ['filename', 'path', 'compression', 'band', 'expnum', 'ccdnum']:
                ImgDict[ImgName][key] = rowd[key]
            if rowd['mag_zero'] is not None:
                ImgDict[ImgName]['mag_zero'] = rowd['mag_zero']
            HeadDict[ImgName]['filename'] = rowd['headfile']
            for key in ['band', 'expnum', 'ccdnum']:
                HeadDict[ImgName][key] = rowd[key]

#
#   Secondary query to get paths for Head Files
#
    ImgList = []
    for ImgName in HeadDict:
        ImgList.append([HeadDict[ImgName]['filename']])
#   Make sure teh GTT_FILENAME table is empty
    curDB.execute('delete from GTT_FILENAME')
#   load img ids into opm_filename_gtt table
    print(f"# Loading GTT_FILENAME table with headfile names for secondary query to get paths with {len(ImgList):d} images")
    dbh.insert_many('GTT_FILENAME', ['FILENAME'], ImgList)

    query = f"""SELECT
        fai.filename as filename,
        fai.path as path,
        fai.compression as compression
    FROM {dbSchema:s}file_archive_info fai, gtt_filename g
    WHERE fai.filename=g.filename
        """

    if verbose > 0:
        print("# Executing query to obtain red_immask images (based on their use in a previous multiepoch attempt)")
        if verbose == 1:
            print(f"# sql = " + ' '.join([d.strip() for d in query.split('\n')]))
        if verbose > 1:
            print(f"# sql = {query:s}")
    curDB = dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

#
#   Form a temporary dictionary (tmpDict) with results
#
    tmpDict = {}
    for row in curDB:
        rowd = dict(zip(desc, row))
        ImgName = rowd['filename']
        tmpDict[ImgName] = rowd

#
#   Go through HeadDict and assign path and compression fields from the tmpDict
#
    for Img in HeadDict:
        if HeadDict[Img]['filename'] in tmpDict:
            HeadDict[Img]['path'] = tmpDict[HeadDict[Img]['filename']]['path']
            HeadDict[Img]['compression'] = tmpDict[HeadDict[Img]['filename']]['compression']
        else:
            print(f"Warning: No entry in FILE_ARCHIVE_INFO found for {HeadDict[Img]['filename']:s}")

    return ImgDict, HeadDict


######################################################################################
def query_attempt_from_tag_tile(ProcTag, TileName, dbh, dbSchema, verbose=0):
    """ Query code to obtain an attempt ID given a tag and tilename

        Uses an existing DB connection to execute a query
        Returns a PFW_ATTEMPT_ID (or None)
        Note that for this use the PFW_ATTEMPT_ID returned will be an INT type so 
            for some uses a subsequent conversion to a str type may be necessary

        Inputs:
            ProcTag:   Tag name to use for query
            TileName:  Tile name to use for query
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            attemptID: PFW_ATTEMPT_ID (or NoneType if failed)
    """
#
#
    query = f"""SELECT t.pfw_attempt_id
    FROM {dbSchema:s}proctag t, {dbSchema:s}pfw_attempt_val av
    WHERE t.tag='{ProcTag:s}'
        and t.pfw_attempt_id=av.pfw_attempt_id
        and av.key='tilename'
        and av.val='{TileName:s}'
        """

    if verbose > 0:
        print("# Executing query to obtain PFW_ATTEMPT_ID)")
        if verbose == 1:
            print(f"# sql = " + ' '.join([d.strip() for d in query.split('\n')]))
        if verbose > 1:
            print(f"# sql = {query:s}")
    curDB = dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    attemptID=None 
    for row in curDB:
        rowd = dict(zip(desc, row))
        attemptID=rowd['pfw_attempt_id']

#
#   Give warning if no value was found.
#
    if (attemptID is None):
        print("Warning: No PFW_ATTEMPT_ID (i.e. no run) identified for tag={:s} and tilename{:s}".format(ProcTag,TileName)) 

    return attemptID


