import os
import time
from functools import lru_cache

from mepipelineappintg.mepochmisc import get_root_archive

MAGZP_REF = 30.0


def _execute_query(query, dbh, verbose, Timing):
    """actually execute a query"""
    t0 = time.time()

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

    rowds = []
    for row in curDB:
        rowds.append(dict(zip(desc, row)))

    if Timing:
        t1 = time.time()
        print(f"Query execution time: {t1 - t0:.2f}")
    curDB.close()

    return rowds


@lru_cache(maxsize=1024)
def _do_piff_info_query(
    dbh, verbose, Timing, expnum, PiffTag, relPrefix, dbSchema
):
    query = f"""SELECT
                    DISTINCT
                    qa.filename,
                    qa.flag as desdm_flags,
                    qa.fwhm_cen,
                    qa.star_t_std,
                    qa.star_t_mean,
                    qa.nstar,
                    qa.exp_star_t_mean,
                    qa.exp_star_t_std
                FROM
                    {dbSchema:s}{relPrefix:s}proctag t,
                    {dbSchema:s}{relPrefix:s}miscfile m,
                    {dbSchema:s}{relPrefix:s}PIFF_MODEL_QA qa
                where
                    qa.expnum = {expnum:d}
                    and t.tag = '{PiffTag:s}'
                    and t.pfw_attempt_id = m.pfw_attempt_id
                    and m.filetype = 'piff_model'
                    and m.filename = qa.filename
                """

    return _execute_query(query, dbh, verbose, Timing)


def add_piff_info_to_yaml(
    yaml_data, PiffTag, dbh, dbSchema, releasePrefix=None,
    Timing=False, verbose=0
):
    """Get coadd info from attemp ID.

        Inputs:
            yaml_data: the yaml data to adjust
            PiffTag:   the Piff tag to use
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            releasePrefix: Prefix string (including _'s) to identify a specific
                           set of tables
                           (Useful when working from releases in DESSCI).
                           None --> will substitute a null string.
            Timing:    Causes internal timing to report results.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            coadd_data: A dictionary containing the relevant coadd data.
    """
    if releasePrefix is None:
        relPrefix = ""
    else:
        relPrefix = releasePrefix

    for band in yaml_data:
        for isrc in range(len(yaml_data[band]["src_info"])):
            expnum = yaml_data[band]["src_info"][isrc]["expnum"]
            piff_rows = _do_piff_info_query(
                dbh, verbose, Timing, expnum, PiffTag, relPrefix, dbSchema
            )
            piff_info = None
            for row in piff_rows:
                if (
                    row["filename"]
                    == os.path.basename(yaml_data[band]["src_info"][isrc]["piff_path"])
                ):
                    piff_info = row
            if piff_info is None:
                raise RuntimeError("could not find piff info for expnum %s" % expnum)
            yaml_data[band]["src_info"][isrc]["piff_info"] = piff_info


def get_coadd_info_from_attempt(
    tilename, band, AttemptID, ProcTag, dbh, dbSchema, releasePrefix=None,
    Timing=False, verbose=0
):
    """Get coadd info from attemp ID.

        Inputs:
            tilename: The name of the tile.
            band: the band as a string
            AttemptID:  The AttemptID for which to extract a tilename.
            ProcTag:   Proctag name containing set to be worked on
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            releasePrefix: Prefix string (including _'s) to identify a specific
                           set of tables
                           (Useful when working from releases in DESSCI).
                           None --> will substitute a null string.
            Timing:    Causes internal timing to report results.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            coadd_data: A dictionary containing the relevant coadd data.
    """
    if releasePrefix is None:
        relPrefix = ""
    else:
        relPrefix = releasePrefix

    root_archive = get_root_archive(dbh, archive_name='desar2home', verb=verbose)

    data = {}
    for filetype in ["coadd", "coadd_segmap", "coadd_psfex_model"]:
        if filetype == "coadd":
            db_table = "coadd"
        else:
            db_table = "miscfile"

        query = f"""SELECT
                m.tilename as tilename,
                fai.path as path,
                fai.filename as filename,
                fai.compression as compression,
                m.band as band,
                m.pfw_attempt_id as pfw_attempt_id
            from
                {dbSchema:s}{relPrefix:s}proctag t,
                {dbSchema:s}{relPrefix:s}{db_table} m,
                {dbSchema:s}{relPrefix:s}file_archive_info fai
            where
                t.tag='{ProcTag:s}'
                and t.pfw_attempt_id=m.pfw_attempt_id
                and m.tilename='{tilename:s}'
                and m.band='{band:s}'
                and m.filetype='{filetype:s}'
                and fai.filename=m.filename
                and fai.archive_name='desar2home'
            """

        rowds = _execute_query(query, dbh, verbose, Timing)
        assert len(rowds) == 1
        rowd = rowds[0]

        if rowd["compression"] is None:
            rowd["compression"] = ""

        rowd["fullname"] = os.path.join(
            root_archive,
            rowd['path'],
            rowd['filename'] + rowd['compression'],
        )
        data[filetype] = rowd

    filetype = "coadd_cat"
    query = f"""SELECT
            m.tilename as tilename,
            fai.path as path,
            fai.filename as filename,
            fai.compression as compression,
            m.band as band,
            m.pfw_attempt_id as pfw_attempt_id
        from
            {dbSchema:s}{relPrefix:s}proctag t,
            {dbSchema:s}{relPrefix:s}catalog m,
            {dbSchema:s}{relPrefix:s}file_archive_info fai
        where
            t.tag='{ProcTag:s}'
            and t.pfw_attempt_id=m.pfw_attempt_id
            and m.tilename='{tilename:s}'
            and m.band='{band:s}'
            and m.filetype='{filetype:s}'
            and fai.filename=m.filename
            and fai.archive_name='desar2home'
        """

    rowds = _execute_query(query, dbh, verbose, Timing)
    assert len(rowds) == 1
    rowd = rowds[0]
    if rowd["compression"] is None:
        rowd["compression"] = ""

    rowd["fullname"] = os.path.join(
        root_archive,
        rowd['path'],
        rowd['filename'] + rowd['compression'],
    )
    data[filetype] = rowd

    return data


######################################################################################
def get_tilename_from_attempt(
    AttemptID, ProcTag, dbh, dbSchema, releasePrefix=None, Timing=False, verbose=0
):
    """ Query code to obtain COADD tile PFW_ATTEMPT_ID after constraining
        that results are part of a specific PROCTAG.

        Inputs:
            AttemptID:  The AttemptID for which to extract a tilename.
            ProcTag:   Proctag name containing set to be worked on
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            releasePrefix: Prefix string (including _'s) to identify a specific
                           set of tables
                           (Useful when working from releases in DESSCI).
                           None --> will substitute a null string.
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
            distinct c.tilename as tilename
        FROM {dbSchema:s}{relPrefix:s}proctag t, {dbSchema:s}{relPrefix:s}catalog c
        WHERE t.tag='{ProcTag:s}'
            and t.pfw_attempt_id=c.pfw_attempt_id
            and c.filetype='coadd_cat'
            and t.pfw_attempt_id='{AttemptID:s}'
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
            attval = rowd['tilename']
        else:
            print(
                f"Found more than one tile for attempt"
                f"={AttemptID:s} attval={attval:d} vs {rowd['tilename']:s} "
            )
            attval = rowd['tilename']

    #
    #   This is a backup attempt that is meant to handle cases where no coadd_catalogs
    #   were output (eg. Mangle standalone)
    #
    if attval is None:
        print(
            "First attempt to find PFW_ATTEMPT_ID failed... "
            "switching to use miscfile"
        )

        query = f"""SELECT
                distinct m.tilename as tilename
            FROM {dbSchema:s}{relPrefix:s}proctag t, {dbSchema:s}{relPrefix:s}miscfile m
            WHERE t.tag='{ProcTag:s}'
                and t.pfw_attempt_id=m.pfw_attempt_id
                and m.pfw_attempt_id='{AttemptID:s}'
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
                attval = rowd['tilename']
            else:
                print(
                    f"Found more than one tile for attempt"
                    f"={AttemptID:s} attval={attval:d} vs {rowd['tilename']:s} "
                )
                attval = rowd['tilename']

    if Timing:
        t1 = time.time()
        print(f" Query to find tilename execution time: {t1 - t0:.2f}")
    curDB.close()

    return attval


def make_pizza_cutter_yaml(
    pfw_attempt_id, tilename, gcat,
    img_dict, head_dict, bkg_dict, seg_dict, psf_dict,
    bands_to_write, coadd_data,
):
    """Make the yaml input required by pizza cutter

    Its format is

    ```yaml
    tilename: DES2029-5457
    band: r
    bmask_ext: msk
    bmask_path: $MEDS_DIR/des-pizza-slices-y3-v02/DES2029-5457/sources-r/OPS/multiepoch/Y3A1/r2597/DES2029-5457/p01/coadd/DES2029-5457_r2597p01_r.fits.fz
    cat_path: $MEDS_DIR/des-pizza-slices-y3-v02/DES2029-5457/sources-r/OPS/multiepoch/Y3A1/r2597/DES2029-5457/p01/cat/DES2029-5457_r2597p01_r_cat.fits
    compression: .fz
    filename: DES2029-5457_r2597p01_r.fits
    image_ext: sci
    image_flags: 0
    image_path: $MEDS_DIR/des-pizza-slices-y3-v02/DES2029-5457/sources-r/OPS/multiepoch/Y3A1/r2597/DES2029-5457/p01/coadd/DES2029-5457_r2597p01_r.fits.fz
    image_shape: [10000, 10000]
    magzp: 30.0
    path: OPS/multiepoch/Y3A1/r2597/DES2029-5457/p01/coadd
    pfw_attempt_id: 576520
    position_offset: 1
    psf_path: $MEDS_DIR/des-pizza-slices-y3-v02/DES2029-5457/sources-r/OPS/multiepoch/Y3A1/r2597/DES2029-5457/p01/psf/DES2029-5457_r2597p01_r_psfcat.psf
    scale: 1.0
    seg_ext: sci
    seg_path: $MEDS_DIR/des-pizza-slices-y3-v02/DES2029-5457/sources-r/OPS/multiepoch/Y3A1/r2597/DES2029-5457/p01/seg/DES2029-5457_r2597p01_r_segmap.fits
    src_info:
    - band: r
      bkg_ext: sci
      bkg_path: $MEDS_DIR/des-pizza-slices-y3-v02/DES2029-5457/sources-r/OPS/finalcut/Y2A1/Y1-2356/20130831/D00229314/p01/red/bkg/D00229314_r_c51_r2356p01_bkg.fits.fz
      bmask_ext: msk
      bmask_path: $MEDS_DIR/des-pizza-slices-y3-v02/DES2029-5457/sources-r/OPS/finalcut/Y2A1/Y1-2356/20130831/D00229314/p01/red/immask/D00229314_r_c51_r2356p01_immasked.fits.fz
      ccdnum: 51
      compression: .fz
      expnum: 229314
      filename: D00229314_r_c51_r2356p01_immasked.fits
      head_path: $MEDS_DIR/des-pizza-slices-y3-v02/DES2029-5457/sources-r/OPS/multiepoch/Y3A1/r2597/DES2029-5457/p01/aux/DES2029-5457_r2597p01_D00229314_r_c51_scamp.ohead
      image_ext: sci
      image_flags: 0
      image_path: $MEDS_DIR/des-pizza-slices-y3-v02/DES2029-5457/sources-r/OPS/finalcut/Y2A1/Y1-2356/20130831/D00229314/p01/red/immask/D00229314_r_c51_r2356p01_immasked.fits.fz
      image_shape: [4096, 2048]
      magzp: 31.707290649414062
      path: OPS/finalcut/Y2A1/Y1-2356/20130831/D00229314/p01/red/immask
      pfw_attempt_id: 576520
      piff_path: $PIFF_DATA_DIR/y3a1-v29/229314/D00229314_r_c51_r2356p01_piff.fits
      position_offset: 1
      psf_path: $MEDS_DIR/des-pizza-slices-y3-v02/DES2029-5457/sources-r/OPS/finalcut/Y2A1/Y1-2356/20130831/D00229314/p01/psf/D00229314_r_c51_r2356p01_psfexcat.psf
      psfex_path: $MEDS_DIR/des-pizza-slices-y3-v02/DES2029-5457/sources-r/OPS/finalcut/Y2A1/Y1-2356/20130831/D00229314/p01/psf/D00229314_r_c51_r2356p01_psfexcat.psf
      scale: 0.20753136388105564
      seg_path: $MEDS_DIR/des-pizza-slices-y3-v02/DES2029-5457/sources-r/OPS/finalcut/Y2A1/Y1-2356/20130831/D00229314/p01/seg/D00229314_r_c51_r2356p01_segmap.fits.fz
      tilename: DES2029-5457
      weight_ext: wgt
      weight_path: $MEDS_DIR/des-pizza-slices-y3-v02/DES2029-5457/sources-r/OPS/finalcut/Y2A1/Y1-2356/20130831/D00229314/p01/red/immask/D00229314_r_c51_r2356p01_immasked.fits.fz
    ```

    We assume the following

        - The mag zero point is 30 for the coadds. All SE images are scaled to put their units
          on this same zero point.
        - All images found are included in the code (i.e. `image_flags` == 0 always below)
        - The names of the FITS extensions (e.g., 'sci', 'msk', 'wgt', etc.) have not changed since Y3.

    Parameters
    ----------
    pfw_attempt_id : int
        The attempt ID to use.
    tilename : str
        The name of the tile.
    gcat: str
        The filename/path of a GAIA input catalogs (FITS format)
    img_dict : dict
    head_dict : dict
    bkg_dict : dict
    seg_dict : dict
    psf_dict : dict
    bands_to_write : list of str
        The bands for which to produce the data.

    Returns
    -------
    data : dict
        A python dictionary keyed on band that when dumped with a yaml parser produces the right
        data format for metadetect w/ the pizza-cutter.
    """  # noqa
    total_data = {}
    for band in bands_to_write:
        total_data[band] = {
            "tilename": tilename,
            "band": band,
            "bmask_ext": "msk",
            "bmask_path": coadd_data[band]["coadd"]["fullname"],
            "cat_path": coadd_data[band]["coadd_cat"]["fullname"],
            "compression": coadd_data[band]["coadd"]["compression"],
            "filename": coadd_data[band]["coadd"]["filename"],
            "image_ext": "sci",
            "image_flags": 0,
            "image_path": coadd_data[band]["coadd"]["fullname"],
            "image_shape": [10000, 10000],
            "magzp": MAGZP_REF,
            "path": coadd_data[band]["coadd"]["path"],
            "pfw_attempt_id": int(pfw_attempt_id),
            "position_offset": 1,
            "psf_path": coadd_data[band]["coadd_psfex_model"]["fullname"],
            "scale": 1.0,
            "seg_ext": "sci",
            "seg_path": coadd_data[band]["coadd_segmap"]["fullname"],
            "src_info": []
        }
        if (gcat is not None):
            total_data[band]['gaia_stars_file'] = gcat

        for img in img_dict:
            if (
                any(img not in d for d in [head_dict, bkg_dict, seg_dict, psf_dict])
                or img_dict[img]["band"] != band
            ):
                continue
            info = {
                "band": band,
                "bkg_ext": "sci",
                "bkg_path": bkg_dict[img]["fullname"],
                "bmask_ext": "msk",
                "bmask_path": img_dict[img]["fullname"],
                "ccdnum": img_dict[img]["ccdnum"],
                "compression": img_dict[img]["compression"],
                "expnum": img_dict[img]["expnum"],
                "filename": os.path.basename(img_dict[img]["fullname"]),
                "head_path": head_dict[img]["fullname"],
                "image_ext": "sci",
                "image_flags": 0,
                "image_path": img_dict[img]["fullname"],
                "image_shape": [4096, 2048],
                "magzp": img_dict[img]["mag_zero"],
                "path": img_dict[img]["path"],
                "pfw_attempt_id": int(pfw_attempt_id),
                "piff_path": psf_dict[img]["fullname"],
                "position_offset": 1,
                # these are set to None and should not be needed
                # contact MRB if the code errors due to this.
                "psf_path": None,
                "psfex_path": None,
                "scale": 10.0**(0.4*(MAGZP_REF - img_dict[img]["mag_zero"])),
                "seg_path": seg_dict[img]["fullname"],
                "tilename": tilename,
                "weight_ext": "wgt",
                "weight_path": img_dict[img]["fullname"],
            }

            total_data[band]["src_info"].append(info)

    return total_data
