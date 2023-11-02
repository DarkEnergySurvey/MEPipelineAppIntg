#! /usr/bin/env python3
# coadd_bleedtrail_mask.py 
# primary author: rgruendl
"""
Code to provide re-estimation of bleed trail masking needed in a coadd tile based on the
bleed trails detected in single-frames.
"""

######################################################################################
def query_coadd_geometry(TileDict,CoaddTile,ProcTag,dbh,dbSchema,PFWID=None,verbose=0):
    """ Query code to obtain COADD tile geometry

        Inputs:
            TileDict:  Existing TileDict, new records are added (and possibly old records updated)
            CoaddTile: Name of COADD tile for search
            Proctag:   Group of attempts to work from
            PFWID:     Working using a given/specified run (perhaps no tag)
            dbh:       Database connection to be used
            dbSchema:  Schema over which queries will occur.
            verbose:   Integer setting level of verbosity when running.

        Returns:
            TileDict:  Updated version of input TileDict
    """

#
#   Query to obtain geometric specification of a specific COADD tile(s).
#

    if (PFWID is not None):
#
#       Case where an ongoing/untagged run is targetted.
#
        query="""SELECT
            av.pfw_attempt_id as pfw_attempt_id, 
            t.tilename as tilename,
            t.ra_cent as ra_cent, t.dec_cent as dec_cent,
            t.rac1 as rac1, t.rac2 as rac2, t.rac3 as rac3, t.rac4 as rac4,
            t.decc1 as decc1, t.decc2 as decc2, t.decc3 as decc3, t.decc4 as decc4,
            t.crossra0 as crossra0, 
            t.RACMIN as racmin, t.RACMAX as racmax, t.DECCMIN as deccmin, t.DECCMAX as deccmax, 
            t.URAMIN as uramin, t.URAMAX as uramax, t.UDECMIN as udecmin, t.UDECMAX as udecmax,
            t.pixelscale as pixelscale, t.naxis1 as naxis1, t.naxis2 as naxis2
        FROM {schema:s}coaddtile_geom t, {schema:s}pfw_attempt_val av
        WHERE av.pfw_attempt_id={pfwid:d}
            and av.key='tilename'
            and av.val=t.tilename""".format(schema=dbSchema,pfwid=PFWID)
    else:
        if (CoaddTile is None):
#
#           Case where a list of tiles is assumed (grabbing a whole tag).
#
            query="""SELECT 
                av.pfw_attempt_id as pfw_attempt_id,
                t.tilename as tilename,
                t.ra_cent as ra_cent, t.dec_cent as dec_cent,
                t.rac1 as rac1, t.rac2 as rac2, t.rac3 as rac3, t.rac4 as rac4,
                t.decc1 as decc1, t.decc2 as decc2, t.decc3 as decc3, t.decc4 as decc4,
                t.crossra0 as crossra0, 
                t.RACMIN as racmin, t.RACMAX as racmax, t.DECCMIN as deccmin, t.DECCMAX as deccmax, 
                t.URAMIN as uramin, t.URAMAX as uramax, t.UDECMIN as udecmin, t.UDECMAX as udecmax,
                t.pixelscale as pixelscale, t.naxis1 as naxis1, t.naxis2 as naxis2
            FROM {schema:s}coaddtile_geom t, {schema:s}pfw_attempt_val av, {schema:s}proctag pt
            WHERE pt.tag='{ptag:s}'
                and pt.pfw_attempt_id=av.pfw_attempt_id
                and av.key='tilename'
                and av.val=t.tilename""".format(schema=dbSchema,ptag=ProcTag)
        else:
#
#           Case where a list of tiles was given
#
            query="""SELECT 
                av.pfw_attempt_id as pfw_attempt_id, 
                t.tilename as tilename,
                t.ra_cent as ra_cent, t.dec_cent as dec_cent,
                t.rac1 as rac1, t.rac2 as rac2, t.rac3 as rac3, t.rac4 as rac4,
                t.decc1 as decc1, t.decc2 as decc2, t.decc3 as decc3, t.decc4 as decc4,
                t.crossra0 as crossra0, 
                t.RACMIN as racmin, t.RACMAX as racmax, t.DECCMIN as deccmin, t.DECCMAX as deccmax, 
                t.URAMIN as uramin, t.URAMAX as uramax, t.UDECMIN as udecmin, t.UDECMAX as udecmax,
                t.pixelscale as pixelscale, t.naxis1 as naxis1, t.naxis2 as naxis2
            FROM {schema:s}coaddtile_geom t, {schema:s}pfw_attempt_val av, {schema:s}proctag pt, gtt_str g
            WHERE pt.tag='{ptag:s}'
                and pt.pfw_attempt_id=av.pfw_attempt_id
                and av.key='tilename'
                and av.val=t.tilename
                and t.tilename=g.str""".format(schema=dbSchema,ptag=ProcTag)
#
#   Have worked out which query is needed, get on with it
#
    if (verbose > 0):
        if (verbose == 1):
            QueryLines=query.split('\n')
            QueryOneLine='sql = '
            for line in QueryLines:
                QueryOneLine=QueryOneLine+" "+line.strip()
            print(QueryOneLine)
        if (verbose > 1):
            print(query)
#
#
#
    curDB=dbh.cursor()

    if (CoaddTile is not None):
#
#       Needed when preloading a list of tiles
#
        TileList=[]
        for Tile in CoaddTile:
            TileList.append([Tile])
        # Make sure the GTT_STR table is empty
        curDB.execute('delete from GTT_STR')
        # load img ids into opm_filename_gtt table
        print("# Loading GTT_STR table to accomodate a list of tiles with entries for {:d} tiles".format(len(TileList)))
        dbh.insert_many('GTT_STR',['STR'],TileList)
        
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        TileName=rowd['tilename']
        TileDict[TileName]=rowd
#
    curDB.close()

    return TileDict


######################################################################################
def query_coadd_bleed(AID,dbh,dbSchema,BandList=[],verbose=0):
    """ Query code to obtain bleedtrail records for image inputs into a COADD 

        Inputs:
##            ImgDict:    Existing ImgDict, new records are added (and possibly old records updated)
##            CoaddTile:  Name of COADD tile for search
##            ProcTag:    Processing Tag used to constrain pool of input images
##            Blacklistnfo: Dictionary containing information about Blacklist Constraint 
##                            (NoneType yields no constraint)
##                            BlacklistInfo['table'] provides table to use
            curDB:      Database connection to be used
            dbSchema:   Schema over which queries will occur.
            verbose:    Integer setting level of verbosity when running.

        Returns:
            BleedDict:    Updated version of input ImgDict
    """


    t0=time.time()
    curDB=dbh.cursor()
    if (len(AID)>0):
        curDB.execute('delete from GTT_ID')
        # load img ids into opm_filename_gtt table
        print("# Loading GTT_ID table with PFW_ATTEMPT_IDs for BLEEDTRAIL queries with entries for {:d} attempts".format(len(AID)))
        dbh.insert_many('GTT_ID',['ID'],AID)
    else:
        print("query_coadd_bleedtrail, expecting a list of IDs in AID but list is empty or malformed.")
        print("Abort")
        exit(0)


    query="""SELECT 
        av.val as tilename,
        i.filename as filename,
        c.expnum as expnum,
        c.ccdnum as ccdnum,
        c.band as band,
        b.rnum as rnum,
        b.ra_1 as ra_1,
        b.ra_2 as ra_2,
        b.ra_3 as ra_3,
        b.ra_4 as ra_4,
        b.dec_1 as dec_1,
        b.dec_2 as dec_2,
        b.dec_3 as dec_3,
        b.dec_4 as dec_4
    FROM {schema:s}bleedtrail b, {schema:s}catalog c, {schema:s}image i, {schema:s}opm_was_derived_from wdf, {schema:s}desfile d1, {schema:s}desfile d2, {schema:s}pfw_attempt_val av, gtt_id g
    WHERE d1.pfw_attempt_id=g.id
        and g.id=av.pfw_attempt_id
        and av.key='tilename'
        and d1.filetype='coadd_nwgint'
        and d1.id=wdf.child_desfile_id
        and wdf.parent_desfile_id=d2.id
        and d2.filetype='red_immask'
        and d2.pfw_attempt_id=c.pfw_attempt_id
        and d2.filename=i.filename
        and c.filetype='cat_trailbox'
        and c.expnum=i.expnum
        and c.ccdnum=i.ccdnum
        and c.filename=b.filename""".format(att_id=AID,schema=dbSchema)

    if (verbose > 0):
        if (verbose == 1):
            QueryLines=query.split('\n')
            QueryOneLine='sql = '
            for line in QueryLines:
                QueryOneLine=QueryOneLine+" "+line.strip()
            print(QueryOneLine)
        if (verbose > 1):
            print(query)

    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    BleedDict={}
    for row in curDB:
        rowd = dict(zip(desc, row))
#
#       Fix any known problematic NoneTypes before they get in the way.
#
        if (rowd['band'] is None):
            rowd['band']='None'
        Tile=rowd['tilename']
#        Img=rowd['filename']
        if (Tile not in BleedDict):
            BleedDict[Tile]=[]
        BleedDict[Tile].append(rowd)

    t3=time.time()
    print("Completed query to obtain BLEED TRAIL data")
    print("Elapsed time: {:.2f} seconds.".format(t3-t0))

    return BleedDict




######################################################################################
def work_bleedlist(Tile,BleedDict,mrad=4.0,SkipEdgeBleed=False,ThinPix=3.3,MinFrame=0,verbose=0):

    """ Work through dict of bleeds from a given tile. 
        Subdivide into types, find overlapping sets

        Inputs:
            Tile:           Just for messsaging currently.
            BleedDict:      Dict of Bleeds w/ attributes from query code.
            mrad:           Matching radius (currently not used)
            SkipEdgeBleed:  Do not try to mix EdgeBleeds into the final result.
            ThinPix:        Extent (in pix) in Declination direction where a bleed might arise from CCD
                                specific issues (hot-pix)... that should be erased in a COADD (so ignore).
            MinFrame:       Minimum number of overlapping trails required for a consolidated set to be 
                                carried forward (output).
            verbose:        Integer setting level of verbosity when running.

        Returns:
            BleedSet:   Consolidated list of bleeds with attributes.
    """
#   HERE
#   A code refactor might allow removal of SSTalk:  
#   pre-process img based set:  look for edgebleed
#   find reflected (long) trail around min/max Dec of Edgebleed
#   ---also will provide NIMG so could add setting minframe as a fraction...
#   ---also provides a way to spot things that are multi-trail (.e.g. more than one satstar) where median will not work...
#
    if (verbose > 0):
        print("Working in tile: {:s}".format(Tile))

    pi=3.141592654
    halfpi=pi/2.0
    deg2rad=pi/180.0
#
    ThinAsec=ThinPix*0.263/3600.
#    match_rad2=4.*(match_rad/3600.)*(match_rad/3600.)
    match_rad=mrad

##########################################
#   Preprocess Trails subdividing into classes.
#
    BleedList=BleedDict[Tile]
    if (verbose > 1):
        print("Input BleedTrail List size: {:d}".format(len(BleedList)))

    bandlist=[]
    BleedPerBand={}
    EdgeBleedPerBand={}
    ThinSmall={}
    for BleedTrail in BleedList:
        if (BleedTrail['band'] not in bandlist):
            bandlist.append(BleedTrail['band'])
            BleedPerBand[BleedTrail['band']]=[]
            EdgeBleedPerBand[BleedTrail['band']]=[]
            ThinSmall[BleedTrail['band']]=[]

        bleed_ra=np.array([BleedTrail['ra_1'],BleedTrail['ra_2'],BleedTrail['ra_3'],BleedTrail['ra_4']])
        wsm=np.where(bleed_ra>180.0)
        bleed_ra[wsm]=bleed_ra[wsm]-360.0
        bleed_dec=np.array([BleedTrail['dec_1'],BleedTrail['dec_2'],BleedTrail['dec_3'],BleedTrail['dec_4']])
        ra_cen=np.average(bleed_ra)
        dec_cen=np.average(bleed_dec)
        ra_min=np.amin(bleed_ra)
        ra_max=np.amax(bleed_ra)
        dec_min=np.amin(bleed_dec)
        dec_max=np.amax(bleed_dec)
        ra_size=ra_max-ra_min
        dec_size=dec_max-dec_min

        if (dec_size > 0.05):
#
#           Separate regions that describe amplifier size blocks for EdgeBleeds
#
            EdgeBleedPerBand[BleedTrail['band']].append(
                {'ra_cen':ra_cen,'dec_cen':dec_cen,
                 'ra_size':ra_size,'dec_size':dec_size,
                 'ra_min':ra_min,'ra_max':ra_max,
                 'dec_min':dec_min,'dec_max':dec_max,
                 'ra_corn':bleed_ra,'dec_corn':bleed_dec})
        else:
            if ((dec_size < ThinAsec)):
#
#               Eliminate extremely small, thin regions:
#                   - recall if they are associated with a bright star they will also be masked by the star
#                   - Bad column/hot pixels can generate a flagged bleed but are typically just 1 pix wide 
#                       (expanded to 3 in the mask)
#                   - Three-pixel wide region would be 0.000225 deg 
#
                ThinSmall[BleedTrail['band']].append(
                    {'ra_cen':ra_cen,'dec_cen':dec_cen,
                     'ra_size':ra_size,'dec_size':dec_size,
                     'ra_min':ra_min,'ra_max':ra_max,
                     'dec_min':dec_min,'dec_max':dec_max,
                     'ra_corn':bleed_ra,'dec_corn':bleed_dec})
                if (verbose>2):
                    print(" ThinSmall {:11.7f} {:11.7f}  {:9.6f} {:9.6f}  {:} {:}".format(
                        ra_cen,dec_cen,ra_size,dec_size,bleed_ra,bleed_dec))
            else:
#
#               Finally the "normal" bleeds
#
                BleedPerBand[BleedTrail['band']].append(
                    {'ra_cen':ra_cen,'dec_cen':dec_cen,
                     'ra_size':ra_size,'dec_size':dec_size,
                     'ra_min':ra_min,'ra_max':ra_max,
                     'dec_min':dec_min,'dec_max':dec_max,
                     'ra_corn':bleed_ra,'dec_corn':bleed_dec})

#   Summary
    for band in bandlist:
        print(" {:s}-band trails subdivided: {:d} normal, {:d} thin-small, and {:d} probable edge-bleeds".format(
            band,len(BleedPerBand[band]),len(ThinSmall[band]),len(EdgeBleedPerBand[band])))


    t00=time.time()
    BleedSet={}
#
#   Eliminate VISTA and DES Y-band for now
#
    new_bandlist=[]
    for band in bandlist:
        if (band not in ['Y','VY','J','H','Ks']):
            new_bandlist.append(band)
    bandlist=new_bandlist

##########################################
#
#   Begin consolidating Bleeds on a by-band basis
#
    for band in bandlist:
        nsize=len(BleedPerBand[band])
        match=np.zeros((nsize,nsize),dtype=int)
        used=np.zeros((nsize),dtype=int)

#
#       Determine which bleed trails intersect
#
        t0=time.time()
        racen=np.array([BleedPerBand[band][ix]['ra_cen'] for ix in range(nsize)])
        deccen=np.array([BleedPerBand[band][ix]['dec_cen'] for ix in range(nsize)])
        rasz=np.array([BleedPerBand[band][ix]['ra_size'] for ix in range(nsize)])
        decsz=np.array([BleedPerBand[band][ix]['dec_size'] for ix in range(nsize)])

        for iy in range(nsize):
            dra=np.abs(racen-racen[iy])
            ddec=np.abs(deccen-deccen[iy])
            sra=rasz+rasz[iy]
            sdec=decsz+decsz[iy]
            wsm=np.where(np.logical_and(2.0*dra < sra,2.0*ddec < sdec))
            match[iy,:][wsm]=1
        print("Form Matching array for {:s}-band. Execution Time: {:.2f}".format(band,time.time()-t0))

        t0=time.time()
        ######################
        #  Iterative grouping together of overlaps...

        xind=np.arange(nsize)
        BleedSet[band]=[]
        cnt=1
        NumOrphans=0
        NumRejects=0
        for iy in range(nsize):
            if (used[iy]==0):
                if (verbose > 2):
                    print("Starting on entry {:d}".format(iy))
                mlist=[]
                mlist.append(iy)
                used[iy]=1
                nfound=1
                # Find matches based on box overlap.
                wsm=np.where(np.logical_and(match[iy,:]==1,used==0))
                used[wsm]=1
                for ix in xind[wsm]:
                    mlist.append(ix)
                niter=1
                if (verbose > 2):
                    print("Niter {:d} found a group of {:d}".format(niter,len(mlist)))
                if (verbose > 3):
                    print("mlist: ",mlist)
                # Now Iterate through matches of matches until list stops growing.
                while (len(mlist)>nfound):
                    nfound=len(mlist)
                    for ifnd in range(nfound):
                        wsm=np.where(np.logical_and(match[mlist[ifnd],:]==1,used==0))
                        if(used[wsm].size > 0):
                            used[wsm]=1
                            for ix in xind[wsm]:
                                mlist.append(ix)
                    niter=niter+1
                    if (verbose > 2):
                        print("Niter {:d} found a group of {:d}".format(niter,len(mlist)))
                    if (verbose > 3):
                        print(mlist)
                if (verbose > 2):
                    print("Finished")
                if (verbose > 4):
                    for ifnd in mlist:
                        print(" {:6d} {:13.7f} {:13.7f} {:13.7f} {:13.7f} ".format(
                            ifnd,
                            BleedPerBand[band][ifnd]['ra_min'],
                            BleedPerBand[band][ifnd]['ra_max'],
                            BleedPerBand[band][ifnd]['dec_min'],
                            BleedPerBand[band][ifnd]['dec_max']))
                    for ifnd in mlist:
                        print("fk5;box({:13.7f},{:13.7f},{:13.7f}\",{:13.7f}\")".format(
                            BleedPerBand[band][ifnd]['ra_cen'],
                            BleedPerBand[band][ifnd]['dec_cen'],
                            3600.*BleedPerBand[band][ifnd]['ra_size']*np.cos(deg2rad*BleedPerBand[band][ifnd]['dec_cen']),
                            3600.*BleedPerBand[band][ifnd]['dec_size']))

                bleed_ra_min=np.array([BleedPerBand[band][ifnd]['ra_min'] for ifnd in mlist])
                bleed_ra_max=np.array([BleedPerBand[band][ifnd]['ra_max'] for ifnd in mlist])
                bleed_dec_min=np.array([BleedPerBand[band][ifnd]['dec_min'] for ifnd in mlist])
                bleed_dec_max=np.array([BleedPerBand[band][ifnd]['dec_max'] for ifnd in mlist])
                if (bleed_ra_min.size < 2):
                    NumOrphans=NumOrphans+1
                if (bleed_ra_min.size < MinFrame):
                    NumRejects=NumRejects+1
                else:
                    BDict={}
                    BDict['count']=bleed_ra_min.size
                    if (bleed_ra_min.size > 1):
                        BDict['ra_min']=np.amin(bleed_ra_min)
                        BDict['ra_max']=np.amax(bleed_ra_max)
                        BDict['dec_min']=np.amin(bleed_dec_min)
                        BDict['dec_max']=np.amax(bleed_dec_max)
                        BDict['mra_min']=np.median(bleed_ra_min)
                        BDict['mra_max']=np.median(bleed_ra_max)
                        BDict['mdec_min']=np.median(bleed_dec_min)
                        BDict['mdec_max']=np.median(bleed_dec_max)
                    else:    
                        BDict['ra_min']=bleed_ra_min[0]
                        BDict['ra_max']=bleed_ra_max[0]
                        BDict['dec_min']=bleed_dec_min[0]
                        BDict['dec_max']=bleed_dec_max[0]
                        BDict['mra_min']=bleed_ra_min[0]
                        BDict['mra_max']=bleed_ra_max[0]
                        BDict['mdec_min']=bleed_dec_min[0]
                        BDict['mdec_max']=bleed_dec_max[0]
                    BleedSet[band].append(BDict)

        print("Integration Execution Time: {:.2f}".format(time.time()-t0))

        ######################
        # Summarize result

        print("Integrated list of BleedTrails at {:s}-band".format(band))
        print("  Discrete Trails: {:d}".format(len(BleedSet[band])))
        print("          Rejects: {:d} (MinFrame<{:d})".format(NumRejects,MinFrame))
        print("          Orphans: {:d} (i.e. singular)".format(NumOrphans))

        ######################
        # Finished normal (now check in on the EdgeBleeds))

        if (SkipEdgeBleed):
            print("Skipping check on edgebleeds")
        else:
            if (len(EdgeBleedPerBand[band])>0):
                print("Working on EdgeBleeds")
                if (verbose > 3):
                    for Bleed in EdgeBleedPerBand[band]:
                        print(" {:13.7f} {:13.7f} {:13.7f} {:13.7f} {:13.7f} {:13.7f} {:13.7f} {:13.7f} ".format(
                            Bleed['ra_cen'],Bleed['dec_cen'],
                            Bleed['ra_size'],Bleed['dec_size'],
                            Bleed['ra_min'],Bleed['ra_max'],
                            Bleed['dec_min'],Bleed['dec_max']))

                bleed_ra_min=np.array([BleedPerBand[band][ifnd]['ra_min'] for ifnd in range(len(EdgeBleedPerBand[band])) ])
                bleed_ra_max=np.array([BleedPerBand[band][ifnd]['ra_max'] for ifnd in range(len(EdgeBleedPerBand[band])) ])
                bleed_dec_min=np.array([BleedPerBand[band][ifnd]['dec_min'] for ifnd in range(len(EdgeBleedPerBand[band])) ])
                bleed_dec_max=np.array([BleedPerBand[band][ifnd]['dec_max'] for ifnd in range(len(EdgeBleedPerBand[band])) ])
                BDict={}
                BDict['count']=bleed_ra_min.size
                if (bleed_ra_min.size > 1):
                    BDict['ra_min']=np.amin(bleed_ra_min)
                    BDict['ra_max']=np.amax(bleed_ra_max)
                    BDict['dec_min']=np.amin(bleed_dec_min)
                    BDict['dec_max']=np.amax(bleed_dec_max)
                    BDict['mra_min']=np.median(bleed_ra_min)
                    BDict['mra_max']=np.median(bleed_ra_max)
                    BDict['mdec_min']=np.median(bleed_dec_min)
                    BDict['mdec_max']=np.median(bleed_dec_max)
                else:    
                    BDict['ra_min']=bleed_ra_min[0]
                    BDict['ra_max']=bleed_ra_max[0]
                    BDict['dec_min']=bleed_dec_min[0]
                    BDict['dec_max']=bleed_dec_max[0]
                    BDict['mra_min']=bleed_ra_min[0]
                    BDict['mra_max']=bleed_ra_max[0]
                    BDict['mdec_min']=bleed_dec_min[0]
                    BDict['mdec_max']=bleed_dec_max[0]
                BleedSet[band].append(BDict)
                print("  EdgeBleed Added: (consolidated as one)")

#   Finished band
    print("    Execution Time: {:.2f}".format(time.time()-t00))

    return BleedSet


######################################################################################
######################################################################################
######################################################################################

if __name__ == "__main__":

    import argparse
    import os
    import despydb.desdbi
#    import stat
    import time
    import re
    import sys
    import numpy as np
    import intgutils.queryutils as queryutils
#    import multiepoch_appintg.coadd_query as me
    
    parser = argparse.ArgumentParser(description='Query code to obtain image inputs for COADD/multiepoch pipelines.')
    parser.add_argument('-p', '--proctag',  action='store', type=str, default=None,
                        help='Processing Tag from which to draw COADD inputs (default=None)')
    parser.add_argument('-t', '--tile',     action='store', type=str, required=None,
                        help='COADD tile name for which to asssemble inputs (default=None)')
    parser.add_argument('-A', '--attemptID', action='store', type=int, default=None, 
                        help='Alternate (PFW_ATTEMPT_ID) for tile processing attempt (for untagged data or mid-proceess use)')
    parser.add_argument('-b', '--band', action='store', type=str, required=True,
                        help='Band being considered')
    parser.add_argument('-o', '--outfile',  action='store', type=str, required=True, 
                        help='Output region file to be returned')
#    parser.add_argument('--exclude_list',  action='store', type=str, default='EXCLUDE_LIST', 
#                        help='EXCLUDE_LIST table to use in queries. (Default=EXCLUDE_LIST, "NONE", results in no exclude list constraint')
    parser.add_argument('--skipedgebleed', action='store_true', default=False, 
                        help='Switch to turn off consideration of edgebleeds')
    parser.add_argument('--thinpix',       action='store', type=float, default=3.3, 
                        help='Remove thin small trails (hot-pixel bad columns), default=3.3 pixels')
    parser.add_argument('--minframe',      action='store', type=int, default=3, 
                        help='Minimum number of overlapping trails required, default=3')
    parser.add_argument('-s', '--section', action='store', type=str, default=None, 
                        help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',  action='store', type=str, default=None, 
                        help='DB schema (do not include \'.\').')
    parser.add_argument('-v', '--verbose', action='store', type=int, default=0,
                        help='Verbosity (defualt:0; currently values up to 2)')

    args = parser.parse_args()
    if (args.verbose):
        print("Args: ",args)

    verbose=args.verbose

    if (args.Schema is None):
        dbSchema=""
    else:
        dbSchema="%s." % (args.Schema)

    if (args.tile is None):
        TileList=None
    else:
        TileList=[]
        tmp_list=args.tile.split(',')
        for tmp_entry in tmp_list:
            TileList.append(tmp_entry)

    if (args.proctag is None):
        if (args.attemptID is None):
            print("Must specify either an attempt ID or a proctag and tile")
            print("Aborting!")
            exit(1)
        else:
            print("Working from PFW_ATTEMPT_ID: {:d}".format(args.attemptID))
    else:
        if (args.attemptID is None):
            print("Working from proctag, tile(s): {:s} {:}".format(args.proctag,TileList))
        else:
            print("Working from PFW_ATTEMPT_ID: {:d}".format(args.attemptID))
        
    pi=3.141592654
    halfpi=pi/2.0
    deg2rad=pi/180.0

#   Finished rationalizing input
########################################################
#
#   Setup a DB connection
#
    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = despydb.desdbi.DesDbi(desdmfile,args.section)
#    cur = dbh.cursor()

    TileDict={}
    TileDict=query_coadd_geometry(TileDict,TileList,args.proctag,dbh,dbSchema,PFWID=args.attemptID,verbose=verbose)
    if (TileList is None):
        TileList=[]
        for Tile in TileDict:
            TileList.append(Tile)

    AIDList=[]
    for key in TileDict:
#        print(TileDict[key])
        AIDList.append([TileDict[key]['pfw_attempt_id']])

    t0=time.time()
    BleedDict={}
    BleedDict=query_coadd_bleed(AIDList,dbh,dbSchema,verbose)
    print("BleedTrails acquired by query of image inputs tile={:}".format(TileList))
    print("    Execution Time: {:.2f}".format(time.time()-t0))
    print("    BleedDict size: {:d}".format(len(BleedDict)))

    dbh.close()


#    ftxt=open("%s.satstars.dat"%(args.outfile),'w') 
#    ftxt.write("# {:s} \n".format(args.tile))
#    ftxt.write("# num RA[deg] DEC[deg] band Radius[arcsec] \n")
#    fall=open("%s.all.reg"%(args.outfile),'w')
    for Tile in TileDict:
        if (Tile in BleedDict):
            BleedSet=work_bleedlist(Tile,BleedDict,TileDict,SkipEdgeBleed=args.skipedgebleed,
                                    ThinPix=args.thinpix,MinFrame=args.minframe,verbose=verbose)

##          SUPERHACK to get running in production
            if (args.band not in BleedSet):
                freg=open(args.outfile,'w') 
                freg.write("# bleed trail region for {:s} {:s}-band \n".format(Tile,args.band))
                freg.close()
            else:
                band=args.band
#                ftxt=open("{:s}.{:s}.dat".format(args.outfile,band),'w') 
#                ftxt.write("# bleed trail region for {:s} {:s}-band".format(Tile,band))
                freg=open(args.outfile,'w') 
                freg.write("# bleed trail region for {:s} {:s}-band \n".format(Tile,band))

                cnt=0
                for Bleed in BleedSet[band]:
                    cnt=cnt+1
#                    ftxt.write(" {:5d} {:11.7f} {:11.7f} {:11.7f} {:11.7f} \n".format(
#                        cnt,Bleed['ra_min'],Bleed['ra_max'],Bleed['dec_min'],Bleed['dec_max']))

                    ra_cen=0.5*(Bleed['ra_min']+Bleed['ra_max'])
                    dec_cen=0.5*(Bleed['dec_min']+Bleed['dec_max'])
                    ra_size=3600.*(Bleed['ra_max']-Bleed['ra_min'])*np.cos(deg2rad*dec_cen)
                    dec_size=3600.*(Bleed['dec_max']-Bleed['dec_min'])

#                    freg.write(" fk5;box({:.7f},{:.7f},{:.2f}\",{:.2f}\") # color=red width=2 text={{{:d}}}\n".format(
#                        ra_cen,dec_cen,ra_size,dec_size,Bleed['count']))
                    freg.write(" fk5;polygon({:.7f},{:.7f},".format(Bleed['ra_min'],Bleed['dec_min']))
                    freg.write("{:.7f},{:.7f},".format(Bleed['ra_max'],Bleed['dec_min']))
                    freg.write("{:.7f},{:.7f},".format(Bleed['ra_max'],Bleed['dec_max']))
                    freg.write("{:.7f},{:.7f}) # color=red width=2 \n".format(Bleed['ra_min'],Bleed['dec_max']))

#                cnt=0
#                for Bleed in BleedSet[band]:
#                    cnt=cnt+1
##                    ftxt.write(" {:5d} {:11.7f} {:11.7f} {:11.7f} {:11.7f} \n".format(
##                        cnt,Bleed['mra_min'],Bleed['mra_max'],Bleed['mdec_min'],Bleed['mdec_max']))
#
#                    ra_cen=0.5*(Bleed['mra_min']+Bleed['mra_max'])
#                    dec_cen=0.5*(Bleed['mdec_min']+Bleed['mdec_max'])
#                    ra_size=3600.*(Bleed['mra_max']-Bleed['mra_min'])*np.cos(deg2rad*dec_cen)
#                    dec_size=3600.*(Bleed['mdec_max']-Bleed['mdec_min'])
#
#                    freg.write(" fk5;box({:.7f},{:.7f},{:.2f}\",{:.2f}\") # color=blue width=2 text={{{:d}}}\n".format(
#                        ra_cen,dec_cen,ra_size,dec_size,Bleed['count']))
#                    freg.write(" fk5;polygon({:.7f},{:.7f},".format(Bleed['mra_min'],Bleed['mdec_min']))
#                    freg.write("{:.7f},{:.7f},".format(Bleed['mra_max'],Bleed['mdec_min']))
#                    freg.write("{:.7f},{:.7f},".format(Bleed['mra_max'],Bleed['mdec_max']))
#                    freg.write("{:.7f},{:.7f}) # color=red width=2 \n".format(Bleed['mra_min'],Bleed['mdec_max']))

#                ftxt.close()
                freg.close()

    exit(0)


