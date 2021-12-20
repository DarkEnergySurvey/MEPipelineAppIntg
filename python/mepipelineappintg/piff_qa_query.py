import despyastro
import fitsio
import numpy

def get_piff_qa(fname,piff_tag,mAttID,dbh,dbSchema,verbose=0):
    """ Pull QA info for PIFF solutions (designated by piff_tag) that correspond to a given COADD tile (previous me_tag)
    """

    # Format and query with query2rec
    QUERY = """select q.* from {schema:s}piff_hsm_model_qa q, {schema:s}proctag t, {schema:s}miscfile m, {schema:s}image i
        where i.pfw_attempt_id={AID} and i.filetype='coadd_nwgint'
            and t.tag='{ptag:s}' and t.pfw_attempt_id=m.pfw_attempt_id and m.filetype='piff_model'
            and i.ccdnum=m.ccdnum and i.expnum=m.expnum and m.filename=q.filename """.format(AID=mAttID,ptag=piff_tag,schema=dbSchema)

    print("# Will query: ")
    print(QUERY)
    qa_data = despyastro.query2rec(QUERY, dbhandle=dbh, verb=True)

    # Make sure we get an answer, if no entries found query2rec() will return False
    if qa_data is False:
        raise ValueError("ERROR: Query to PIFF_HSM_MODEL_QA (tag={ptag:s}) returned no entries corrseponding to {tname:s} (tag={mtag:s}).".format(
            ptag=piff_tag,tname=tilename,mtag=me_tag))
    else:
        print("# Sucessfull query for PIFF_HSM_MODEL_QA")


    # check for nulls in DOF column (and change to -1.)

#    print(qa_data['DOF'].size)
#    wsm=numpy.where(qa_data['DOF'] == None)
#    print(qa_data['DOF'][wsm].size)
#    qa_data['DOF'][wsm]=-1.
#    print(qa_data['DOF'].size)
#    print(qa_data)

    # Write a fits file with the record array
    fitsio.write(fname, qa_data, extname='PIFF_QA', clobber=True)
    print("# Wrote PIFF_HSM_MODEL_QA to: {ftab:s}".format(ftab=fname))

    return 0
