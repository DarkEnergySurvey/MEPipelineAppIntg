# Simple set of function to generate global, chunk seeds and other utils for ngmixer

import numpy
import re
import fitsio
import math

def get_globalseed(tilename,shift=''):

    """ Go from TILENAME to unique interger by removing all non
    numeric values"""
    result = re.sub('[^0-9]','', tilename)
    seed = result + shift
    return int(seed)

def chunkseed(tilename,chunk, shift=''):
    tile_seed = get_globalseed(tilename,shift)
    rng = numpy.random.RandomState(tile_seed)
    k = 1
    while k <= chunk:
        newseed = rng.randint(low=1,high=1000000000)
        k=k+1
    return newseed

def find_number_fof(filename,ext):

    fofs = fitsio.read(filename)
    num = numpy.unique(fofs['fofid']).size
    return num

def getrange(n,nfof,nranges):

    chunck_size = math.ceil( float(nfof)/float(nranges) )
    chunck_size = int(chunck_size)
    j1 = (n-1)*chunck_size
    j2 = n*chunck_size - 1
    # Make sure we won't assing more jobs
    if j2 > nfof-1:
        j2 = nfof-1
    return j1,j2


if __name__ == "__main__":

    tilename = 'DES0102+5675'
    print tilename,get_globalseed(tilename,'69')
    print tilename,get_globalseed(tilename)
    tilename = 'DES2302+5675'
    print tilename,get_globalseed(tilename,'69')
    print tilename,get_globalseed(tilename)



    print chunkseed(tilename,12)
    print chunkseed(tilename,12)
    print chunkseed(tilename,20)
    print chunkseed(tilename,20)
    print chunkseed(tilename,12)
    #chunkseed(tilename,12)
