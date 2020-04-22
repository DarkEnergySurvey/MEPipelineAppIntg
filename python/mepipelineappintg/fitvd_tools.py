""" Simple set of function to generate global,
    chunk seeds and other utils for ngmixer
"""

import re
import math
import os
import fitvd
import numpy
import fitsio


def get_globalseed(tilename, shift=''):

    """ Go from TILENAME to unique interger by removing all non
    numeric values"""
    result = re.sub('[^0-9]', '', tilename)
    seed = result + shift
    return int(seed)


def chunkseed(tilename, chunk, shift=''):
    tile_seed = get_globalseed(tilename, shift)
    rng = numpy.random.RandomState(tile_seed)
    k = 1
    while k <= chunk:
        newseed = rng.randint(low=1, high=1000000000)
        k = k+1
    return newseed


def find_number_fof(filename, ext='fofid'):
    fofs = fitsio.read(filename)
    num = numpy.unique(fofs[ext]).size
    return num


def find_number_meds(filename):
    meds = fitsio.FITS(filename)
    num = meds['object_data'].get_nrows()
    return int(num)


def getrange(n, nfof, nranges):

    chunck_size = math.ceil(float(nfof)/float(nranges))
    chunck_size = int(chunck_size)
    j1 = (n-1)*chunck_size
    j2 = n*chunck_size - 1
    # Make sure we won't assing more jobs
    if j2 > nfof-1:
        j2 = nfof-1
    return j1, j2


def getrange_dynamical(n, fof_file, nranges, threshold):

    """Get the dynamical range for n, we compute the full set and only
    select the one we need"""
    fofs = fitsio.read(fof_file)
    splits = fitvd.split.get_splits_variable_fixnum(fofs, nranges, threshold)
    (j1, j2) = splits[n-1]
    return j1, j2


def read_meds_list(filename):
    """Reads and return a dictionary with meds filenames by band"""
    fnames = {}
    for line in open(filename).readlines():
        if line[0] == "#":
            continue
        band = line.split()[1]
        fnames[band] = line.split()[0]
    return fnames


def read_psf_list(filename):

    """Reads and return a dictionary with meds filenames by band"""
    fnames = {}
    for line in open(filename).readlines():
        if line[0] == "#":
            continue
        band = line.split()[-1]
        fnames[band] = []
        fnames[band].append(' '.join(line.split()[0:-1]))
    return fnames


def make_psf_map_files(filename):
    fnames = read_psf_list(filename)
    psf_map = {}
    for key in fnames:
        psf_map[key] = "%s_%s.list" % (os.path.splitext(filename)[0], key)
        # Open list for writing
        flist = open(psf_map[key], 'w')
        for l in fnames[key]:
            flist.write("%s\n" % l)
        flist.close()
    return psf_map


def parse_comma_separated_list(inputlist):
    if ',' in inputlist[0]:
        return inputlist[0].split(',')
    return inputlist
