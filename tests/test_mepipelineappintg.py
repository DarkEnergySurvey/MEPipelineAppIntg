#!/usr/bin/env python3

import unittest
import os
import stat
import sys
import mock
import copy
import argparse
from mock import patch, MagicMock
from contextlib import contextmanager
from io import StringIO
import re
from pathlib import Path
from collections import OrderedDict
from astropy.io import fits
import fitsio

from MockDBI import MockConnection
os.environ['MEPIPELINEAPPINTG_DIR'] = os.getcwd()
sys.modules['fitvd'] = mock.Mock()

import mepipelineappintg.ngmixit_tools as ngmt
import mepipelineappintg.fitvd_tools as fvdt
import mepipelineappintg.mepochmisc as mem
import mepipelineappintg.meds_query as mq
import mepipelineappintg.meappintg_tools as met
import mepipelineappintg.coadd_query as cq
import run_shredx as rsx

from despydb import desdbi

@contextmanager
def capture_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err

class Junk:
    def __init__(self):
        pass
    def get_nrows(self):
        return 1001

class TestGeneral(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sfile = 'services.ini'
        cls.files = [cls.sfile]
        open(cls.sfile, 'w').write("""

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))
        cls.result = None
        cls.output = None

    @classmethod
    def tearDownClass(cls):
        for fl in cls.files:
            try:
                os.unlink(fl)
            except:
                pass
        MockConnection.destroy()

    def runtest(self, func, args, tests):
        #with capture_output() as (self.output, _):
        self.result = func(*args)
        for t in tests:
            t.run(self.result)
                #func, arg1, arg2, msg = t
                #res1 = None
                #res2 = None
                #if callable(arg1):
                #    res1 = arg1(self.result)
                #else:
                #    res1 = arg1
                #if callable(arg2):
                #    res2 = arg2(self.result)
                #else:
                #    res2 = arg2
                #func(res1, res2, msg)

RESULT = "RESULT"
GET = "GET"
class TestFunc:
    def __init__(self, func, arg1, arg2=None, msg=None):
        self.func = func
        self.arg1 = arg1
        self.arg2 = arg2
        self.msg = msg
        self.result = None

    def process(self, arg):
        if isinstance(arg, TestFunc):
            #print('Executing1')
            return arg.run(self.result)
        elif arg == RESULT:
            return self.result
        else:
            return arg

    def run(self, result):
        self.result = result
        r1 = self.process(self.arg1)
        r2 = self.process(self.arg2)
        #print('rr1 ' + str(r1))
        #print('rr2 ' + str(r2))
        #if isinstance(self.arg1, TestFunc):
        #    #print('Executing1')
        #    r1 = self.arg1.run(result)
        #elif self.arg1 == RESULT:
        #    r1 = result
        #else:
        #    r1 = self.arg1
        #if isinstance(self.arg2, TestFunc):
        #    r2 = self.arg2.run(result)
        #elif self.arg2 == RESULT:
        #    r2 = result
        #else:
        #    r2 = self.arg2
        #print('\n\n--------\n\n' + str(r1) + '  ' + str(r2))
        if self.func == GET:
            return r1.get(r2)
        if r2 is None:
            if self.msg is None:
                return self.func(r1)
            else:
                return self.func(r1, msg)
        else:
            if self.msg is None:
                return self.func(r1, r2)
            else:
                return self.func(r1, r2, self.msg)




class TestNgmixitTools(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fname = 'meds.list'
        open(cls.fname, 'w').write("""# filename  band
test_r.meds r
test_Y.meds Y
""")

    @classmethod
    def tearDownClass(cls):
        try:
            os.unlink(cls.fname)
        except:
            pass

    def test_get_globalseed(self):
        tid = 996644
        tilename = f'DES-{tid:08d}'
        res = ngmt.get_globalseed(tilename)
        self.assertTrue(isinstance(res, int))
        self.assertEqual(tid, res)

        tilename = f'DES-{tid:d}-A'
        res = ngmt.get_globalseed(tilename)
        self.assertTrue(isinstance(res, int))
        self.assertEqual(tid, res)

        res = ngmt.get_globalseed(tilename, '0')
        self.assertTrue(isinstance(res, int))
        self.assertEqual(tid * 10, res)

    def test_chunkseed(self):
        tilename = 'DES-1234886'
        self.assertTrue(isinstance(ngmt.chunkseed(tilename, 10), int))
        self.assertTrue(isinstance(ngmt.chunkseed(tilename, 10, '5'), int))
        self.assertTrue(isinstance(ngmt.chunkseed(tilename, 1000), int))

    def test_find_number_fof(self):
        with patch('mepipelineappintg.ngmixit_tools.fitsio.read', return_value={'fofid': [1,1,3,4,8,3]}):
            res = ngmt.find_number_fof(None, None)
            self.assertEqual(res, 4)

    def test_find_number_meds(self):
        with patch('mepipelineappintg.ngmixit_tools.fitsio.FITS', return_value={'object_data': Junk()}):
            res = ngmt.find_number_meds(None)
            self.assertEqual(res, 1001)

    def test_getrange(self):
        self.assertEqual((12, 5), ngmt.getrange(5, 6, 2))
        self.assertEqual((4, 4), ngmt.getrange(5, 6, 7))

    def test_read_meds_list(self):
        res = ngmt.read_meds_list(self.fname)
        self.assertTrue('r' in res.keys())
        self.assertTrue('Y' in res.keys())
        self.assertEqual('test_r.meds', res['r'])

    def test_parse_comma_separated_list(self):
        a = ['1,2,3,4,5', 9, 0]
        res = ngmt.parse_comma_separated_list(a)
        self.assertEqual(len(res), 5)
        self.assertTrue('2' in res)
        a = ['1', '2', '3']
        res = ngmt.parse_comma_separated_list(a)
        self.assertEqual(res, a)

class TestFitvdTools(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fname = 'meds.list'
        open(cls.fname, 'w').write("""# filename  band
test_r.meds r
test_Y.meds Y
""")
        cls.pfname = 'psf.list'
        open(cls.pfname, 'w').write("""# filename(s)   band
test_r.psf test2_r.psf r
test_Y.psf Y
""")
        cls.files = [cls.fname, cls.pfname]

    @classmethod
    def tearDownClass(cls):
        for i in cls.files:
            try:
                os.unlink(i)
            except:
                pass

    def test_get_globalseed(self):
        tid = 996644
        tilename = f'DES-{tid:08d}'
        res = fvdt.get_globalseed(tilename)
        self.assertTrue(isinstance(res, int))
        self.assertEqual(tid, res)

        tilename = f'DES-{tid:d}-A'
        res = fvdt.get_globalseed(tilename)
        self.assertTrue(isinstance(res, int))
        self.assertEqual(tid, res)

        res = fvdt.get_globalseed(tilename, '0')
        self.assertTrue(isinstance(res, int))
        self.assertEqual(tid * 10, res)

    def test_chunkseed(self):
        tilename = 'DES-1234886'
        self.assertTrue(isinstance(fvdt.chunkseed(tilename, 10), int))
        self.assertTrue(isinstance(fvdt.chunkseed(tilename, 10, '5'), int))
        self.assertTrue(isinstance(fvdt.chunkseed(tilename, 1000), int))

    def test_find_number_fof(self):
        with patch('mepipelineappintg.fitvd_tools.fitsio.read', return_value={'fofid': [1,1,3,4,8,3]}):
            res = fvdt.find_number_fof(None)
            self.assertEqual(res, 4)

    def test_find_number_meds(self):
        with patch('mepipelineappintg.fitvd_tools.fitsio.FITS', return_value={'object_data': Junk()}):
            res = fvdt.find_number_meds(None)
            self.assertEqual(res, 1001)

    def test_getrange(self):
        self.assertEqual((12, 5), fvdt.getrange(5, 6, 2))
        self.assertEqual((4, 4), fvdt.getrange(5, 6, 7))

    def test_getrange_dynamical(self):
        with patch('mepipelineappintg.fitvd_tools.fitsio.read', return_value={'fofid': [1,1,3,4,8,3]}):
            with patch('mepipelineappintg.fitvd_tools.fitvd.split.get_splits_variable_fixnum', return_value=[[1, 2], [3, 4], [5, 6]]):
                res = fvdt.getrange_dynamical(2, None, None, None)
                self.assertEqual(res, (3, 4))

    def test_read_meds_list(self):
        res = fvdt.read_meds_list(self.fname)
        self.assertTrue('r' in res.keys())
        self.assertTrue('Y' in res.keys())
        self.assertEqual('test_r.meds', res['r'])

    def test_read_psf_list(self):
        res = fvdt.read_psf_list(self.pfname)
        self.assertTrue('r' in res.keys())
        self.assertTrue('Y' in res.keys())
        self.assertTrue('test_r.psf' in res['r'][0])
        self.assertTrue('test2_r.psf' in res['r'][0])

    def test_make_psf_map_files(self):
        res = fvdt.make_psf_map_files(self.pfname)
        self.assertTrue('r' in res.keys())
        self.assertTrue('Y' in res.keys())
        for fl in res.values():
            self.assertTrue(os.path.isfile(fl))
            self.files.append(fl)

    def test_parse_comma_separated_list(self):
        a = ['1,2,3,4,5', 9, 0]
        res = fvdt.parse_comma_separated_list(a)
        self.assertEqual(len(res), 5)
        self.assertTrue('2' in res)
        a = ['1', '2', '3']
        res = fvdt.parse_comma_separated_list(a)
        self.assertEqual(res, a)


class TestMepochmisc(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sfile = 'services.ini'
        cls.files = [cls.sfile]
        open(cls.sfile, 'w').write("""

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))

    @classmethod
    def tearDownClass(cls):
        for fl in cls.files:
            try:
                os.unlink(fl)
            except:
                pass
        MockConnection.destroy()

    def test_get_tile_info(self):
        tilename = 'TEST0000+2917'
        res = mem.get_tile_info({'submit_des_services': self.sfile,
                                 'submit_des_db_section': 'db-test',
                                 'tilename': tilename})
        self.assertEqual(len(res), 11)
        self.assertEqual(10004, res['tileid'])

    def test_write_textlist(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        dict_input1 = {'f1': {'compression': None,
                              'filename': 'File1.dat',
                              'path': 'test/path',
                              'band': 'r',
                              'expnum': 100},
                       'f2': {'compression': '.fz',
                              'filename': 'test.fits',
                              'path': 'test/path',
                              'band': 'g',
                              'expnum': 101}
                       }
        dict_input2 = {'f3': {'compression': '.gz',
                              'filename': 'testout.dat',
                              'path': 'test2/path',
                              'expnum': 102,
                              'ccdnum': 16}
                       }

        outfile1 = 'test1.txt'
        outfile2 = 'test2.txt'
        self.files.append(outfile1)
        self.files.append(outfile2)

        self.assertFalse(os.path.exists(outfile1))
        mem.write_textlist(dbh, dict_input1, outfile1)
        self.assertTrue(os.path.exists(outfile1))
        self.assertTrue(Path(outfile1).stat().st_size > 0)

        self.assertFalse(os.path.exists(outfile2))
        with capture_output() as (out, _):
            mem.write_textlist(dbh, dict_input2, outfile2, fields=['fullname', 'pexpnum', 'ngmixid'], verb=True)
            output = out.getvalue().strip()
            self.assertTrue('Wrote file' in output)
            self.assertTrue(os.path.exists(outfile2))
            self.assertGreater(Path(outfile2).stat().st_size, 0)

    def test_get_root_archive(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')

        res = mem.get_root_archive(dbh)
        self.assertEqual(res, '/decade/decarchive')
        with capture_output() as (out, _):
            res = mem.get_root_archive(dbh,verb=True)
            output = out.getvalue().strip()
            self.assertTrue('SELECT' in output)

    def test_find_tile_attempt(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        with capture_output() as (out, _):
            self.assertIsNone(mem.find_tile_attempt('THETILE2', 'Y6A1_COADD_INPUT', dbh, ''))
            output = out.getvalue().strip()
            self.assertTrue('First attempt' in output)

        with capture_output() as (out, _):
            self.assertIsNone(mem.find_tile_attempt('THETILE2', 'Y6A1_COADD_INPUT', dbh, '', verbose=1))
            output = out.getvalue().strip()
            self.assertTrue('First attempt' in output)
            self.assertTrue('SELECT' in output)
            self.assertTrue('     ' not in output)

        with capture_output() as (out, _):
            self.assertIsNone(mem.find_tile_attempt('THETILE2', 'Y6A1_COADD_INPUT', dbh, '', verbose=2))
            output = out.getvalue().strip()
            self.assertTrue('First attempt' in output)
            self.assertTrue('SELECT' in output)
            self.assertTrue('     ' in output)

        with capture_output() as (out, _):
            self.assertIsNone(mem.find_tile_attempt('THETILE2', 'Y6A1_COADD_INPUT', dbh, '', Timing=True))
            output = out.getvalue().strip()
            self.assertTrue('First attempt' in output)
            self.assertTrue('execution time' in output)

        with capture_output() as (out, _):
            res = mem.find_tile_attempt('THETILE', 'Y6A1_COADD_INPUT', dbh, '', Timing=True)
            self.assertTrue(isinstance(res, int))
            self.assertTrue(res > 0)
            output = out.getvalue().strip()
            self.assertTrue('First attempt' in output)
            self.assertTrue('Found more than' in output)

        res = mem.find_tile_attempt('THETILE', 'TEST_COADD', dbh, '', Timing=True)
        self.assertEqual(res, 512)


class TestMeds_query(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sfile = 'services.ini'
        cls.files = [cls.sfile]
        open(cls.sfile, 'w').write("""

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))

    @classmethod
    def tearDownClass(cls):
        for fl in cls.files:
            try:
                os.unlink(fl)
            except:
                pass
        MockConnection.destroy()

    def test_query_imgs_from_attempt(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        with capture_output() as (out, _):
            imd, hd = mq.query_imgs_from_attempt('2309774', dbh, '')
            output = out.getvalue().strip()
            self.assertTrue('No entry' in output)
            self.assertEqual(len(imd), len(hd))
            self.assertEqual(len(imd), 422)
            count = 0
            for img in hd.keys():
                if 'path' not in hd[img]:
                    count += 1
            self.assertEqual(count, 1)

        with capture_output() as (out, _):
            imd, hd = mq.query_imgs_from_attempt('2309774', dbh, '', verbose=1)
            output = out.getvalue().strip()
            self.assertTrue('sql =' in output)

        with capture_output() as (out, _):
            imd, hd = mq.query_imgs_from_attempt('2309774', dbh, '', verbose=2)
            output = out.getvalue().strip()
            self.assertTrue('sql =' in output)

class TestMeappintgTools(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fname = 'meds.list'
        open(cls.fname, 'w').write("""# filename  band
test_r.meds r
test_Y.meds Y
""")
        cls.pfname = 'psf.list'
        open(cls.pfname, 'w').write("""# filename(s)   band
test_r.psf test2_r.psf r
test_Y.psf Y
""")
        cls.files = [cls.fname, cls.pfname]


    @classmethod
    def tearDownClass(cls):
        for i in cls.files:
            try:
                os.unlink(i)
            except:
                pass

    def test_get_globalseed(self):
        tid = 996644
        tilename = f'DES-{tid:08d}'
        res = met.get_globalseed(tilename)
        self.assertTrue(isinstance(res, int))
        self.assertEqual(tid, res)

        tilename = f'DES-{tid:d}-A'
        res = met.get_globalseed(tilename)
        self.assertTrue(isinstance(res, int))
        self.assertEqual(tid, res)

        res = met.get_globalseed(tilename, '0')
        self.assertTrue(isinstance(res, int))
        self.assertEqual(tid * 10, res)

    def test_chunkseed(self):
        tilename = 'DES-1234886'
        self.assertTrue(isinstance(met.chunkseed(tilename, 10), int))
        self.assertTrue(isinstance(met.chunkseed(tilename, 10, '5'), int))
        self.assertTrue(isinstance(met.chunkseed(tilename, 1000), int))

    def test_find_number_fof(self):
        with patch('mepipelineappintg.meappintg_tools.fitsio.read', return_value={'fofid': [1,1,3,4,8,3]}):
            res = met.find_number_fof(None, None)
            self.assertEqual(res, 4)

    def test_find_number_meds(self):
        with patch('mepipelineappintg.meappintg_tools.fitsio.FITS', return_value={'object_data': Junk()}):
            res = met.find_number_meds(None)
            self.assertEqual(res, 1001)

    def test_getrange(self):
        self.assertEqual((12, 5), met.getrange(5, 6, 2))
        self.assertEqual((4, 4), met.getrange(5, 6, 7))

    def test_read_meds_list(self):
        res = met.read_meds_list(self.fname)
        self.assertTrue('r' in res.keys())
        self.assertTrue('Y' in res.keys())
        self.assertEqual('test_r.meds', res['r'])

    def test_parse_comma_separated_list(self):
        a = ['1,2,3,4,5', 9, 0]
        res = met.parse_comma_separated_list(a)
        self.assertEqual(len(res), 5)
        self.assertTrue('2' in res)
        a = ['1', '2', '3']
        res = met.parse_comma_separated_list(a)
        self.assertEqual(res, a)

    def test_getrange_dynamical(self):
        with patch('mepipelineappintg.meappintg_tools.fitsio.read', return_value={'fofid': [1,1,3,4,8,3]}):
            with patch('mepipelineappintg.meappintg_tools.fitvd.split.get_splits_variable_fixnum', return_value=[[1, 2], [3, 4], [5, 6]]):
                res = fvdt.getrange_dynamical(2, None, None, None)
                self.assertEqual(res, (3, 4))

    def test_read_psf_list(self):
        res = met.read_psf_list(self.pfname)
        self.assertTrue('r' in res.keys())
        self.assertTrue('Y' in res.keys())
        self.assertTrue('test_r.psf' in res['r'][0])
        self.assertTrue('test2_r.psf' in res['r'][0])

    def test_make_psf_map_files(self):
        res = met.make_psf_map_files(self.pfname)
        self.assertTrue('r' in res.keys())
        self.assertTrue('Y' in res.keys())
        for fl in res.values():
            self.assertTrue(os.path.isfile(fl))
            self.files.append(fl)

class TestCoaddQuery(TestGeneral):
    #@classmethod
    #def setUpClass(cls):
    #    cls.sfile = 'services.ini'
    #    cls.files = [cls.sfile]
    #    open(cls.sfile, 'w').write("""
#
#[db-test]
#USER    =   Minimal_user
#PASSWD  =   Minimal_passwd
#name    =   Minimal_name
#sid     =   Minimal_sid
#server  =   Minimal_server
#type    =   test
#port    =   0
#""")
#        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))

#    @classmethod
#    def tearDownClass(cls):
#        for fl in cls.files:
#            try:
#                os.unlink(fl)
#            except:
#                pass
#        MockConnection.destroy()

    def test_query_coadd_geometry(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        tiles = ['TEST1112+3000', 'TEST1115+3000']
        tileDict = {}
        tileDict = cq.query_coadd_geometry(tileDict, tiles[0], dbh, '')
        self.assertEqual(len(tileDict), 1)
        self.assertTrue(tiles[0] in tileDict)
        self.assertTrue('ra_cent' in tileDict[tiles[0]])

        with capture_output() as (out, _):
            tileDict = cq.query_coadd_geometry(tileDict, tiles[1], dbh, '', verbose=1)
            self.assertEqual(len(tileDict), 2)
            self.assertTrue(tiles[1] in tileDict)
            self.assertTrue('ra_cent' in tileDict[tiles[1]])
            output = out.getvalue()
            self.assertTrue('sql' in output)
            self.assertTrue('SELECT' in output)

        with capture_output() as (out, _):
            tileDict = cq.query_coadd_geometry(tileDict, tiles[0], dbh, '', verbose=2)
            self.assertEqual(len(tileDict), 2)
            self.assertTrue(tiles[0] in tileDict)
            self.assertTrue('ra_cent' in tileDict[tiles[0]])
            output = out.getvalue()
            self.assertFalse('sql' in output)
            self.assertTrue('SELECT' in output)

    def test_query_coadd_img_by_edges(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        #zres = None
        tests = [TestFunc(self.assertGreater, TestFunc(len, RESULT), 0, 'Failed Here'),
                 TestFunc(self.assertIn, 'filename', TestFunc(GET, RESULT, TestFunc(next, TestFunc(iter, RESULT)))),
                 TestFunc(self.assertEqual, 'z', TestFunc(GET, TestFunc(GET, RESULT, TestFunc(next, TestFunc(iter, RESULT))), 'band'))]
        self.runtest(cq.query_coadd_img_by_edges,
                     [{}, 'TEST1118+3000', 'Y6A1_COADD_INPUT', ['z'], 'desar2home', dbh, '', 0],
                     tests)
                     #[[self.assertGreater, len, 1000, 'Failed here']])




        with capture_output() as (out, _):
            zres = cq.query_coadd_img_by_edges({}, 'TEST1118+3000', 'Y6A1_COADD_INPUT', ['z'], 'desar2home', dbh, '', verbose=2)
            #self.assertTrue(len(zres) > 0)
            key = list(zres.keys())[0]
            #self.assertTrue('filename' in zres[key])
            self.assertEqual('z', zres[key]['band'])
            output = out.getvalue().strip()
            self.assertTrue("Post query constraint" in output)

        #with capture_output() as (out, _):
        #    res2 = cq.query_coadd_img_by_edges(zres, 'TEST1118+3000', 'Y6A1_COADD_INPUT', ['z'], 'desar2home', dbh, '', verbose=1)
        #    self.assertEqual(res2, zres)
        #    output = out.getvalue().strip()
        #    self.assertTrue('sql' in output)

        #zres = cq.query_coadd_img_by_edges(zres, 'TEST1118+3000', 'Y6A1_COADD_INPUT', ['Y'], 'desar2home', dbh, '')
        #gres = cq.query_coadd_img_by_edges({}, 'TEST1118+3000', 'Y6A1_COADD_INPUT', ['g'], 'desar2home', dbh, '')
        #ires = cq.query_coadd_img_by_edges({}, 'TEST1118+3000', 'Y6A1_COADD_INPUT', ['i'], 'desar2home', dbh, '')
        #rres = cq.query_coadd_img_by_edges({}, 'TEST1118+3000', 'Y6A1_COADD_INPUT', ['r'], 'desar2home', dbh, '')

        #merged = {**zres, **gres, **ires, **rres}

        #res = cq.query_coadd_img_by_edges({}, 'TEST1118+3000', 'Y6A1_COADD_INPUT', ['g','r','i','z','Y'], 'desar2home', dbh, '')

        #self.assertDictEqual(merged, res)

    def test_query_coadd_img_by_fiat(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        zres = None
        with capture_output() as (out, _):
            zres = cq.query_coadd_img_by_fiat({}, 'TEST1118+3000', 'Y6A1_COADD_INPUT', ['z'], 'desar2home', 'testfiat', dbh, '', verbose=2)
            self.assertEqual(len(zres), 100)
            key = list(zres.keys())[0]
            self.assertTrue('filename' in zres[key])
            self.assertEqual('z', zres[key]['band'])
            output = out.getvalue().strip()
            self.assertTrue("Post query constraint" in output)

        with capture_output() as (out, _):
            res2 = cq.query_coadd_img_by_fiat(zres, 'TEST1118+3000', 'Y6A1_COADD_INPUT', ['z'], 'desar2home', 'testfiat', dbh, '', verbose=1)
            self.assertEqual(res2, zres)
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_by_fiat(zres, 'TEST1118+3000', 'Y6A1_COADD_INPUT', ['Y'], 'desar2home', 'testfiat', dbh, '')
        gres = cq.query_coadd_img_by_fiat({}, 'TEST1118+3000', 'Y6A1_COADD_INPUT', ['g'], 'desar2home', 'testfiat', dbh, '')
        ires = cq.query_coadd_img_by_fiat({}, 'TEST1118+3000', 'Y6A1_COADD_INPUT', ['i'], 'desar2home', 'testfiat', dbh, '')
        rres = cq.query_coadd_img_by_fiat({}, 'TEST1118+3000', 'Y6A1_COADD_INPUT', ['r'], 'desar2home', 'testfiat', dbh, '')

        merged = {**zres, **gres, **ires, **rres}

        res = cq.query_coadd_img_by_fiat({}, 'TEST1118+3000', 'Y6A1_COADD_INPUT', ['g','r','i','z','Y'], 'desar2home', 'testfiat', dbh, '')

        self.assertDictEqual(merged, res)

    def test_query_coadd_img_from_attempt(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        zres = None
        with capture_output() as (out, _):
            zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '', verbose=2)
            self.assertEqual(len(zres), 94)
            key = list(zres.keys())[0]
            self.assertTrue('filename' in zres[key])
            self.assertEqual('z', zres[key]['band'])
            output = out.getvalue().strip()
            self.assertTrue("Post query constraint" in output)

        with capture_output() as (out, _):
            res2 = cq.query_coadd_img_from_attempt(zres, 2309774, ['z'], 'desar2home', dbh, '', verbose=1)
            self.assertEqual(res2, zres)
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt(zres, 2309774, ['Y'], 'desar2home', dbh, '')
        gres = cq.query_coadd_img_from_attempt({}, 2309774, ['g'], 'desar2home', dbh, '')
        ires = cq.query_coadd_img_from_attempt({}, 2309774, ['i'], 'desar2home', dbh, '')
        rres = cq.query_coadd_img_from_attempt({}, 2309774, ['r'], 'desar2home', dbh, '')

        merged = {**zres, **gres, **ires, **rres}

        res = cq.query_coadd_img_from_attempt({}, 2309774, ['g','r','i','z','Y'], 'desar2home', dbh, '')

        self.assertDictEqual(merged, res)

    def test_query_zeropoint(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        zdict = {'table': 'ZEROPOINT',
                 'source': 'PGCM_FORCED'
                 }
        zdict2 = {'source': 'FGCM'}
                 #'version': None,
                 #'flag': None}


        zres['D00617212_z_c58_r2989p02_immasked.fits']['mag_zero'] = None
        with capture_output() as (out, _):
            zpr = cq.query_zeropoint(zres, zdict, None, dbh, '', verbose=2)
            self.assertTrue(len(zres) > len(zpr))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        with capture_output() as (out, _):
            zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
            zpr = cq.query_zeropoint(zres, zdict, zdict2, dbh, '', verbose=1)
            self.assertEqual(len(zres), len(zpr))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        del zdict['source']
        del zdict2['source']
        zdict['flag'] = 0
        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        zpr = cq.query_zeropoint(zres, zdict, None, dbh, '')
        self.assertTrue(len(zres) > len(zpr))
        zdict2['flag'] = 2
        with capture_output() as (out, _):
            zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
            zpr = cq.query_zeropoint(zres, zdict, zdict2, dbh, '', verbose=2)
            self.assertTrue(len(zres) > len(zpr))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        zpr = cq.query_zeropoint(zres, zdict, zdict2, dbh, '')
        self.assertTrue(len(zres) > len(zpr))


        del zdict['flag']
        zdict['version'] = '0.0'
        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zpr = cq.query_zeropoint(zres, zdict, None, dbh, '', verbose=3)
            self.assertTrue(len(zres) > len(zpr))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zdict2['version'] = 'y4a1_v1.5'
        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zpr = cq.query_zeropoint(zres, zdict, zdict2, dbh, '', verbose=3)
            self.assertTrue(len(zres) > len(zpr))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)


        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        self.assertRaises(KeyError, cq.query_zeropoint, zres, None, None, dbh, '')
        #self.assertEqual(len(zres), len(zpr))

    def test_query_bkg_img(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zb = cq.query_bkg_img(zres, 'desar2home', dbh, '', verbose=2)
            self.assertEqual(len(zres), len(zb))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zb = cq.query_bkg_img(zres, 'desar2home', dbh, '', verbose=1)
            self.assertEqual(len(zres), len(zb))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        zb = cq.query_bkg_img(zres, 'desar2home', dbh, '')
        self.assertEqual(len(zres), len(zb))

    def test_query_segmap(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zb = cq.query_segmap(zres, 'desar2home', dbh, '', verbose=2)
            self.assertEqual(len(zres), len(zb))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zb = cq.query_segmap(zres, 'desar2home', dbh, '', verbose=1)
            self.assertEqual(len(zres), len(zb))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        zb = cq.query_segmap(zres, 'desar2home', dbh, '')
        self.assertEqual(len(zres), len(zb))

    def test_query_psfmodel(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zb = cq.query_psfmodel(zres, 'desar2home', dbh, '', verbose=2)
            self.assertEqual(len(zres), len(zb))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zb = cq.query_psfmodel(zres, 'desar2home', dbh, '', verbose=1)
            self.assertEqual(len(zres), len(zb))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        zb = cq.query_psfmodel(zres, 'desar2home', dbh, '')
        self.assertEqual(len(zres), len(zb))

    def test_query_PIFFmodel(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zb = cq.query_PIFFmodel(zres, 'desar2home', dbh, '', 'Y6A1_COADD_TEST', verbose=2)
            self.assertEqual(len(zres), len(zb))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zb = cq.query_PIFFmodel(zres, 'desar2home', dbh, '', 'Y6A1_COADD_TEST', verbose=1)
            self.assertEqual(len(zres), len(zb))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        zb = cq.query_PIFFmodel(zres, 'desar2home', dbh, '', 'Y6A1_COADD_TEST')
        self.assertEqual(len(zres), len(zb))


    def test_query_headfile_from_attempt(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zb = cq.query_headfile_from_attempt(zres, 2309774, 'desar2home', dbh, '', verbose=2)
            self.assertEqual(len(zres) - 1, len(zb))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zb = cq.query_headfile_from_attempt(zres, 2309774, 'desar2home', dbh, '', verbose=1)
            self.assertEqual(len(zres) - 1, len(zb))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        zb = cq.query_headfile_from_attempt(zres, 2309774, 'desar2home', dbh, '')
        self.assertEqual(len(zres) - 1, len(zb))

    def test_cat_finalcut(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zb = cq.query_catfinalcut(zres, 'desar2home', dbh, '', verbose=2)
            self.assertEqual(len(zres), len(zb))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zb = cq.query_catfinalcut(zres, 'desar2home', dbh, '', verbose=1)
            self.assertEqual(len(zres), len(zb))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        zb = cq.query_catfinalcut(zres, 'desar2home', dbh, '')
        self.assertEqual(len(zres), len(zb))

    def test_coadd_img_by_extent(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            ze = cq.query_coadd_img_by_extent(zres, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '',['z'], verbose=2)
            self.assertEqual(len(zres), len(ze))
            count = 0
            for v in ze.values():
                if 'ra_cent' in v.keys():
                    count += 1
            self.assertEqual(1, count)

        with capture_output() as (out, _):
            ze = cq.query_coadd_img_by_extent(zres, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '',['z'], verbose=1)
            self.assertEqual(len(zres), len(ze))
            count = 0
            for v in ze.values():
                if 'ra_cent' in v.keys():
                    count += 1
            self.assertEqual(1, count)

        ze = cq.query_coadd_img_by_extent(zres, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '',[])
        self.assertEqual(len(zres), len(ze))
        count = 0
        for v in ze.values():
            if 'ra_cent' in v.keys():
                count += 1
        self.assertEqual(1, count)

        ze = cq.query_coadd_img_by_extent(zres, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '',['z'])
        self.assertEqual(len(zres), len(ze))
        count = 0
        for v in ze.values():
            if 'ra_cent' in v.keys():
                count += 1
        self.assertEqual(1, count)

    def test_coadd_img_by_extent_cross(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            ze = cq.query_coadd_img_by_extent(zres, 'TESTC000+0000', 'Y6A1_COADD_TEST2', dbh, '',['z'], verbose=2)
            self.assertEqual(len(zres), len(ze))
            count = 0
            for v in ze.values():
                if 'ra_cent' in v.keys():
                    count += 1
            self.assertEqual(1, count)

        with capture_output() as (out, _):
            ze = cq.query_coadd_img_by_extent(zres, 'TESTC000+0000', 'Y6A1_COADD_TEST2', dbh, '',['z'], verbose=1)
            self.assertEqual(len(zres), len(ze))
            count = 0
            for v in ze.values():
                if 'ra_cent' in v.keys():
                    count += 1
            self.assertEqual(1, count)

        ze = cq.query_coadd_img_by_extent(zres, 'TESTC000+0000', 'Y6A1_COADD_TEST2', dbh, '',['z'])
        self.assertEqual(len(zres), len(ze))
        count = 0
        for v in ze.values():
            if 'ra_cent' in v.keys():
                count += 1
        self.assertEqual(1, count)

    def test_query_astref_scampcat(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        with capture_output() as (out, _):
            za = cq.query_astref_scampcat({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', ['z','i','g'], verbose=2)
            self.assertEqual(1, len(za))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        with capture_output() as (out, _):
            za = cq.query_astref_scampcat({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', ['z','i','g'], verbose=1)
            self.assertEqual(1, len(za))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        za = cq.query_astref_scampcat({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', ['z','i','g'])
        self.assertEqual(1, len(za))

        za = cq.query_astref_scampcat({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', [])
        self.assertEqual(1, len(za))

    def test_query_astref_scampcat_by_fiat(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        curs = dbh.cursor()
        curs.execute("""CREATE TABLE SCAMPCAT_FIAT ("FILENAME" TEXT NOT NULL, "TILENAME" TEXT NOT NULL, PRIMARY KEY ("FILENAME"))""")
        try:
            za = cq.query_astref_scampcat_by_fiat({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', ['z','i','g'], 'SCAMPCAT_FIAT', verbose=1)
            self.assertEqual(0, len(za))
            curs.execute("INSERT INTO SCAMPCAT_FIAT (FILENAME, TILENAME) VALUES ('D00397535_z_c58_r3496p01_immasked.fits', 'DES1002+0126')")
            with capture_output() as (out, _):
                za = cq.query_astref_scampcat_by_fiat({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', ['z','i','g'], 'SCAMPCAT_FIAT', verbose=2)
                self.assertEqual(1, len(za))
                output = out.getvalue().strip()
                self.assertTrue('sql' in output)

            with capture_output() as (out, _):
                za = cq.query_astref_scampcat_by_fiat({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', ['z','i','g'], 'SCAMPCAT_FIAT', verbose=1)
                self.assertEqual(1, len(za))
                output = out.getvalue().strip()
                self.assertTrue('sql' in output)

            za = cq.query_astref_scampcat_by_fiat({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', ['z','i','g'], 'SCAMPCAT_FIAT')
            self.assertEqual(1, len(za))
            za = cq.query_astref_scampcat_by_fiat({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', [], 'SCAMPCAT_FIAT')
            self.assertEqual(1, len(za))
        finally:
            curs.execute("rollback")

    def test_query_astref_catfinalcut(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        with capture_output() as (out, _):
            za = cq.query_astref_catfinalcut({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', ['z','i','g'], verbose=2)
            self.assertEqual(1, len(za))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        with capture_output() as (out, _):
            za = cq.query_astref_catfinalcut({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', ['z','i','g'], verbose=1)
            self.assertEqual(1, len(za))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        za = cq.query_astref_catfinalcut({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', ['z','i','g'])
        self.assertEqual(1, len(za))

        za = cq.query_astref_catfinalcut({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', [])
        self.assertEqual(1, len(za))

    def test_query_astref_catfinalcut_by_fiat(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        curs = dbh.cursor()
        curs.execute("""CREATE TABLE SCAMPCAT_FIAT ("FILENAME" TEXT NOT NULL, "TILENAME" TEXT NOT NULL, PRIMARY KEY ("FILENAME"))""")
        try:
            za = cq.query_astref_catfinalcut_by_fiat({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', ['z','i','g'], 'SCAMPCAT_FIAT', verbose=1)
            self.assertEqual(0, len(za))
            curs.execute("INSERT INTO SCAMPCAT_FIAT (FILENAME, TILENAME) VALUES ('D00397535_z_c58_r3496p01_immasked.fits', 'DES1002+0126')")
            with capture_output() as (out, _):
                za = cq.query_astref_catfinalcut_by_fiat({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', ['z','i','g'], 'SCAMPCAT_FIAT', verbose=2)
                self.assertEqual(1, len(za))
                output = out.getvalue().strip()
                self.assertTrue('sql' in output)

            with capture_output() as (out, _):
                za = cq.query_astref_catfinalcut_by_fiat({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', ['z','i','g'], 'SCAMPCAT_FIAT', verbose=1)
                self.assertEqual(1, len(za))
                output = out.getvalue().strip()
                self.assertTrue('sql' in output)

            za = cq.query_astref_catfinalcut_by_fiat({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', ['z','i','g'], 'SCAMPCAT_FIAT')
            self.assertEqual(1, len(za))
            za = cq.query_astref_catfinalcut_by_fiat({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', [], 'SCAMPCAT_FIAT')
            self.assertEqual(1, len(za))
        finally:
            curs.execute("rollback")

    def test_query_blacklist(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zb = cq.query_blacklist(zres, {'table':'BLACKLIST'}, dbh, '', verbose=2)
            self.assertEqual(len(zres), len(zb))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        with capture_output() as (out, _):
            zb = cq.query_blacklist(zres, {'table':'BLACKLIST'}, dbh, '', verbose=1)
            self.assertEqual(len(zres), len(zb))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        zb = cq.query_blacklist(zres, {}, dbh, '')
        self.assertEqual(len(zres), len(zb))

        curs = dbh.cursor()
        curs.execute("INSERT INTO BLACKLIST (EXPNUM,CCDNUM,REASON,ANALYST) VALUES (515441,58,'Bad image','USER')")
        zres = cq.query_coadd_img_from_attempt({}, 2309774, ['z'], 'desar2home', dbh, '')
        zb = cq.query_blacklist(zres, {'table':'BLACKLIST'}, dbh, '')
        self.assertTrue(len(zres) > len(zb))

    def test_query_meds_psfmodels(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        with capture_output() as (out, _):
            z = cq.query_meds_psfmodels('meds', 'COSMOS_C09', 'Y3A2_COADD_TEST', '', False, ['z'],'desar2home', dbh, '', verbose=2)
            self.assertEqual(2, len(z))
            output = out.getvalue().strip()
            self.assertTrue('Post query constrain' in output)

        with capture_output() as (out, _):
            z = cq.query_meds_psfmodels('meds', 'COSMOS_C09', 'Y3A2_COADD_TEST', '', False, ['z'],'desar2home', dbh, '', verbose=1)
            self.assertEqual(2, len(z))
            output = out.getvalue().strip()
            self.assertFalse('Post query constrain' in output)
            self.assertTrue('sql' in output)

        with capture_output() as (out, _):
            z = cq.query_meds_psfmodels('meds', 'COSMOS_C09', 'Y3A2_COADD_TEST', '', False, ['z','g','r','i','Y','u'],'desar2home', dbh, '', verbose=2)
            self.assertEqual(12, len(z))
            output = out.getvalue().strip()
            self.assertFalse('Post query constrain' in output)
            self.assertTrue('sql' in output)

        with capture_output() as (out, _):
            z = cq.query_meds_psfmodels('meds', 'COSMOS_C09', 'Y3A2_COADD_TEST', '', False, ['z'],'desar2home', dbh, '')
            self.assertEqual(2, len(z))
            output = out.getvalue().strip()
            self.assertFalse('Post query constrain' in output)
            self.assertFalse('sql' in output)

        with capture_output() as (out, _):
            z = cq.query_meds_psfmodels('psfmodel', 'COSMOS_C09', 'Y3A2_COADD_TEST2', 'Y3A1_COADD_TEST_DEEP', False, ['z','g','r','i','Y','u'],'desar2home', dbh, '', verbose=2)
            self.assertEqual(4, len(z))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        with capture_output() as (out, _):
            z = cq.query_meds_psfmodels('psfmodel', 'COSMOS_C09', 'Y3A2_COADD_TEST2', 'Y3A1_COADD_TEST_DEEP', False, ['z','g','r','i','Y','u'],'desar2home', dbh, '', verbose=1)
            self.assertEqual(4, len(z))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        with capture_output() as (out, _):
            z = cq.query_meds_psfmodels('psfmodel', 'COSMOS_C09', 'Y3A2_COADD_TEST2', 'Y3A1_COADD_TEST_DEEP', False, ['z','g','r','i','Y','u'],'desar2home', dbh, '')
            self.assertEqual(4, len(z))
            output = out.getvalue().strip()
            self.assertFalse('sql' in output)

        with capture_output() as (out, _):
            z = cq.query_meds_psfmodels('psfmodel', 'COSMOS_C09', 'Y3A2_COADD_TEST2', 'Y3A1_COADD_TEST_DEEP', True, ['z','g','r','i','Y','u'],'desar2home', dbh, '', verbose=2)
            self.assertEqual(6, len(z))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)

        with capture_output() as (out, _):
            z = cq.query_meds_psfmodels('psfmodel', 'COSMOS_C09', 'Y3A2_COADD_TEST2', 'Y3A1_COADD_TEST_DEEP', True, ['z','g','r','i','Y','u'],'desar2home', dbh, '', verbose=1)
            self.assertEqual(6, len(z))
            output = out.getvalue().strip()
            self.assertTrue('sql' in output)
        with capture_output() as (out, _):
            z = cq.query_meds_psfmodels('psfmodel', 'COSMOS_C09', 'Y3A2_COADD_TEST2', 'Y3A1_COADD_TEST_DEEP', True, ['z','g','r','i','Y','u'],'desar2home', dbh, '')
            self.assertEqual(6, len(z))
            output = out.getvalue().strip()
            self.assertFalse('sql' in output)

    def test_ImgDict_to_LLD(self):
        mtypes = {'red_img': ['type', 'offset']}
        imgs = OrderedDict([('Img1.fits', {'filename': 'Img1.fits',
                                           'path': 'a/path',
                                           'compression': '.fz',
                                           'band': 'z',
                                           'expnum': 101010,
                                           'ccdnum': 5,
                                           'red_img': {'filename': 'Img1.fits',
                                                       'type': 'red_img',
                                                       'offset': 52},
                                           'rac1': 150.120978,
                                           'rac2': 150.12093,
                                           'rac3': 150.420285,
                                           'rac4': 150.420543,
                                           'decc1': 1.545518,
                                           'decc2': 1.396321,
                                           'decc3': 1.396405,
                                           'decc4': 1.545604}),
                            ('Img2.fits', {'filename': 'Img2.fits',
                                           'path': 'a/path',
                                           'compression': '.fz',
                                           'band': 'g',
                                           'red_img': {'filename': 'Img2.fits'},
                                           'expnum': 101010,
                                           'ccdnum': 5,
                                           'rac1': 150.120978,
                                           'rac2': 150.12093,
                                           'rac3': 150.420285,
                                           'rac4': 150.420543,
                                           'decc1': 1.545518,
                                           'decc2': 1.396321,
                                           'decc3': 1.396405,
                                           'decc4': 1.545604})
                     ])
        with capture_output() as (out, _):
            z = cq.ImgDict_to_LLD(imgs, ['red_img'], mtypes, verbose=1)
            self.assertEqual(len(z), 2)
            self.assertEqual(len(z[0]), 1)
            print('\n\n-------------\n' + str (z[0]))
            for k in mtypes['red_img']:
                self.assertTrue(k in z[0][0])
                self.assertFalse(k in z[1][0])
            output = out.getvalue().strip()
            self.assertTrue('Warning: missing' in output)
        with capture_output() as (out, _):
            z = cq.ImgDict_to_LLD(imgs, ['red_img'], mtypes)
            self.assertEqual(len(z), 2)
            self.assertEqual(len(z[0]), 1)
            for k in mtypes['red_img']:
                self.assertTrue(k in z[0][0])
                self.assertFalse(k in z[1][0])
            output = out.getvalue().strip()
            self.assertFalse('Warning: missing' in output)

    def test_CatDict_to_LLD(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        ftypes = ['headfile']
        mdata = ['expnum','ccdnum']
        za = cq.query_astref_scampcat({}, 'DES1002+0126', 'Y6A1_COADD_TEST', dbh, '', ['z','i','g'])
        zr = cq.CatDict_to_LLD(za, ['headfile'], ['expnum', 'ccdnum'])
        self.assertTrue(len(zr), 1)
        for k in mdata:
            self.assertTrue(k in zr[0][0])
        self.assertIsNone(zr[0][0]['compression'])

class Test_run_shredex(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sfile = 'services.ini'
        cls.files = [cls.sfile]
        open(cls.sfile, 'w').write("""

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))
        dbh = desdbi.DesDbi('services.ini', 'db-test')
        cur = dbh.cursor()
        cur.executescript(open('tests/DATA.sql', 'r').read())
        dbh.commit()
        cur.close()

    @classmethod
    def tearDownClass(cls):
        for fl in cls.files:
            try:
                os.unlink(fl)
            except:
                pass
        MockConnection.destroy()

    def test_make_coadd_object_map(self):
        filename = 'test_ids.fits'
        parser = argparse.ArgumentParser()
        parser.add_argument("--cat", type=str, action="store", default=None, required=True,
                        help="The name of the coadd catalog")
        parser.add_argument("--coadd_object_tablename", type=str, action="store", default='COADD_OBJECT',
                        help="Name of the table with COADD_OBJECT")
        parser.add_argument("--ids", type=str, action="store", default=None,
                        help="File with map between COADD_OBJECT ID and SExtractor OBJECT_NUMBER")
        parser.add_argument("--db_section", type=str, action="store", default=None,
                        help="Database section to connect")
        parser.add_argument("--services", type=str, action="store", default='.desservices.ini',
                            help="services file name")
        args = parser.parse_args(['--cat', 'test_coadd_objects.fits',
                                  '--coadd_object_tablename', 'coadd_object_test2',
                                  '--ids', filename,
                                  '--db_section', 'db-test',
                                  '--services', 'services.ini'])
        rsx.make_coadd_object_map(args)
        data, hdr = fitsio.read(filename, header=True)
        self.assertEqual(100, hdr['NAXIS2'])
        self.assertEqual(100, data.shape[0])
        os.unlink(filename)
        args = parser.parse_args(['--cat', 'test_coadd_objects.fits',
                                  '--coadd_object_tablename', 'coadd_object',
                                  '--ids', filename,
                                  '--db_section', 'db-test',
                                  '--services', 'services.ini'])
        self.assertRaises(ValueError, rsx.make_coadd_object_map, args)

    def test_main(self):
        filename = 'test_ids.fits'
        temp = copy.deepcopy(sys.argv)
        sys.argv = ['run_shredx','--cat', 'test_coadd_objects.fits',
                    '--coadd_object_tablename', 'coadd_object_test2',
                    '--ids', filename,
                    '--db_section', 'db-test',
                    '--services', 'services.ini',
                    '--tilename', 'TEST1234-567',
                    '--coadd_ima_list', 'x',
                    '--coadd_psf_list', 'y',
                    '--bands', 'g',
                    '--nranges', '10',
                    '--wrange', '2'
                    ]
        with mock.patch.object(fvdt, 'read_meds_list', return_value={'g': 'a'}):
            with mock.patch.object(fvdt, 'find_number_fof', return_value=2):
                self.assertRaises(Exception, rsx.main)
                data, hdr = fitsio.read(filename, header=True)
                self.assertEqual(100, hdr['NAXIS2'])
                self.assertEqual(100, data.shape[0])
                os.unlink(filename)
        sys.argv.append('--fofs')
        sys.argv.append('fofs.file')
        with mock.patch.object(fvdt, 'read_meds_list', return_value={'g': 'a'}):
            with mock.patch.object(fvdt, 'find_number_fof', return_value=2):
                self.assertRaises(SystemExit, rsx.main)
                data, hdr = fitsio.read(filename, header=True)
                self.assertEqual(100, hdr['NAXIS2'])
                self.assertEqual(100, data.shape[0])
                os.unlink(filename)
        sys.argv.append('--dryrun')
        with mock.patch.object(fvdt, 'read_meds_list', return_value={'g': 'a'}):
            with mock.patch.object(fvdt, 'find_number_fof', return_value=2):
                self.assertRaises(SystemExit, rsx.main)
                data, hdr = fitsio.read(filename, header=True)
                self.assertEqual(100, hdr['NAXIS2'])
                self.assertEqual(100, data.shape[0])
                os.unlink(filename)

        sys.argv = copy.deepcopy(temp)


if __name__ == '__main__':
    unittest.main()
