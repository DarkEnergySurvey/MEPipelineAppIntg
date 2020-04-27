#!/usr/bin/env python3

import unittest
import os
import stat
import sys
import mock
import copy
from mock import patch, MagicMock
from contextlib import contextmanager
from io import StringIO
import re
from pathlib import Path
from collections import OrderedDict
from astropy.io import fits

from MockDBI import MockConnection
os.environ['MEPIPELINEAPPINTG_DIR'] = os.getcwd()
sys.modules['fitvd'] = mock.Mock()

import mepipelineappintg.ngmixit_tools as ngmt
import mepipelineappintg.fitvd_tools as fvdt
import mepipelineappintg.mepochmisc as mem
import mepipelineappintg.meds_query as mq
import mepipelineappintg.meappintg_tools as met
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
            self.assertTrue(Path(outfile2).stat().st_size > 0)

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

if __name__ == '__main__':
    unittest.main()
