#!/usr/bin/env python3

import unittest
import os
import stat
import sys
import copy
import mock
from mock import patch, MagicMock
from contextlib import contextmanager
from io import StringIO
import re
from collections import OrderedDict
from astropy.io import fits

from MockDBI import MockConnection
os.environ['MEPIPELINEAPPINTG_DIR'] = os.getcwd()

import mepipelineappintg.ngmixit_tools as ngmt
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
'''
    @classmethod
    def setUpClass(cls):
        cls.sfile = 'services.ini'
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
        os.unlink(cls.sfile)
        MockConnection.destroy()
'''
if __name__ == '__main__':
    unittest.main()
