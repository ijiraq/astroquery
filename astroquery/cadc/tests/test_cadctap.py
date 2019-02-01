# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
=============
Cadc TAP plus
=============

"""
import unittest
import os

from astroquery.cadc.core import CadcTAP
from astroquery.cadc.tests.DummyTapHandler import DummyTapHandler


def data_path(filename):
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    return os.path.join(data_dir, filename)


class TestTap(unittest.TestCase):

    def test_get_tables(self):
        dummyTapHandler = DummyTapHandler()
        tap = CadcTAP(tap_plus_handler=dummyTapHandler)
        # default parameters
        parameters = {}
        parameters['only_names'] = False
        parameters['verbose'] = False
        tap.get_tables()
        dummyTapHandler.check_call('get_tables', parameters)
        # test with parameters
        dummyTapHandler.reset()
        parameters = {}
        parameters['only_names'] = True
        parameters['verbose'] = True
        tap.get_tables(True, True)
        dummyTapHandler.check_call('get_tables', parameters)

    def test_get_table(self):
        dummyTapHandler = DummyTapHandler()
        tap = CadcTAP(tap_plus_handler=dummyTapHandler)
        # default parameters
        parameters = {}
        parameters['table'] = 'table'
        parameters['verbose'] = False
        tap.get_table('table')
        dummyTapHandler.check_call('get_table', parameters)
        # test with parameters
        dummyTapHandler.reset()
        parameters = {}
        parameters['table'] = 'table'
        parameters['verbose'] = True
        tap.get_table('table', verbose=True)
        dummyTapHandler.check_call('get_table', parameters)

    def test_run_query(self):
        dummyTapHandler = DummyTapHandler()
        tap = CadcTAP(tap_plus_handler=dummyTapHandler)
        query = "query"
        operation = 'sync'
        # default parameters
        parameters = {}
        parameters['query'] = query
        parameters['name'] = None
        parameters['output_file'] = None
        parameters['output_format'] = 'votable'
        parameters['verbose'] = False
        parameters['dump_to_file'] = False
        parameters['upload_resource'] = None
        parameters['upload_table_name'] = None
        tap.run_query(query, operation)
        dummyTapHandler.check_call('run_query', parameters)
        # test with parameters
        dummyTapHandler.reset()
        output_file = 'output'
        output_format = 'format'
        verbose = True
        upload_resource = 'upload_res'
        upload_table_name = 'upload_table'
        parameters['query'] = query
        parameters['name'] = None
        parameters['output_file'] = output_file
        parameters['output_format'] = output_format
        parameters['verbose'] = verbose
        parameters['dump_to_file'] = True
        parameters['upload_resource'] = upload_resource
        parameters['upload_table_name'] = upload_table_name
        tap.run_query(query,
                      operation,
                      output_file=output_file,
                      output_format=output_format,
                      verbose=verbose,
                      upload_resource=upload_resource,
                      upload_table_name=upload_table_name)
        dummyTapHandler.check_call('run_query', parameters)

    def test_load_async_job(self):
        dummyTapHandler = DummyTapHandler()
        tap = CadcTAP(tap_plus_handler=dummyTapHandler)
        jobid = '123'
        # default parameters
        parameters = {}
        parameters['jobid'] = jobid
        parameters['verbose'] = False
        tap.load_async_job(jobid)
        dummyTapHandler.check_call('load_async_job', parameters)
        # test with parameters
        dummyTapHandler.reset()
        parameters['jobid'] = jobid
        parameters['verbose'] = True
        tap.load_async_job(jobid, verbose=True)
        dummyTapHandler.check_call('load_async_job', parameters)

    def test_list_async_jobs(self):
        dummyTapHandler = DummyTapHandler()
        tap = CadcTAP(tap_plus_handler=dummyTapHandler)
        # default parameters
        parameters = {}
        parameters['verbose'] = False
        tap.list_async_jobs()
        dummyTapHandler.check_call('list_async_jobs', parameters)
        # test with parameters
        dummyTapHandler.reset()
        parameters['verbose'] = True
        tap.list_async_jobs(verbose=True)
        dummyTapHandler.check_call('list_async_jobs', parameters)

    def test_save_results(self):
        dummyTapHandler = DummyTapHandler()
        tap = CadcTAP(tap_plus_handler=dummyTapHandler)
        job = '123'
        # default parameters
        parameters = {}
        parameters['job'] = job
        parameters['filename'] = 'file.txt'
        parameters['verbose'] = False
        tap.save_results(job, 'file.txt')
        dummyTapHandler.check_call('save_results', parameters)
        # test with parameters
        dummyTapHandler.reset()
        parameters['job'] = job
        parameters['filename'] = 'file.txt'
        parameters['verbose'] = True
        tap.save_results(job, 'file.txt', verbose=True)
        dummyTapHandler.check_call('save_results', parameters)

    def test_login(self):
        dummyTapHandler = DummyTapHandler()
        tap = CadcTAP(tap_plus_handler=dummyTapHandler)
        user = 'user'
        password = 'password'
        cert = 'cert'
        cookie = 'cookie'
        login = 'http://login.com/login'
        # default parameters
        parameters = {}
        parameters['user'] = None
        parameters['password'] = None
        parameters['certificate_file'] = None
        parameters['cookie_prefix'] = None
        parameters['login_url'] = None
        parameters['verbose'] = False
        tap.login(None, None, None, None, None, False)
        dummyTapHandler.check_call('login', parameters)
        # test with parameters
        dummyTapHandler.reset()
        parameters['user'] = user
        parameters['password'] = password
        parameters['certificate_file'] = cert
        parameters['cookie_prefix'] = cookie
        parameters['login_url'] = login
        parameters['verbose'] = True
        tap.login(user, password, cert, cookie, login, verbose=True)
        dummyTapHandler.check_call('login', parameters)

    def test_logout(self):
        dummyTapHandler = DummyTapHandler()
        tap = CadcTAP(tap_plus_handler=dummyTapHandler)
        # default parameters
        parameters = {}
        parameters['verbose'] = False
        tap.logout(False)
        dummyTapHandler.check_call('logout', parameters)
        # test with parameters
        dummyTapHandler.reset()
        parameters['verbose'] = True
        tap.logout(True)
        dummyTapHandler.check_call('logout', parameters)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
