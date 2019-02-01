# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
=============
Cadc TAP plus
=============

"""

from astroquery.cadc.cadctap.core import TapPlusCadc
from astroquery.cadc.cadctap.job import JobCadc

import requests

__all__ = ['Cadc', 'CadcTAP']

DEFAULT_URL = 'http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/tap'


class CadcTAP(object):
    """
    Proxy class to default TapPlus object (pointing to CADA Archive)
    """
    def __init__(self, url=None, tap_plus_handler=None, verbose=False):
        """
        Initialize CadcTAP object

        Parameters
        ----------
        url : str, optional, default 'None;
            a url to use instead of the default
        tap_plus_handler : TAP/TAP+ object, optional, default 'None'
            connection to use instead of the default one created
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        CadcTAP object
        """
        if url is not None and tap_plus_handler is not None:
            raise AttributeError('Can not input both url and tap handler')

        if tap_plus_handler is None:
            if url is None:
                self.__cadctap = TapPlusCadc(
                    url=DEFAULT_URL,
                    verbose=verbose)
            else:
                self.__cadctap = TapPlusCadc(url=url, verbose=verbose)
        else:
            self.__cadctap = tap_plus_handler

    def get_tables(self, only_names=False, verbose=False):
        """
        Gets all public tables

        Parameters
        ----------
        only_names : bool, TAP+ only, optional, default 'False'
            True to load table names only
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A list of table objects
        """
        return self.__cadctap.load_tables(only_names, verbose)

    def get_table(self, table, verbose=False):
        """
        Gets the specified table

        Parameters
        ----------
        table : str, mandatory
            full qualified table name (i.e. schema name + table name)
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A table object
        """
        return self.__cadctap.load_table(table,
                                         verbose)

    def run_query(self, query, operation, output_file=None,
                  output_format="votable", verbose=False,
                  background=False, upload_resource=None,
                  upload_table_name=None):
        """
        Runs a query

        Parameters
        ----------
        query : str, mandatory
            query to be executed
        operation : str, mandatory,
            'sync' or 'async' to run a synchronous or asynchronous job
        output_file : str, optional, default None
            file name where the results are saved if dumpToFile is True.
            If this parameter is not provided, the jobid is used instead
        output_format : str, optional, default 'votable'
            results format, 'csv', 'tsv' and 'votable'
        verbose : bool, optional, default 'False'
            flag to display information about the process
        save_to_file : bool, optional, default 'False'
            if True, the results are saved in a file instead of using memory
        background : bool, optional, default 'False'
            when the job is executed in asynchronous mode,
            this flag specifies whether the execution will wait until results
            are available
        upload_resource: str, optional, default None
            resource to be uploaded to UPLOAD_SCHEMA
        upload_table_name: str, required if uploadResource is provided,
            default None
            resource temporary table name associated to the uploaded resource

        Returns
        -------
        A Job object
        """
        if output_file is not None:
            save_to_file = True
        else:
            save_to_file = False
        if operation == 'sync':
            job = self.__cadctap.launch_job(
                query,
                None,
                output_file=output_file,
                output_format=output_format,
                verbose=verbose,
                dump_to_file=save_to_file,
                upload_resource=upload_resource,
                upload_table_name=upload_table_name)
            op = False
        elif operation == 'async':
            job = self.__cadctap.launch_job_async(
                query,
                None,
                output_file=output_file,
                output_format=output_format,
                verbose=verbose,
                dump_to_file=save_to_file,
                background=True,
                upload_resource=upload_resource,
                upload_table_name=upload_table_name)
            op = True
        cjob = JobCadc(async_job=op, query=job.parameters['query'],
                       connhandler=self.__cadctap._TapPlus__getconnhandler())
        cjob.jobid = job.jobid
        cjob.outputFile = job.outputFile
        cjob.set_response_status(job._Job__responseStatus,
                                 job._Job__responseMsg)
        cjob.remoteLocation = job.remoteLocation
        cjob.parameters['format'] = job.parameters['format']
        if operation == 'async':
            if save_to_file:
                cjob.save_results(output_file, verbose)
            else:
                cjob.get_results()
        else:
            if job.results is not None:
                cjob.set_results(job.results)
        return cjob

    def load_async_job(self, jobid, verbose=False):
        """
        Loads an asynchronous job

        Parameters
        ----------
        jobid : str, mandatory
            job identifier
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A Job object
        """
        return self.__cadctap.load_async_job(jobid, verbose=verbose)

    def list_async_jobs(self, verbose=False):
        """
        Returns all the asynchronous jobs

        Parameters
        ----------
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A list of Job objects
        """
        try:
            joblist = self.__cadctap.list_async_jobs(verbose)
            cadclist = []
            if joblist is not None:
                for job in joblist:
                    newJob = JobCadc(async_job=True,
                                     connhandler=job.connHandler)
                    newJob.jobid = job.jobid
                    cadclist.append(newJob)
        except requests.exceptions.HTTPError:
            return None
        return cadclist

    def save_results(self, job, filename, verbose=False):
        """
        Saves job results

        Parameters
        ----------
        job : Job, mandatory
            job
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        return self.__cadctap.save_results(job, filename, verbose)

    def login(self, user=None, password=None, certificate_file=None,
              cookie_prefix=None, login_url=None, verbose=False):
        """
        Login, set varibles to use for logging in

        Parameters
        ----------
        user : str, required if certificate is None
            username to login with
        password : str, required if user is set
            password to login with
        certificate : str, required if user is None
            path to certificate to use with logging in
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """

        return self.__cadctap.login(user=user, password=password,
                                    certificate_file=certificate_file,
                                    cookie_prefix=cookie_prefix,
                                    login_url=login_url,
                                    verbose=verbose)

    def logout(self, verbose=False):
        """
        Logout
        """
        return self.__cadctap.logout(verbose)


Cadc = CadcTAP()
