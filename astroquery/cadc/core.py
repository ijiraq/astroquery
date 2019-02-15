# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
=============
CADC
=============

"""

from ..utils.class_or_instance import class_or_instance
from ..utils import async_to_sync, commons
from ..query import QueryWithLogin
# prepend_docstr is a way to copy docstrings between methods
from ..utils import prepend_docstr_nosections
from bs4 import BeautifulSoup
import astropy
import astroquery
from six import StringIO
from . import conf
from astroquery.cadc.cadctap.core import TapPlusCadc
from astroquery.cadc.cadctap.job import JobCadc
import logging

import requests

__all__ = ['Cadc', 'CadcClass', 'CadcTAP']

DEFAULT_URL = 'http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/tap'


logger = logging.getLogger(__name__)


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
        cjob._phase = job._phase
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


@async_to_sync
class CadcClass(QueryWithLogin):
    """
    Cadc class
    """

    CADC_REGISTRY_URL = conf.CADC_REGISTRY_URL
    CADCTAP_SERVICE_URI = conf.CADCTAP_SERVICE_URI
    CADCDATALINK_SERVICE_URI = conf.CADCDATLINK_SERVICE_URI
    TIMEOUT = conf.TIMEOUT

    def __init__(self, *args):
        """ set some parameters """
        # do login here
        self.cadctap = CadcTAP()
        try:
            r = requests.get(self.CADC_REGISTRY_URL)
            r.raise_for_status()
            self._capabilities = self._parse_reg(r.text)
        except requests.exceptions.HTTPError as err:
            logger.debug(
                "ERROR getting the resource caps: {}".format(str(err)))
            raise err

        soup = BeautifulSoup(requests.get(self._capabilities[self.CADCDATALINK_SERVICE_URI]).text)
        print(self._capabilities)

        soup.find_all('capability')[3].find_all('interface')[1].securitymethod[
            'standardid']



    #@class_or_instance
    def query_region_async(self, coordinates, radius = 0.016666666666667,
                           collection=None,
                           get_query_payload=False, cache=True):
        """
                Queries a region around the specified coordinates.

                Parameters
                ----------
                coordinates : str or `astropy.coordinates`.
                    coordinates around which to query
                radius : str or `astropy.units.Quantity`.
                    the radius of the cone search
                collection: Name of the CADC collection to query, optional
                get_query_payload : bool, optional
                    Just return the dict of HTTP request parameters.
                verbose : bool, optional
                    Display VOTable warnings or not.

                Returns
                -------
                response : `requests.Response`
                    The HTTP response returned from the service.
                    All async methods should return the raw HTTP response.
                """

        request_payload = self._args_to_payload(coordinates=coordinates,
                                                radius=radius,
                                                collection=collection)
        response = self.cadctap.run_query(request_payload['query'],
                                          operation='sync')

        # primarily for debug purposes, but also useful if you want to send
        # someone a URL linking directly to the data
        if get_query_payload:
            return request_payload

        return response

    def _parse_reg(self, content):
        # parses the CADC registry and returns a dictionary of services and
        # the URL to their capabilities
        capabilities = {}
        for line in content.splitlines():
            if len(line) > 0 and not line.startswith('#'):
                service_id, capabilies_url = line.split('=')
                capabilities[service_id.strip()] = capabilies_url.strip()
        return capabilities

    def _login(self, user, password):
        """

        :param user:
        :param password:
        :return:
        """
        return self.__cadctap.login(user=user, password=password)

    def _parse_result(self, result, verbose=False):
        # result is a job
        #TODO check state of the job
        if result._phase != 'COMPLETED':
            raise RuntimeError('Query not completed')
        return result.results

    def _args_to_payload(self, *args, **kwargs):
        # convert arguments to a valid requests payload
        coordinates = commons.parse_coordinates(kwargs['coordinates'])
        radius = kwargs['radius']
        collection = kwargs['collection']
        payload = {format: 'VOTable'}
        payload['query'] = \
            "SELECT * from caom2.Observation o join caom2.Plane p " \
            "ON o.obsID=p.obsID " \
            "WHERE INTERSECTS( " \
            "CIRCLE('ICRS', {}, {}, {}), position_bounds) = 1 AND " \
            "(quality_flag IS NULL OR quality_flag != 'junk')".\
            format(coordinates.ra.degree, coordinates.dec.degree, radius)
        if ['collection' in kwargs]:
            payload['query'] = "{} AND collection='{}'".\
                format(payload['query'],kwargs['collection'])
        return payload

    def get_images(self, coordinates, radius=0.016666667,
                   collection=None, get_query_payload=False):
        """
        A query function that searches for image cut-outs around coordinates

        Parameters
        ----------
        coordinates : str or `astropy.coordinates`.
            coordinates around which to query
        radius : str or `astropy.units.Quantity`.
            the radius of the cone search
        get_query_payload : bool, optional
            If true than returns the dictionary of query parameters, posted to
            remote server. Defaults to `False`.

        Returns
        -------
        A list of `astropy.fits.HDUList` objects
        """
        readable_objs = self.get_images_async(coordinates, radius,
                                              get_query_payload=get_query_payload)
        if get_query_payload:
            return readable_objs  # simply return the dict of HTTP request params
        # otherwise return the images as a list of astropy.fits.HDUList
        return [obj.get_fits() for obj in readable_objs]

    @prepend_docstr_nosections(get_images.__doc__)
    def get_images_async(self, coordinates, radius, get_query_payload=False):
        """
        Returns
        -------
        A list of context-managers that yield readable file-like objects
        """
        # As described earlier, this function should return just
        # the handles to the remote image files. Use the utilities
        # in commons.py for doing this:

        # first get the links to the remote image files
        image_urls = self.get_image_list(coordinates, radius,
                                         get_query_payload=get_query_payload)
        if get_query_payload:  # if true then return the HTTP request params dict
            return image_urls
        # otherwise return just the handles to the image files.
        return [commons.FileContainer(U) for U in image_urls]

    # the get_image_list method, simply returns the download
    # links for the images as a list

    @prepend_docstr_nosections(get_images.__doc__)
    def get_image_list(self, coordinates, radius=0.01666667, collection=None,
                       get_query_payload=False):
        """
        Returns
        -------
        list of image urls
        """
        # This method should implement steps as outlined below:
        # 1. Construct the actual dict of HTTP request params.
        # 2. Check if the get_query_payload is True, in which
        #    case it should just return this dict.
        # 3. Otherwise make the HTTP request and receive the
        #    HTTP response.
        # 4. Pass this response to the extract_image_urls
        #    which scrapes it to extract the image download links.
        # 5. Return the download links as a list.
        table = self.query_region(coordinates=coordinates,
                                        radius=radius,
                                        collection=collection,
                                        get_query_payload=get_query_payload)

        return self.get_image_urls(table['caomPublisherID'])


    def get_image_urls(self, publisher_ids):
        """
        Helper function that uses the data link web service to resolve a list
        of publisher IDs into a list of files

        Parameters
        ----------
        publisher_ids : list of publisher ids
            source from which the urls are to be extracted

        Returns
        -------
        list of image URLs
        """

        # find the access URL corresponding to the caomPublisherID
        result = []
        for id in publisher_ids:
            pass # TODO find the corresponding url

        return result


Cadc = CadcClass()

