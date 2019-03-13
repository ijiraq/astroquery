# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
=============
Canadian Astronomy Data Centre
=============
"""

import pytest
from astropy.tests.helper import remote_data
from astroquery.cadc import Cadc
from astropy.coordinates import SkyCoord


@remote_data
class TestCadcClass:
    # now write tests for each method here
    def test_query_region(self):
        result = Cadc.query_region('08h45m07.5s +54d18m00s', collection='CFHT')
        # do some manipulation of the results. Below it's filtering out based
        # on target name but other manipulations are possible.
        urls = Cadc.get_data_urls(result[result['target_name']=='Nr3491_1'])
        assert 1 == len(urls)
        # now get the auxilary files too
        urls = Cadc.get_data_urls(result[result['target_name'] == 'Nr3491_1'],
                                  include_auxilaries=True)
        assert 4 == len(urls)

        # the same result should be obtained by querying the entire region
        # and filtering out on the CFHT collection
        result2 = Cadc.query_region('08h45m07.5s +54d18m00s')
        assert len(result) == len(result2[result2['collection'] == 'CFHT'])

        # search for a target
        results = Cadc.query_region(SkyCoord.from_name('M31'))
        assert len(results) > 20

    def test_query_name(self):
        result1 = Cadc.query_name('M31')
        assert len(result1) > 20
        # test case insensitive
        result2 = Cadc.query_name('m31')
        assert len(result1) == len(result2)

    def test_query(self):
        result3 = Cadc.query(
            "select count(*) from caom2.Observation where target_name='M31'")
        assert 1000 < result3[0][0]

