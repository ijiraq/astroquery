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
    def atest_query_region(self):
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

    def test_query_name(self):
        target = SkyCoord.from_name('M31')
        result = Cadc.query_region(target)
        print(result)
