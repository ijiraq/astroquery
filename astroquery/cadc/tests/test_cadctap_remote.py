# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
=============
Canadian Astronomy Data Centre
=============
"""

import pytest
from astropy.tests.helper import remote_data
from astroquery.cadc import Cadc


@remote_data
class TestCadcClass:
    # now write tests for each method here
    def test_query_region(self):
        result = Cadc.query_region('08h45m07.5s +54d18m00s',
                                   collection='VLASS')
        assert 1 == len(result)  # TODO correct?

    def test_get_images(self):
        result = Cadc.get_images('08h45m07.5s +54d18m00s',
                                   collection='VLASS')

        print(result)


