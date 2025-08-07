######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
import os.path
from unittest import mock
from spinedb_api.spine_io.gdx_utils import gams_supports_new_api


class TestGamsSupportsNewApi:
    def test_version_below_42_does_not_support(self):
        # output_24, output_40 and output_47 are real, the rest are fake.
        output_24 = r"""--- Job ? Start 08/07/25 10:14:21 24.1.3 r41464 WEX-WEI x86_64/MS Windows
***
*** GAMS Base Module 24.1.3 r41464 Released Jul 26, 2013 WEI x86_64/MS Windows
***
*** GAMS Development Corporation
*** 1217 Potomac Street, NW
*** Washington, DC 20007, USA
*** 202-342-0180, 202-342-0181 fax
*** support@gams.com, www.gams.com
***
*** GAMS Release     : 24.1.3 r41464 WEX-WEI x86_64/MS Windows
*** Release Date     : Released Jul 26, 2013
*** License Date     : May 30, 2013
*** To use this release, the maintenance expiration date for
*** your license must be later than the License Date (May 30, 2013).
*** System Directory : C:\GAMS\win64\24.1\
*** License          : C:\GAMS\win64\24.1\gamslice.txt
***
*** Energy Systems                                 W120511:1343CP-WIN
*** VTT Energy
*** DC1884  01CP                                                   00
***
*** Status: Normal completion
--- Job ? Stop 08/07/25 10:14:21 elapsed 0:00:00.018"""
        output_40 = r"""--- Job ? Start 08/07/25 10:37:53 40.4.0 d540b52e WEX-WEI x86 64bit/MS Windows
***
*** GAMS Base Module 40.4.0 d540b52e Oct 3, 2022           WEI x86 64bit/MS Window
***
*** GAMS Development Corporation
*** 2751 Prosperity Ave, Suite 210
*** Fairfax, VA 22031, USA
*** +1 202-342-0180, +1 202-342-0181 fax
*** support@gams.com, www.gams.com
***
*** GAMS Release     : 40.4.0 d540b52e WEX-WEI x86 64bit/MS Windows
*** Release Date     : Oct 3, 2022
*** To use this release, you must have a valid license file for
*** this platform with maintenance expiration date later than
*** Aug 01, 2022
*** System Directory : C:\GAMS\40\
*** License file not found.
*** The following directories have been searched:
    C:\ProgramData\GAMS
    C:\GAMS\40\data
    C:\GAMS\40\data\GAMS
    C:\GAMS\40
*** Status: Normal completion
--- Job ? Stop 08/07/25 10:37:53 elapsed 0:00:00.015"""
        output_41 = r"""--- Job ? Start 08/07/25 10:37:53 41.4.0 d540b52e WEX-WEI x86 64bit/MS Windows
***
*** GAMS Base Module 41.4.0 d540b52e Oct 3, 2022           WEI x86 64bit/MS Window
***
*** GAMS Development Corporation
*** 2751 Prosperity Ave, Suite 210
*** Fairfax, VA 22031, USA
*** +1 202-342-0180, +1 202-342-0181 fax
*** support@gams.com, www.gams.com
***
*** GAMS Release     : 41.4.0 d540b52e WEX-WEI x86 64bit/MS Windows
*** Release Date     : Oct 3, 2022
*** To use this release, you must have a valid license file for
*** this platform with maintenance expiration date later than
*** Aug 01, 2022
*** System Directory : C:\GAMS\41\
*** License file not found.
*** The following directories have been searched:
    C:\ProgramData\GAMS
    C:\GAMS\41\data
    C:\GAMS\41\data\GAMS
    C:\GAMS\41
*** Status: Normal completion
--- Job ? Stop 08/07/25 10:37:53 elapsed 0:00:00.015"""
        output_42 = r"""--- Job ? Start 08/07/25 10:37:53 42.4.0 d540b52e WEX-WEI x86 64bit/MS Windows
***
*** GAMS Base Module 42.4.0 d540b52e Oct 3, 2022           WEI x86 64bit/MS Window
***
*** GAMS Development Corporation
*** 2751 Prosperity Ave, Suite 210
*** Fairfax, VA 22031, USA
*** +1 202-342-0180, +1 202-342-0181 fax
*** support@gams.com, www.gams.com
***
*** GAMS Release     : 42.4.0 d540b52e WEX-WEI x86 64bit/MS Windows
*** Release Date     : Oct 3, 2022
*** To use this release, you must have a valid license file for
*** this platform with maintenance expiration date later than
*** Aug 01, 2022
*** System Directory : C:\GAMS\42\
*** License file not found.
*** The following directories have been searched:
    C:\ProgramData\GAMS
    C:\GAMS\42\data
    C:\GAMS\42\data\GAMS
    C:\GAMS\42
*** Status: Normal completion
--- Job ? Stop 08/07/25 10:37:53 elapsed 0:00:00.015"""
        output_47 = r"""--- Job ? Start 08/07/25 11:58:53 47.6.0 c2de9d6d WEX-WEI x86 64bit/MS Windows
***
*** GAMS Base Module 47.6.0 c2de9d6d Sep 12, 2024          WEI x86 64bit/MS Window
***
*** GAMS Development Corporation
*** 2751 Prosperity Ave, Suite 210
*** Fairfax, VA 22031, USA
*** +1 202-342-0180, +1 202-342-0181 fax
*** support@gams.com, www.gams.com
***
*** GAMS Release     : 47.6.0 c2de9d6d WEX-WEI x86 64bit/MS Windows
*** Release Date     : Sep 12, 2024
*** To use this release, you must have a valid license file for
*** this platform with maintenance expiration date later than
*** Jun 13, 2024
*** System Directory : C:\GAMS\47\
***
*** License          : C:\GAMS\47\gamslice.txt
*** GAMS Demo, for EULA and demo limitations see   G240530/0001CB-GEN
*** https://www.gams.com/latest/docs/UG%5FLicense.html
*** DC0000  00
***
*** Licensed platform                             : Generic platforms
*** Evaluation expired
*** Evaluation expiration date (GAMS base module) : Oct 27, 2024
*** Note: For solvers, other expiration dates may apply.
*** Status: Normal completion
--- Job ? Stop 08/07/25 11:58:53 elapsed 0:00:00.016"""
        for output, expected in [(output_24, False), (output_41, False), (output_42, True), (output_47, True)]:
            completed_process = mock.MagicMock()
            completed_process.returncode = 0
            completed_process.stderr = ""
            completed_process.stdout = output
            with mock.patch("spinedb_api.spine_io.gdx_utils.subprocess.run") as mock_run:
                mock_run.return_value = completed_process
                assert gams_supports_new_api(r"C:\GAMS\xxx") == expected
                mock_run.assert_called_once_with(
                    [os.path.join(r"C:\GAMS\xxx", "gams"), "?"], capture_output=True, text=True
                )
