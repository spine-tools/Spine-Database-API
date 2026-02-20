######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Database API.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
import pytest
from spinedb_api import SpineDBAPIError
import spinedb_api.scenario_recipes as recipes


class TestDuplicateScenario:
    def test_duplicate_scenario_without_alternatives(self, db_map):
        original = db_map.add_scenario(name="Scenario", description="Original scenario")
        duplicate = recipes.duplicate_scenario(original, "Duplicate scenario")
        assert duplicate["name"] == "Duplicate scenario"
        assert duplicate["description"] == "Original scenario"
        assert db_map.find_scenario_alternatives(scenario_id=duplicate["id"]) == []

    def test_duplicate_scenario_with_alternatives(self, db_map):
        original = db_map.add_scenario(name="Scenario", description="Original scenario")
        alternative_1 = db_map.add_alternative(name="Alternative 1")
        alternative_2 = db_map.add_alternative(name="Alternative 2")
        db_map.add_scenario_alternative(scenario_id=original["id"], alternative_id=alternative_1["id"], rank=0)
        db_map.add_scenario_alternative(scenario_id=original["id"], alternative_id=alternative_2["id"], rank=1)
        duplicate = recipes.duplicate_scenario(original, "Duplicate scenario")
        assert duplicate["name"] == "Duplicate scenario"
        assert duplicate["description"] == "Original scenario"
        assert duplicate["alternative_name_list"] == ["Alternative 1", "Alternative 2"]


class TestCreateWithAlternatives:
    def test_no_alternatives_raises(self):
        with pytest.raises(SpineDBAPIError, match="^no alternatives given$"):
            recipes.create_with_alternatives([], "Scenario 1")

    def test_alternatives_get_added_in_order(self, db_map):
        alternative_1 = db_map.add_alternative(name="Alternative 1")
        alternative_2 = db_map.add_alternative(name="Alternative 2")
        alternative_3 = db_map.add_alternative(name="Alternative 3")
        scenario = recipes.create_with_alternatives([alternative_2, alternative_3, alternative_1], "Scenario")
        assert scenario["name"] == "Scenario"
        assert scenario["description"] is None
        assert scenario["alternative_id_list"] == [alternative_2["id"], alternative_3["id"], alternative_1["id"]]


class TestWithAlternative:
    def test_add_alternative_to_empty_scenario(self, db_map):
        scenario = db_map.add_scenario(name="Scenario")
        alternative = db_map.add_alternative(name="Alternative")
        recipes.with_alternative(scenario, alternative)
        assert scenario["alternative_name_list"] == ["Alternative"]

    def test_added_alternative_has_the_highest_rank(self, db_map):
        scenario = db_map.add_scenario(name="Scenario")
        alternative_1 = db_map.add_alternative(name="Alternative 1")
        db_map.add_scenario_alternative(scenario_id=scenario["id"], alternative_id=alternative_1["id"], rank=1)
        alternative_2 = db_map.add_alternative(name="Alternative 2")
        db_map.add_scenario_alternative(scenario_id=scenario["id"], alternative_id=alternative_2["id"], rank=23)
        alternative_3 = db_map.add_alternative(name="Alternative 3")
        recipes.with_alternative(scenario, alternative_3)
        assert scenario["alternative_name_list"] == ["Alternative 1", "Alternative 2", "Alternative 3"]

    def test_adding_alternative_that_is_already_in_scenario(self, db_map):
        scenario = db_map.add_scenario(name="Scenario")
        alternative_1 = db_map.add_alternative(name="Alternative 1")
        db_map.add_scenario_alternative(scenario_id=scenario["id"], alternative_id=alternative_1["id"], rank=1)
        with pytest.raises(SpineDBAPIError, match="^there's already a scenario_alternative with"):
            recipes.with_alternative(scenario, alternative_1)


class TestWithAlternatives:
    def test_empty_alternatives(self, db_map):
        scenario = db_map.add_scenario(name="Scenario")
        recipes.with_alternatives(scenario, [])
        assert scenario["alternative_name_list"] == []

    def test_alternatives_added_to_highest_ranks(self, db_map):
        scenario = db_map.add_scenario(name="Scenario")
        alternative_1 = db_map.add_alternative(name="Alternative 1")
        recipes.with_alternative(scenario, alternative_1)
        alternative_2 = db_map.add_alternative(name="Alternative 2")
        alternative_3 = db_map.add_alternative(name="Alternative 3")
        recipes.with_alternatives(scenario, (alternative_2, alternative_3))
        assert scenario["alternative_id_list"] == [alternative_1["id"], alternative_2["id"], alternative_3["id"]]
