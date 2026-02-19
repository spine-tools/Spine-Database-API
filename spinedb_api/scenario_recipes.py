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

"""
This module contains functions and methods to support scenario generation.

.. warning::

    This API is experimental.

Let's build some scenarios!
Imagine we have five alternatives: ``Base``, ``high_fuel``, ``low_fuel``, ``high_co2`` and ``low_co2``.
Each scenario should use ``Base`` as the lowest rank alternative.
On top of that, we want to build useful combinations of the other alternatives.
First, we combine ``Base`` with all other alternatives.

.. code-block:: python

    import spinedb_api as api
    import spinedb_api.scenario_recipes as recipes

    url = "<enter url here>"

    with api.DatabaseMapping(url) as db_map:
        base_alternative = db_map.alternative(name="Base")
        for alternative_name in ("high_fuel", "low_fuel", "high_co2", "low_co2"):
            scenario_name = alternative_name
            alternative = db_map.alternative(name=alternative_name)
            recipes.create_with_alternatives([base_alternative, alternative], scenario_name)

Now, we have four scenarios, ``high_fuel``, ``low_fuel``, ``high_co2`` and ``low_co2``.
Each scenario has ``Base`` alternative, and one of the other alternatives corresponding to scenario's name.

Let's create another set of scenarios by duplicating the existing ones
and adding ``high_co2`` and ``low_co2`` alternatives to the scenarios that deal with fuel prices.

.. code-block:: python

    import spinedb_api as api
    import spinedb_api.scenario_recipes as recipes

    url = "<enter url here>"

    with api.DatabaseMapping(url) as db_map:
        for fuel in  ("high_fuel", "low_fuel"):
            for co2 in ("high_co2", "low_co2"):
                scenario_name = f"{fuel}+{co2}"
                base_scenario = db_map.scenario(name=fuel)
                new_scenario = recipes.duplicate_scenario(base_scenario, scenario_name)
                co2_alternative = db_map.alternative(name=co2)
                recipes.with_alternative(new_scenario, co2_alternative)

After running the script above, our database contains four new scenarios:
``high_fuel+high_co2``, ``high_fuel+low_co2``, ``low_fuel+high_co2`` and ``low_fuel+low_co2``.

The combinatoric iterators of the `itertools module`_ in Python's standard library are also useful
when generating scenarios based on existing alternatives.
Let's assume we have another set of alternatives which we want to combine in as many ways as possible
to generate more scenarios.
Again, all scenarios should have ``Base`` alternative as lowest rank alternative.
The available alternatives are: ``coal``, ``coal_chp``, ``wind`` and ``antimatter``.

.. _itertools module: https://docs.python.org/3.14/library/itertools.html

.. code-block:: python

    import itertools
    import spinedb_api as api
    import spinedb_api.scenario_recipes as recipes

    url = "<enter url here>"

    with api.DatabaseMapping(url) as db_map:
        base_alternative = db_map.alternative(name="Base")
        sector_alternatives = [db_map.alternative(name=name) for name in ("coal", "coal_chp", "wind", "anti_matter")]
        for n_sectors in range(1, len(sector_alternatives) + 1):
            for sector_set in itertools.combinations(sector_alternatives, n_sectors):
                scenario_name = "+".join(alternative["name"] for alternative in sector_set)
                full_alternatives = (base_alternative,) + sector_set
                recipes.create_with_alternatives(full_alternatives, scenario_name)

``itertools.combinations()`` creates us all sensible combinations of alternatives which result in 15 scenarios:
``coal``, ``coal_chp``, ``wind``, ``antimatter``,
``coal+coal_chp``, ``coal+wind``, ``coal+antimatter``, ``coal_chp+wind``, ``coal_chp+antimatter``, ``wind+antimatter``,
``coal+coal_chp+wind``, ``coal+coal_chp+antimatter``, ``coal+wind+antimatter``, ``coal_chp+wind+antimatter``
and ``coal+coal_chp+wind+antimatter``
"""
from collections.abc import Iterable, Sequence
from spinedb_api import SpineDBAPIError
from spinedb_api.db_mapping_base import PublicItem


def create_with_alternatives(alternatives: Sequence[PublicItem], scenario_name: str) -> PublicItem:
    """Creates a new scenario with given alternatives."""
    if not alternatives:
        raise SpineDBAPIError("no alternatives given")
    db_map = alternatives[0].db_map
    new_scenario = db_map.add_scenario(name=scenario_name)
    scenario_id = new_scenario["id"]
    for rank, alternative in enumerate(alternatives):
        db_map.add_scenario_alternative(scenario_id=scenario_id, alternative_id=alternative["id"], rank=rank)
    return new_scenario


def duplicate_scenario(scenario: PublicItem, duplicate_name: str) -> PublicItem:
    """Duplicates a scenario and its scenario alternatives."""
    db_map = scenario.db_map
    new_scenario = db_map.add_scenario(name=duplicate_name, description=scenario["description"])
    new_scenario_id = new_scenario["id"]
    for scenario_alternative in db_map.find_scenario_alternatives(scenario_id=scenario["id"]):
        db_map.add_scenario_alternative(
            scenario_id=new_scenario_id,
            alternative_id=scenario_alternative["alternative_id"],
            rank=scenario_alternative["rank"],
        )
    return new_scenario


def with_alternative(scenario: PublicItem, alternative: PublicItem) -> None:
    """Adds an alternative to an existing scenario.

    The alternative will be added as the highest ranking alternative.
    """
    alternative_id = alternative["id"]
    db_map = scenario.db_map
    scenario_id = scenario["id"]
    existing_scenario_alternatives = scenario.db_map.find_scenario_alternatives(scenario_id=scenario_id)
    if existing_scenario_alternatives:
        last_rank = max(scenario_alternative["rank"] for scenario_alternative in existing_scenario_alternatives)
    else:
        last_rank = -1
    db_map.add_scenario_alternative(scenario_id=scenario_id, alternative_id=alternative_id, rank=last_rank + 1)


def with_alternatives(scenario: PublicItem, alternatives: Iterable[PublicItem]) -> None:
    """Adds given alternatives to an existing scenario.

    The alternatives will be added as the highest ranking alternatives.
    """
    db_map = scenario.db_map
    scenario_id = scenario["id"]
    existing_scenario_alternatives = scenario.db_map.find_scenario_alternatives(scenario_id=scenario_id)
    if existing_scenario_alternatives:
        last_rank = max(scenario_alternative["rank"] for scenario_alternative in existing_scenario_alternatives)
    else:
        last_rank = -1
    for i, alternative in enumerate(alternatives):
        db_map.add_scenario_alternative(
            scenario_id=scenario_id, alternative_id=alternative["id"], rank=last_rank + i + 1
        )
