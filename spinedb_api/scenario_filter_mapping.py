######################################################################################################################
# Copyright (C) 2017 - 2020 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Provides :class:`.ScenarioFilterMapping`.

:author: Antti Soininen (VTT)
:date:   19.8.2020
"""
from sqlalchemy import case, func
from sqlalchemy.sql.expression import label
from .db_mapping import DatabaseMapping
from .exception import SpineDBAPIError

class ScenarioFilterMapping(DatabaseMapping):
    """
    A database mapping that filters parameter values by scenarios and alternatives.

    By default, the active scenarios will be used while the Base alternative is used as a fallback.
    """

    def __init__(self, db_url, username=None, upgrade=False, codename=None, _create_engine=None):
        """
        Args:
            db_url (str): A URL in RFC-1738 format pointing to the database to be mapped.
            username (str, optional): A user name. If ``None``, it gets replaced by the string ``"anon"``.
            upgrade (bool): Whether or not the db at the given URL should be upgraded to the most recent version.
            codename (str, optional): A name that uniquely identifies the class instance within a client application.
            _create_engine (callable, optional): A function that given the url, returns the engine.
                It defaults to SQLAlchemy ``create_engine``. Mainly intended to pass ``spinedb_api.create_new_spine_database``
                together with an in-memory SQLite ``db_url``.
        """
        super().__init__(db_url, username, upgrade, codename, _create_engine)
        self._active_alternatives = None
        self._active_scenarios = None

    def override_activate_scenarios(self, *args):
        """
        Overrides the scenarios that are set active in the database for this filter.

        Args:
            *args: scenario names or ids
        """
        ids = list()
        for scenario in args:
            if isinstance(scenario, int):
                exists = self.query(self.scenario_sq.c.id).filter(self.scenario_sq.c.id == scenario).scalar()
                if exists is None:
                    raise SpineDBAPIError(f"Scenario id {scenario} not found")
                ids.append(scenario)
            else:
                id_ = self.query(self.scenario_sq.c.id).filter(self.scenario_sq.c.name == scenario).scalar()
                if id_ is not None:
                    ids.append(id_)
                else:
                    raise SpineDBAPIError(f"Scenario '{scenario}' not found")
        self._parameter_value_sq = None
        self._active_scenarios = ids
        self._active_alternatives = None

    def active_scenarios_overridden(self):
        """
        Queries whether the active scenarios have been overridden.

        Returns:
            bool: True if active scenarios have been overridden, False otherwise
        """
        return self._active_scenarios is not None

    def override_active_alternatives(self, *args):
        """
        Overrides any active scenario, choosing the parameter values by the given alternatives directly.

        Args:
            *args: alternative names or ids
        """
        ids = list()
        for alternative in args:
            if isinstance(alternative, int):
                exists = self.query(self.alternative_sq.c.id).filter(self.alternative_sq.c.id == alternative).scalar()
                if exists is None:
                    raise SpineDBAPIError(f"Alternative id {alternative} not found")
                ids.append(alternative)
            else:
                id_ = self.query(self.alternative_sq.c.id).filter(self.alternative_sq.c.name == alternative).scalar()
                if id_ is None:
                    raise SpineDBAPIError(f"Alternative {alternative} not found")
                ids.append(id_)
        self._parameter_value_sq = None
        self._active_scenarios = None
        self._active_alternatives = ids

    def active_alternatives_overridden(self):
        return self._active_alternatives is not None

    def restore_default_active_scenarios(self):
        """Restores the active scenarios to the ones specified by the database."""
        self._parameter_value_sq = None
        self._active_scenarios = None
        self._active_alternatives = None

    @property
    def parameter_value_sq(self):
        if self._parameter_value_sq is None:
            if self._active_scenarios is not None :
                self._parameter_value_sq = self._parameter_value_sq_overridden_scenario_filtered()
            elif self._active_alternatives is not None:
                subquery = super().parameter_value_sq
                self._parameter_value_sq = (
                    self.query(subquery)
                        .filter(subquery.c.alternative_id.in_(self._active_alternatives))
                        .subquery()
                )
            else:
                self._parameter_value_sq = self._parameter_value_sq_active_scenario_filtered()
        return self._parameter_value_sq

    def _active_scenario_ids(self):
        return {item.id for item in self.query(self.scenario_sq.c.id).filter(self.scenario_sq.c.active == True).all()}

    def _alternatives_ids(self, scenario_ids):
        return {
            item.alternative_id
            for item in self.query(
                self.scenario_alternative_sq.c.alternative_id, func.max(self.scenario_alternative_sq.c.rank)
            )
            .filter(self.scenario_alternative_sq.c.scenario_id.in_(scenario_ids))
            .group_by(self.scenario_alternative_sq.scenario_id)
            .all()
        }

    def _parameter_value_sq_overridden_scenario_filtered(self):
        active_alternatives_subquery = (
            self.query(self.scenario_alternative_sq.c.alternative_id, func.max(self.scenario_alternative_sq.c.rank))
            .filter(self.scenario_alternative_sq.c.scenario_id.in_(self._active_scenarios))
            .group_by(self.scenario_alternative_sq.c.scenario_id)
            .subquery()
        )
        return self._alternative_filtered_parameter_value_sq(active_alternatives_subquery)

    def _parameter_value_sq_active_scenario_filtered(self):
        """
        Returns a parameter value subquery which includes values of active scenarios or the Base alternative.

        Returns:
            Query: parameter value subquery
        """
        active_scenarios_subquery = (
            self.query(self.scenario_sq.c.id).filter(self.scenario_sq.c.active == True).subquery()
        )
        active_alternatives_subquery = (
            self.query(self.scenario_alternative_sq.c.alternative_id, func.max(self.scenario_alternative_sq.c.rank))
            .filter(self.scenario_alternative_sq.c.scenario_id == active_scenarios_subquery.c.id)
            .group_by(self.scenario_alternative_sq.c.scenario_id)
            .subquery()
        )
        return self._alternative_filtered_parameter_value_sq(active_alternatives_subquery)

    def _alternative_filtered_parameter_value_sq(self, active_alternatives_subquery):
        # Here we assume that 'Base' alternative has id == 1.
        which_alternative = case(
            [
                (
                    func.count(active_alternatives_subquery.c.alternative_id) != 0,
                    active_alternatives_subquery.c.alternative_id,
                )
            ],
            else_=1,
        )
        selected_alternatives_subquery = self.query(label("alternative_id", which_alternative)).subquery()
        subquery = super().parameter_value_sq
        return (
            self.query(subquery)
            .filter(subquery.c.alternative_id == selected_alternatives_subquery.c.alternative_id)
            .subquery()
        )
