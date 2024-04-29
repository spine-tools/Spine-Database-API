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

from types import MethodType
from sqlalchemy import Table, Integer, case, func, cast, and_, or_
from sqlalchemy.sql.expression import Alias, label
from sqlalchemy.orm import aliased
from .helpers import forward_sweep, group_concat


class DatabaseMappingQueryMixin:
    """Provides the :meth:`query` method for performing custom ``SELECT`` queries."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Subqueries that select everything from each table
        self._commit_sq = None
        self._alternative_sq = None
        self._scenario_sq = None
        self._scenario_alternative_sq = None
        self._entity_class_sq = None
        self._entity_sq = None
        self._entity_class_dimension_sq = None
        self._entity_element_sq = None
        self._entity_alternative_sq = None
        self._object_class_sq = None
        self._object_sq = None
        self._relationship_class_sq = None
        self._relationship_sq = None
        self._entity_group_sq = None
        self._parameter_definition_sq = None
        self._parameter_value_sq = None
        self._parameter_value_list_sq = None
        self._list_value_sq = None
        self._metadata_sq = None
        self._parameter_value_metadata_sq = None
        self._entity_metadata_sq = None
        self._superclass_subclass_sq = None
        # Special convenience subqueries that join two or more tables
        self._wide_entity_class_sq = None
        self._wide_entity_sq = None
        self._ext_parameter_value_list_sq = None
        self._wide_parameter_value_list_sq = None
        self._ord_list_value_sq = None
        self._ext_scenario_sq = None
        self._wide_scenario_sq = None
        self._linked_scenario_alternative_sq = None
        self._ext_linked_scenario_alternative_sq = None
        self._ext_object_sq = None
        self._ext_relationship_class_sq = None
        self._wide_relationship_class_sq = None
        self._ext_relationship_class_object_parameter_definition_sq = None
        self._wide_relationship_class_object_parameter_definition_sq = None
        self._ext_relationship_sq = None
        self._wide_relationship_sq = None
        self._ext_entity_group_sq = None
        self._entity_parameter_definition_sq = None
        self._object_parameter_definition_sq = None
        self._relationship_parameter_definition_sq = None
        self._entity_parameter_value_sq = None
        self._object_parameter_value_sq = None
        self._relationship_parameter_value_sq = None
        self._ext_parameter_value_metadata_sq = None
        self._ext_entity_metadata_sq = None
        self._import_alternative_name = None
        self._table_to_sq_attr = {}

    def _get_table_to_sq_attr(self):
        if not self._table_to_sq_attr:
            self._table_to_sq_attr = self._make_table_to_sq_attr()
        return self._table_to_sq_attr

    def _make_table_to_sq_attr(self):
        """Returns a dict mapping table names to subquery attribute names, involving that table."""

        def _func(x, tables):
            if isinstance(x, Table):
                tables.add(x.name)  # pylint: disable=cell-var-from-loop

        # This 'loads' our subquery attributes
        for attr in dir(self):
            getattr(self, attr)
        table_to_sq_attr = {}
        for attr, val in vars(self).items():
            if not isinstance(val, Alias):
                continue
            tables = set()
            forward_sweep(val, _func, tables)
            # Now `tables` contains all tables related to `val`
            for table in tables:
                table_to_sq_attr.setdefault(table, set()).add(attr)
        return table_to_sq_attr

    def _clear_subqueries(self, *tablenames):
        """Set to `None` subquery attributes involving the affected tables.
        This forces the subqueries to be refreshed when the corresponding property is accessed.
        """
        attr_names = set(attr for tablename in tablenames for attr in self._get_table_to_sq_attr().get(tablename, []))
        for attr_name in attr_names:
            setattr(self, attr_name, None)
        self.reset(*tablenames)

    def _subquery(self, tablename):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM tablename

        Args:
            tablename (str): the table to be queried.

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        table = self._metadata.tables[tablename]
        return self.query(table).subquery(tablename + "_sq")

    @property
    def superclass_subclass_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM superclass_subclass

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._superclass_subclass_sq is None:
            self._superclass_subclass_sq = self._subquery("superclass_subclass")
        return self._superclass_subclass_sq

    @property
    def entity_class_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM entity_class

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._entity_class_sq is None:
            self._entity_class_sq = self._make_entity_class_sq()
        return self._entity_class_sq

    @property
    def entity_class_dimension_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM entity_class_dimension

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._entity_class_dimension_sq is None:
            self._entity_class_dimension_sq = self._subquery("entity_class_dimension")
        return self._entity_class_dimension_sq

    @property
    def wide_entity_class_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT
                ec.*,
                count(ecd.dimension_id) AS dimension_count
                group_concat(ecd.dimension_id) AS dimension_id_list
            FROM
                entity_class AS ec
                entity_class_dimension AS ecd
            WHERE
                ec.id == ecd.entity_class_id

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._wide_entity_class_sq is None:
            entity_class_dimension_sq = (
                self.query(
                    self.entity_class_dimension_sq.c.entity_class_id,
                    self.entity_class_dimension_sq.c.dimension_id,
                    self.entity_class_dimension_sq.c.position,
                    self.entity_class_sq.c.name.label("dimension_name"),
                )
                .filter(self.entity_class_dimension_sq.c.dimension_id == self.entity_class_sq.c.id)
                .subquery("entity_class_dimension_sq")
            )
            ecd_sq = (
                self.query(
                    self.entity_class_sq.c.id,
                    self.entity_class_sq.c.name,
                    self.entity_class_sq.c.description,
                    self.entity_class_sq.c.display_order,
                    self.entity_class_sq.c.display_icon,
                    self.entity_class_sq.c.hidden,
                    self.entity_class_sq.c.active_by_default,
                    entity_class_dimension_sq.c.dimension_id,
                    entity_class_dimension_sq.c.dimension_name,
                    entity_class_dimension_sq.c.position,
                )
                .outerjoin(
                    entity_class_dimension_sq,
                    self.entity_class_sq.c.id == entity_class_dimension_sq.c.entity_class_id,
                )
                .order_by(self.entity_class_sq.c.id, entity_class_dimension_sq.c.position)
                .subquery("ext_entity_class_sq")
            )
            self._wide_entity_class_sq = (
                self.query(
                    ecd_sq.c.id,
                    ecd_sq.c.name,
                    ecd_sq.c.description,
                    ecd_sq.c.display_order,
                    ecd_sq.c.display_icon,
                    ecd_sq.c.hidden,
                    ecd_sq.c.active_by_default,
                    group_concat(ecd_sq.c.dimension_id, ecd_sq.c.position).label("dimension_id_list"),
                    group_concat(ecd_sq.c.dimension_name, ecd_sq.c.position).label("dimension_name_list"),
                    func.count(ecd_sq.c.dimension_id).label("dimension_count"),
                )
                .group_by(
                    ecd_sq.c.id,
                    ecd_sq.c.name,
                    ecd_sq.c.description,
                    ecd_sq.c.display_order,
                    ecd_sq.c.display_icon,
                    ecd_sq.c.hidden,
                )
                .subquery("wide_entity_class_sq")
            )
        return self._wide_entity_class_sq

    @property
    def entity_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM entity

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._entity_sq is None:
            self._entity_sq = self._make_entity_sq()
        return self._entity_sq

    @property
    def entity_element_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM entity_element

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._entity_element_sq is None:
            self._entity_element_sq = self._make_entity_element_sq()
        return self._entity_element_sq

    @property
    def wide_entity_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT
                e.*,
                count(ee.element_id) AS element_count
                group_concat(ee.element_id) AS element_id_list
            FROM
                entity AS e
                entity_element AS ee
            WHERE
                e.id == ee.entity_id

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._wide_entity_sq is None:
            entity_element_sq = (
                self.query(self.entity_element_sq, self.entity_sq.c.name.label("element_name"))
                .filter(self.entity_element_sq.c.element_id == self.entity_sq.c.id)
                .subquery("entity_element_sq")
            )
            ext_entity_sq = (
                self.query(self.entity_sq, entity_element_sq)
                .outerjoin(
                    entity_element_sq,
                    self.entity_sq.c.id == entity_element_sq.c.entity_id,
                )
                .order_by(self.entity_sq.c.id, entity_element_sq.c.position)
                .subquery("ext_entity_sq")
            )
            self._wide_entity_sq = (
                self.query(
                    ext_entity_sq.c.id,
                    ext_entity_sq.c.class_id,
                    ext_entity_sq.c.name,
                    ext_entity_sq.c.description,
                    ext_entity_sq.c.commit_id,
                    group_concat(ext_entity_sq.c.element_id, ext_entity_sq.c.position).label("element_id_list"),
                    group_concat(ext_entity_sq.c.element_name, ext_entity_sq.c.position).label("element_name_list"),
                )
                .group_by(
                    ext_entity_sq.c.id,
                    ext_entity_sq.c.class_id,
                    ext_entity_sq.c.name,
                    ext_entity_sq.c.description,
                    ext_entity_sq.c.commit_id,
                )
                .subquery("wide_entity_sq")
            )
        return self._wide_entity_sq

    @property
    def entity_group_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM entity_group

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._entity_group_sq is None:
            self._entity_group_sq = self._subquery("entity_group")
        return self._entity_group_sq

    @property
    def alternative_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM alternative

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._alternative_sq is None:
            self._alternative_sq = self._make_alternative_sq()
        return self._alternative_sq

    @property
    def scenario_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM scenario

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._scenario_sq is None:
            self._scenario_sq = self._make_scenario_sq()
        return self._scenario_sq

    @property
    def scenario_alternative_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM scenario_alternative

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._scenario_alternative_sq is None:
            self._scenario_alternative_sq = self._make_scenario_alternative_sq()
        return self._scenario_alternative_sq

    @property
    def entity_alternative_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM entity_alternative

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._entity_alternative_sq is None:
            self._entity_alternative_sq = self._subquery("entity_alternative")
        return self._entity_alternative_sq

    @property
    def parameter_value_list_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM parameter_value_list

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._parameter_value_list_sq is None:
            self._parameter_value_list_sq = self._subquery("parameter_value_list")
        return self._parameter_value_list_sq

    @property
    def list_value_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM list_value

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._list_value_sq is None:
            self._list_value_sq = self._subquery("list_value")
        return self._list_value_sq

    @property
    def parameter_definition_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM parameter_definition

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """

        if self._parameter_definition_sq is None:
            self._parameter_definition_sq = self._make_parameter_definition_sq()
        return self._parameter_definition_sq

    @property
    def parameter_value_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM parameter_value

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._parameter_value_sq is None:
            self._parameter_value_sq = self._make_parameter_value_sq()
        return self._parameter_value_sq

    @property
    def metadata_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM list_value

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._metadata_sq is None:
            self._metadata_sq = self._subquery("metadata")
        return self._metadata_sq

    @property
    def parameter_value_metadata_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM parameter_value_metadata

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._parameter_value_metadata_sq is None:
            self._parameter_value_metadata_sq = self._subquery("parameter_value_metadata")
        return self._parameter_value_metadata_sq

    @property
    def entity_metadata_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM entity_metadata

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._entity_metadata_sq is None:
            self._entity_metadata_sq = self._subquery("entity_metadata")
        return self._entity_metadata_sq

    @property
    def commit_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM commit

        Returns:
            :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._commit_sq is None:
            commit_sq = self._subquery("commit")
            self._commit_sq = self.query(commit_sq).filter(commit_sq.c.comment != "").subquery()
        return self._commit_sq

    @property
    def object_class_sq(self):
        if self._object_class_sq is None:
            self._object_class_sq = (
                self.query(
                    self.wide_entity_class_sq.c.id.label("id"),
                    self.wide_entity_class_sq.c.name.label("name"),
                    self.wide_entity_class_sq.c.description.label("description"),
                    self.wide_entity_class_sq.c.display_order.label("display_order"),
                    self.wide_entity_class_sq.c.display_icon.label("display_icon"),
                    self.wide_entity_class_sq.c.hidden.label("hidden"),
                )
                .filter(self.wide_entity_class_sq.c.dimension_id_list == None)
                .subquery("object_class_sq")
            )
        return self._object_class_sq

    @property
    def object_sq(self):
        if self._object_sq is None:
            self._object_sq = (
                self.query(
                    self.wide_entity_sq.c.id.label("id"),
                    self.wide_entity_sq.c.class_id.label("class_id"),
                    self.wide_entity_sq.c.name.label("name"),
                    self.wide_entity_sq.c.description.label("description"),
                    self.wide_entity_sq.c.commit_id.label("commit_id"),
                )
                .filter(self.wide_entity_sq.c.element_id_list == None)
                .subquery("object_sq")
            )
        return self._object_sq

    @property
    def relationship_class_sq(self):
        if self._relationship_class_sq is None:
            ent_cls_dim_sq = self._subquery("entity_class_dimension")
            self._relationship_class_sq = (
                self.query(
                    ent_cls_dim_sq.c.entity_class_id.label("id"),
                    ent_cls_dim_sq.c.position.label("dimension"),  # NOTE: nothing to do with the `dimension` concept
                    ent_cls_dim_sq.c.dimension_id.label("object_class_id"),
                    self.wide_entity_class_sq.c.name.label("name"),
                    self.wide_entity_class_sq.c.description.label("description"),
                    self.wide_entity_class_sq.c.display_icon.label("display_icon"),
                    self.wide_entity_class_sq.c.hidden.label("hidden"),
                )
                .filter(self.wide_entity_class_sq.c.id == ent_cls_dim_sq.c.entity_class_id)
                .subquery("relationship_class_sq")
            )
        return self._relationship_class_sq

    @property
    def relationship_sq(self):
        if self._relationship_sq is None:
            ent_el_sq = self._subquery("entity_element")
            self._relationship_sq = (
                self.query(
                    ent_el_sq.c.entity_id.label("id"),
                    ent_el_sq.c.position.label("dimension"),  # NOTE: nothing to do with the `dimension` concept
                    ent_el_sq.c.element_id.label("object_id"),
                    ent_el_sq.c.entity_class_id.label("class_id"),
                    self.wide_entity_sq.c.name.label("name"),
                    self.wide_entity_sq.c.commit_id.label("commit_id"),
                )
                .filter(self.wide_entity_sq.c.id == ent_el_sq.c.entity_id)
                .subquery("relationship_sq")
            )
        return self._relationship_sq

    @property
    def ext_parameter_value_list_sq(self):
        if self._ext_parameter_value_list_sq is None:
            self._ext_parameter_value_list_sq = (
                self.query(
                    self.parameter_value_list_sq.c.id,
                    self.parameter_value_list_sq.c.name,
                    self.parameter_value_list_sq.c.commit_id,
                    self.list_value_sq.c.id.label("value_id"),
                    self.list_value_sq.c.index.label("value_index"),
                ).outerjoin(
                    self.list_value_sq,
                    self.list_value_sq.c.parameter_value_list_id == self.parameter_value_list_sq.c.id,
                )
            ).subquery()
        return self._ext_parameter_value_list_sq

    @property
    def wide_parameter_value_list_sq(self):
        if self._wide_parameter_value_list_sq is None:
            self._wide_parameter_value_list_sq = (
                self.query(
                    self.ext_parameter_value_list_sq.c.id,
                    self.ext_parameter_value_list_sq.c.name,
                    self.ext_parameter_value_list_sq.c.commit_id,
                    group_concat(
                        self.ext_parameter_value_list_sq.c.value_id, self.ext_parameter_value_list_sq.c.value_index
                    ).label("value_id_list"),
                    group_concat(
                        self.ext_parameter_value_list_sq.c.value_index, self.ext_parameter_value_list_sq.c.value_index
                    ).label("value_index_list"),
                ).group_by(
                    self.ext_parameter_value_list_sq.c.id,
                    self.ext_parameter_value_list_sq.c.name,
                    self.ext_parameter_value_list_sq.c.commit_id,
                )
            ).subquery()
        return self._wide_parameter_value_list_sq

    @property
    def ord_list_value_sq(self):
        if self._ord_list_value_sq is None:
            self._ord_list_value_sq = (
                self.query(
                    self.list_value_sq.c.id,
                    self.list_value_sq.c.parameter_value_list_id,
                    self.list_value_sq.c.index,
                    self.list_value_sq.c.value,
                    self.list_value_sq.c.type,
                    self.list_value_sq.c.commit_id,
                )
                .order_by(self.list_value_sq.c.parameter_value_list_id, self.list_value_sq.c.index)
                .subquery()
            )
        return self._ord_list_value_sq

    @property
    def ext_scenario_sq(self):
        if self._ext_scenario_sq is None:
            self._ext_scenario_sq = (
                self.query(
                    self.scenario_sq.c.id.label("id"),
                    self.scenario_sq.c.name.label("name"),
                    self.scenario_sq.c.description.label("description"),
                    self.scenario_sq.c.active.label("active"),
                    self.scenario_alternative_sq.c.alternative_id.label("alternative_id"),
                    self.scenario_alternative_sq.c.rank.label("rank"),
                    self.alternative_sq.c.name.label("alternative_name"),
                    self.scenario_sq.c.commit_id.label("commit_id"),
                )
                .outerjoin(
                    self.scenario_alternative_sq, self.scenario_alternative_sq.c.scenario_id == self.scenario_sq.c.id
                )
                .outerjoin(
                    self.alternative_sq, self.alternative_sq.c.id == self.scenario_alternative_sq.c.alternative_id
                )
                .order_by(self.scenario_sq.c.id, self.scenario_alternative_sq.c.rank)
                .subquery()
            )
        return self._ext_scenario_sq

    @property
    def wide_scenario_sq(self):
        if self._wide_scenario_sq is None:
            self._wide_scenario_sq = (
                self.query(
                    self.ext_scenario_sq.c.id.label("id"),
                    self.ext_scenario_sq.c.name.label("name"),
                    self.ext_scenario_sq.c.description.label("description"),
                    self.ext_scenario_sq.c.active.label("active"),
                    self.ext_scenario_sq.c.commit_id.label("commit_id"),
                    group_concat(self.ext_scenario_sq.c.alternative_id, self.ext_scenario_sq.c.rank).label(
                        "alternative_id_list"
                    ),
                    group_concat(self.ext_scenario_sq.c.alternative_name, self.ext_scenario_sq.c.rank).label(
                        "alternative_name_list"
                    ),
                )
                .group_by(
                    self.ext_scenario_sq.c.id,
                    self.ext_scenario_sq.c.name,
                    self.ext_scenario_sq.c.description,
                    self.ext_scenario_sq.c.active,
                    self.ext_scenario_sq.c.commit_id,
                )
                .subquery()
            )
        return self._wide_scenario_sq

    @property
    def linked_scenario_alternative_sq(self):
        if self._linked_scenario_alternative_sq is None:
            scenario_next_alternative = aliased(self.scenario_alternative_sq)
            self._linked_scenario_alternative_sq = (
                self.query(
                    self.scenario_alternative_sq.c.id.label("id"),
                    self.scenario_alternative_sq.c.scenario_id.label("scenario_id"),
                    self.scenario_alternative_sq.c.alternative_id.label("alternative_id"),
                    self.scenario_alternative_sq.c.rank.label("rank"),
                    scenario_next_alternative.c.alternative_id.label("before_alternative_id"),
                    scenario_next_alternative.c.rank.label("before_rank"),
                    self.scenario_alternative_sq.c.commit_id.label("commit_id"),
                )
                .outerjoin(
                    scenario_next_alternative,
                    and_(
                        scenario_next_alternative.c.scenario_id == self.scenario_alternative_sq.c.scenario_id,
                        scenario_next_alternative.c.rank == self.scenario_alternative_sq.c.rank + 1,
                    ),
                )
                .order_by(self.scenario_alternative_sq.c.scenario_id, self.scenario_alternative_sq.c.rank)
                .subquery()
            )
        return self._linked_scenario_alternative_sq

    @property
    def ext_linked_scenario_alternative_sq(self):
        if self._ext_linked_scenario_alternative_sq is None:
            next_alternative = aliased(self.alternative_sq)
            self._ext_linked_scenario_alternative_sq = (
                self.query(
                    self.linked_scenario_alternative_sq.c.id.label("id"),
                    self.linked_scenario_alternative_sq.c.scenario_id.label("scenario_id"),
                    self.scenario_sq.c.name.label("scenario_name"),
                    self.linked_scenario_alternative_sq.c.alternative_id.label("alternative_id"),
                    self.alternative_sq.c.name.label("alternative_name"),
                    self.linked_scenario_alternative_sq.c.rank.label("rank"),
                    self.linked_scenario_alternative_sq.c.before_alternative_id.label("before_alternative_id"),
                    self.linked_scenario_alternative_sq.c.before_rank.label("before_rank"),
                    next_alternative.c.name.label("before_alternative_name"),
                    self.linked_scenario_alternative_sq.c.commit_id.label("commit_id"),
                )
                .filter(self.linked_scenario_alternative_sq.c.scenario_id == self.scenario_sq.c.id)
                .filter(self.alternative_sq.c.id == self.linked_scenario_alternative_sq.c.alternative_id)
                .outerjoin(
                    next_alternative,
                    next_alternative.c.id == self.linked_scenario_alternative_sq.c.before_alternative_id,
                )
                .subquery()
            )
        return self._ext_linked_scenario_alternative_sq

    @property
    def ext_object_sq(self):
        if self._ext_object_sq is None:
            self._ext_object_sq = (
                self.query(
                    self.object_sq.c.id.label("id"),
                    self.object_sq.c.class_id.label("class_id"),
                    self.object_class_sq.c.name.label("class_name"),
                    self.object_sq.c.name.label("name"),
                    self.object_sq.c.description.label("description"),
                    self.entity_group_sq.c.entity_id.label("group_id"),
                    self.object_sq.c.commit_id.label("commit_id"),
                )
                .filter(self.object_sq.c.class_id == self.object_class_sq.c.id)
                .outerjoin(self.entity_group_sq, self.entity_group_sq.c.entity_id == self.object_sq.c.id)
                .distinct(self.entity_group_sq.c.entity_id)
                .subquery()
            )
        return self._ext_object_sq

    @property
    def ext_relationship_class_sq(self):
        if self._ext_relationship_class_sq is None:
            self._ext_relationship_class_sq = (
                self.query(
                    self.relationship_class_sq.c.id.label("id"),
                    self.relationship_class_sq.c.name.label("name"),
                    self.relationship_class_sq.c.description.label("description"),
                    self.relationship_class_sq.c.dimension.label("dimension"),
                    self.relationship_class_sq.c.display_icon.label("display_icon"),
                    self.object_class_sq.c.id.label("object_class_id"),
                    self.object_class_sq.c.name.label("object_class_name"),
                )
                .filter(self.relationship_class_sq.c.object_class_id == self.object_class_sq.c.id)
                .order_by(self.relationship_class_sq.c.id, self.relationship_class_sq.c.dimension)
                .subquery()
            )
        return self._ext_relationship_class_sq

    @property
    def wide_relationship_class_sq(self):
        if self._wide_relationship_class_sq is None:
            self._wide_relationship_class_sq = (
                self.query(
                    self.ext_relationship_class_sq.c.id,
                    self.ext_relationship_class_sq.c.name,
                    self.ext_relationship_class_sq.c.description,
                    self.ext_relationship_class_sq.c.display_icon,
                    group_concat(
                        self.ext_relationship_class_sq.c.object_class_id, self.ext_relationship_class_sq.c.dimension
                    ).label("object_class_id_list"),
                    group_concat(
                        self.ext_relationship_class_sq.c.object_class_name, self.ext_relationship_class_sq.c.dimension
                    ).label("object_class_name_list"),
                )
                .group_by(
                    self.ext_relationship_class_sq.c.id,
                    self.ext_relationship_class_sq.c.name,
                    self.ext_relationship_class_sq.c.description,
                    self.ext_relationship_class_sq.c.display_icon,
                )
                .subquery()
            )
        return self._wide_relationship_class_sq

    @property
    def ext_relationship_sq(self):
        if self._ext_relationship_sq is None:
            self._ext_relationship_sq = (
                self.query(
                    self.relationship_sq.c.id.label("id"),
                    self.relationship_sq.c.name.label("name"),
                    self.relationship_sq.c.class_id.label("class_id"),
                    self.relationship_sq.c.dimension.label("dimension"),
                    self.wide_relationship_class_sq.c.name.label("class_name"),
                    self.ext_object_sq.c.id.label("object_id"),
                    self.ext_object_sq.c.name.label("object_name"),
                    self.ext_object_sq.c.class_id.label("object_class_id"),
                    self.ext_object_sq.c.class_name.label("object_class_name"),
                    self.relationship_sq.c.commit_id.label("commit_id"),
                )
                .filter(self.relationship_sq.c.class_id == self.wide_relationship_class_sq.c.id)
                .outerjoin(self.ext_object_sq, self.relationship_sq.c.object_id == self.ext_object_sq.c.id)
                .order_by(self.relationship_sq.c.id, self.relationship_sq.c.dimension)
                .subquery()
            )
        return self._ext_relationship_sq

    @property
    def wide_relationship_sq(self):
        if self._wide_relationship_sq is None:
            self._wide_relationship_sq = (
                self.query(
                    self.ext_relationship_sq.c.id,
                    self.ext_relationship_sq.c.name,
                    self.ext_relationship_sq.c.class_id,
                    self.ext_relationship_sq.c.class_name,
                    self.ext_relationship_sq.c.commit_id,
                    group_concat(self.ext_relationship_sq.c.object_id, self.ext_relationship_sq.c.dimension).label(
                        "object_id_list"
                    ),
                    group_concat(self.ext_relationship_sq.c.object_name, self.ext_relationship_sq.c.dimension).label(
                        "object_name_list"
                    ),
                    group_concat(
                        self.ext_relationship_sq.c.object_class_id, self.ext_relationship_sq.c.dimension
                    ).label("object_class_id_list"),
                    group_concat(
                        self.ext_relationship_sq.c.object_class_name, self.ext_relationship_sq.c.dimension
                    ).label("object_class_name_list"),
                )
                .group_by(
                    self.ext_relationship_sq.c.id,
                    self.ext_relationship_sq.c.name,
                    self.ext_relationship_sq.c.class_id,
                    self.ext_relationship_sq.c.class_name,
                    self.ext_relationship_sq.c.commit_id,
                )
                # dimension count might be higher than object count when objects have been filtered out
                .having(
                    func.count(self.ext_relationship_sq.c.dimension) == func.count(self.ext_relationship_sq.c.object_id)
                )
                .subquery()
            )
        return self._wide_relationship_sq

    @property
    def ext_entity_group_sq(self):
        if self._ext_entity_group_sq is None:
            group_entity = aliased(self.entity_sq)
            member_entity = aliased(self.entity_sq)
            self._ext_entity_group_sq = (
                self.query(
                    self.entity_group_sq.c.id.label("id"),
                    self.entity_group_sq.c.entity_class_id.label("class_id"),
                    self.entity_group_sq.c.entity_id.label("group_id"),
                    self.entity_group_sq.c.member_id.label("member_id"),
                    self.wide_entity_class_sq.c.name.label("entity_class_name"),
                    group_entity.c.name.label("group_name"),
                    member_entity.c.name.label("member_name"),
                    label("object_class_id", self._object_class_id()),
                    label("relationship_class_id", self._relationship_class_id()),
                )
                .filter(self.entity_group_sq.c.entity_class_id == self.wide_entity_class_sq.c.id)
                .join(group_entity, self.entity_group_sq.c.entity_id == group_entity.c.id)
                .join(member_entity, self.entity_group_sq.c.member_id == member_entity.c.id)
                .subquery()
            )
        return self._ext_entity_group_sq

    @property
    def entity_parameter_definition_sq(self):
        if self._entity_parameter_definition_sq is None:
            self._entity_parameter_definition_sq = (
                self.query(
                    self.parameter_definition_sq.c.id.label("id"),
                    self.parameter_definition_sq.c.entity_class_id,
                    self.wide_entity_class_sq.c.name.label("entity_class_name"),
                    label("object_class_id", self._object_class_id()),
                    label("relationship_class_id", self._relationship_class_id()),
                    label("object_class_name", self._object_class_name()),
                    label("relationship_class_name", self._relationship_class_name()),
                    label("object_class_id_list", self._object_class_id_list()),
                    label("object_class_name_list", self._object_class_name_list()),
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.parameter_definition_sq.c.parameter_value_list_id.label("value_list_id"),
                    self.parameter_value_list_sq.c.name.label("value_list_name"),
                    self.parameter_definition_sq.c.default_value,
                    self.parameter_definition_sq.c.default_type,
                    self.parameter_definition_sq.c.list_value_id,
                    self.parameter_definition_sq.c.description,
                    self.parameter_definition_sq.c.commit_id,
                )
                .join(
                    self.wide_entity_class_sq,
                    self.wide_entity_class_sq.c.id == self.parameter_definition_sq.c.entity_class_id,
                )
                .outerjoin(
                    self.parameter_value_list_sq,
                    self.parameter_value_list_sq.c.id == self.parameter_definition_sq.c.parameter_value_list_id,
                )
                .outerjoin(
                    self.wide_relationship_class_sq,
                    self.wide_relationship_class_sq.c.id == self.wide_entity_class_sq.c.id,
                )
                .subquery()
            )
        return self._entity_parameter_definition_sq

    @property
    def object_parameter_definition_sq(self):
        if self._object_parameter_definition_sq is None:
            self._object_parameter_definition_sq = (
                self.query(
                    self.parameter_definition_sq.c.id.label("id"),
                    self.parameter_definition_sq.c.entity_class_id,
                    self.object_class_sq.c.name.label("entity_class_name"),
                    self.object_class_sq.c.id.label("object_class_id"),
                    self.object_class_sq.c.name.label("object_class_name"),
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.parameter_definition_sq.c.parameter_value_list_id.label("value_list_id"),
                    self.parameter_value_list_sq.c.name.label("value_list_name"),
                    self.parameter_definition_sq.c.default_value,
                    self.parameter_definition_sq.c.default_type,
                    self.parameter_definition_sq.c.description,
                )
                .filter(self.object_class_sq.c.id == self.parameter_definition_sq.c.entity_class_id)
                .outerjoin(
                    self.parameter_value_list_sq,
                    self.parameter_value_list_sq.c.id == self.parameter_definition_sq.c.parameter_value_list_id,
                )
                .subquery()
            )
        return self._object_parameter_definition_sq

    @property
    def relationship_parameter_definition_sq(self):
        if self._relationship_parameter_definition_sq is None:
            self._relationship_parameter_definition_sq = (
                self.query(
                    self.parameter_definition_sq.c.id.label("id"),
                    self.parameter_definition_sq.c.entity_class_id,
                    self.wide_relationship_class_sq.c.name.label("entity_class_name"),
                    self.wide_relationship_class_sq.c.id.label("relationship_class_id"),
                    self.wide_relationship_class_sq.c.name.label("relationship_class_name"),
                    self.wide_relationship_class_sq.c.object_class_id_list,
                    self.wide_relationship_class_sq.c.object_class_name_list,
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.parameter_definition_sq.c.parameter_value_list_id.label("value_list_id"),
                    self.parameter_value_list_sq.c.name.label("value_list_name"),
                    self.parameter_definition_sq.c.default_value,
                    self.parameter_definition_sq.c.default_type,
                    self.parameter_definition_sq.c.description,
                )
                .filter(self.parameter_definition_sq.c.entity_class_id == self.wide_relationship_class_sq.c.id)
                .outerjoin(
                    self.parameter_value_list_sq,
                    self.parameter_value_list_sq.c.id == self.parameter_definition_sq.c.parameter_value_list_id,
                )
                .subquery()
            )
        return self._relationship_parameter_definition_sq

    @property
    def entity_parameter_value_sq(self):
        if self._entity_parameter_value_sq is None:
            self._entity_parameter_value_sq = (
                self.query(
                    self.parameter_value_sq.c.id.label("id"),
                    self.parameter_definition_sq.c.entity_class_id,
                    self.wide_entity_class_sq.c.name.label("entity_class_name"),
                    label("object_class_id", self._object_class_id()),
                    label("relationship_class_id", self._relationship_class_id()),
                    label("object_class_name", self._object_class_name()),
                    label("relationship_class_name", self._relationship_class_name()),
                    label("object_class_id_list", self._object_class_id_list()),
                    label("object_class_name_list", self._object_class_name_list()),
                    self.parameter_value_sq.c.entity_id,
                    self.wide_entity_sq.c.name.label("entity_name"),
                    label("object_id", self._object_id()),
                    label("relationship_id", self._relationship_id()),
                    label("object_name", self._object_name()),
                    label("object_id_list", self._object_id_list()),
                    label("object_name_list", self._object_name_list()),
                    self.parameter_definition_sq.c.id.label("parameter_id"),
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.parameter_value_sq.c.alternative_id,
                    self.alternative_sq.c.name.label("alternative_name"),
                    self.parameter_value_sq.c.value,
                    self.parameter_value_sq.c.type,
                    self.parameter_value_sq.c.list_value_id,
                    self.parameter_value_sq.c.commit_id,
                )
                .join(
                    self.parameter_definition_sq,
                    self.parameter_definition_sq.c.id == self.parameter_value_sq.c.parameter_definition_id,
                )
                .join(self.wide_entity_sq, self.parameter_value_sq.c.entity_id == self.wide_entity_sq.c.id)
                .join(
                    self.wide_entity_class_sq,
                    self.parameter_definition_sq.c.entity_class_id == self.wide_entity_class_sq.c.id,
                )
                .join(self.alternative_sq, self.parameter_value_sq.c.alternative_id == self.alternative_sq.c.id)
                .outerjoin(
                    self.wide_relationship_class_sq,
                    self.wide_relationship_class_sq.c.id == self.wide_entity_class_sq.c.id,
                )
                .outerjoin(self.wide_relationship_sq, self.wide_relationship_sq.c.id == self.wide_entity_sq.c.id)
                # object_id_list might be None when objects have been filtered out
                .filter(
                    or_(
                        self.wide_relationship_sq.c.id.is_(None),
                        self.wide_relationship_sq.c.object_id_list.isnot(None),
                    )
                )
                .subquery()
            )
        return self._entity_parameter_value_sq

    @property
    def object_parameter_value_sq(self):
        if self._object_parameter_value_sq is None:
            self._object_parameter_value_sq = (
                self.query(
                    self.parameter_value_sq.c.id.label("id"),
                    self.parameter_definition_sq.c.entity_class_id,
                    self.object_class_sq.c.id.label("object_class_id"),
                    self.object_class_sq.c.name.label("object_class_name"),
                    self.parameter_value_sq.c.entity_id,
                    self.object_sq.c.id.label("object_id"),
                    self.object_sq.c.name.label("object_name"),
                    self.parameter_definition_sq.c.id.label("parameter_id"),
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.parameter_value_sq.c.alternative_id,
                    self.alternative_sq.c.name.label("alternative_name"),
                    self.parameter_value_sq.c.value,
                    self.parameter_value_sq.c.type,
                )
                .filter(self.parameter_definition_sq.c.id == self.parameter_value_sq.c.parameter_definition_id)
                .filter(self.parameter_value_sq.c.entity_id == self.object_sq.c.id)
                .filter(self.parameter_definition_sq.c.entity_class_id == self.object_class_sq.c.id)
                .filter(self.parameter_value_sq.c.alternative_id == self.alternative_sq.c.id)
                .subquery()
            )
        return self._object_parameter_value_sq

    @property
    def relationship_parameter_value_sq(self):
        if self._relationship_parameter_value_sq is None:
            self._relationship_parameter_value_sq = (
                self.query(
                    self.parameter_value_sq.c.id.label("id"),
                    self.parameter_definition_sq.c.entity_class_id,
                    self.wide_relationship_class_sq.c.id.label("relationship_class_id"),
                    self.wide_relationship_class_sq.c.name.label("relationship_class_name"),
                    self.wide_relationship_class_sq.c.object_class_id_list,
                    self.wide_relationship_class_sq.c.object_class_name_list,
                    self.parameter_value_sq.c.entity_id,
                    self.wide_relationship_sq.c.id.label("relationship_id"),
                    self.wide_relationship_sq.c.object_id_list,
                    self.wide_relationship_sq.c.object_name_list,
                    self.parameter_definition_sq.c.id.label("parameter_id"),
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.parameter_value_sq.c.alternative_id,
                    self.alternative_sq.c.name.label("alternative_name"),
                    self.parameter_value_sq.c.value,
                    self.parameter_value_sq.c.type,
                )
                .filter(self.parameter_definition_sq.c.id == self.parameter_value_sq.c.parameter_definition_id)
                .filter(self.parameter_value_sq.c.entity_id == self.wide_relationship_sq.c.id)
                .filter(self.parameter_definition_sq.c.entity_class_id == self.wide_relationship_class_sq.c.id)
                .filter(self.parameter_value_sq.c.alternative_id == self.alternative_sq.c.id)
                .subquery()
            )
        return self._relationship_parameter_value_sq

    @property
    def ext_parameter_value_metadata_sq(self):
        if self._ext_parameter_value_metadata_sq is None:
            self._ext_parameter_value_metadata_sq = (
                self.query(
                    self.parameter_value_metadata_sq.c.id,
                    self.parameter_value_metadata_sq.c.parameter_value_id,
                    self.metadata_sq.c.id.label("metadata_id"),
                    self.entity_sq.c.name.label("entity_name"),
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.alternative_sq.c.name.label("alternative_name"),
                    self.metadata_sq.c.name.label("metadata_name"),
                    self.metadata_sq.c.value.label("metadata_value"),
                    self.parameter_value_metadata_sq.c.commit_id,
                )
                .filter(self.parameter_value_metadata_sq.c.parameter_value_id == self.parameter_value_sq.c.id)
                .filter(self.parameter_value_sq.c.parameter_definition_id == self.parameter_definition_sq.c.id)
                .filter(self.parameter_value_sq.c.entity_id == self.entity_sq.c.id)
                .filter(self.parameter_value_sq.c.alternative_id == self.alternative_sq.c.id)
                .filter(self.parameter_value_metadata_sq.c.metadata_id == self.metadata_sq.c.id)
                .subquery()
            )
        return self._ext_parameter_value_metadata_sq

    @property
    def ext_entity_metadata_sq(self):
        if self._ext_entity_metadata_sq is None:
            self._ext_entity_metadata_sq = (
                self.query(
                    self.entity_metadata_sq.c.id,
                    self.entity_metadata_sq.c.entity_id,
                    self.metadata_sq.c.id.label("metadata_id"),
                    self.entity_sq.c.name.label("entity_name"),
                    self.metadata_sq.c.name.label("metadata_name"),
                    self.metadata_sq.c.value.label("metadata_value"),
                    self.entity_metadata_sq.c.commit_id,
                )
                .filter(self.entity_metadata_sq.c.entity_id == self.entity_sq.c.id)
                .filter(self.entity_metadata_sq.c.metadata_id == self.metadata_sq.c.id)
                .subquery()
            )
        return self._ext_entity_metadata_sq

    def _make_entity_class_sq(self):
        """
        Creates a subquery for entity classes.

        Returns:
            Alias: an entity class subquery
        """
        return self._subquery("entity_class")

    def _make_entity_sq(self):
        """
        Creates a subquery for entities.

        Returns:
            Alias: an entity subquery
        """
        return self._subquery("entity")

    def _make_entity_element_sq(self):
        """
        Creates a subquery for entity-elements.

        Returns:
            Alias: an entity_element subquery
        """
        return self._subquery("entity_element")

    def _make_parameter_definition_sq(self):
        """
        Creates a subquery for parameter definitions.

        Returns:
            Alias: a parameter definition subquery
        """
        par_def_sq = self._subquery("parameter_definition")
        list_value_id = case(
            [(par_def_sq.c.default_type == "list_value_ref", cast(par_def_sq.c.default_value, Integer()))], else_=None
        )
        default_value = case(
            [(par_def_sq.c.default_type == "list_value_ref", self.list_value_sq.c.value)],
            else_=par_def_sq.c.default_value,
        )
        default_type = case(
            [(par_def_sq.c.default_type == "list_value_ref", self.list_value_sq.c.type)],
            else_=par_def_sq.c.default_type,
        )
        return (
            self.query(
                par_def_sq.c.id.label("id"),
                par_def_sq.c.name.label("name"),
                par_def_sq.c.description.label("description"),
                par_def_sq.c.entity_class_id,
                label("default_value", default_value),
                label("default_type", default_type),
                label("list_value_id", list_value_id),
                par_def_sq.c.commit_id.label("commit_id"),
                par_def_sq.c.parameter_value_list_id.label("parameter_value_list_id"),
            )
            .outerjoin(self.list_value_sq, self.list_value_sq.c.id == list_value_id)
            .subquery("clean_parameter_definition_sq")
        )

    def _make_parameter_value_sq(self):
        """
        Creates a subquery for parameter values.

        Returns:
            Alias: a parameter value subquery
        """
        par_val_sq = self._subquery("parameter_value")
        list_value_id = case([(par_val_sq.c.type == "list_value_ref", cast(par_val_sq.c.value, Integer()))], else_=None)
        value = case([(par_val_sq.c.type == "list_value_ref", self.list_value_sq.c.value)], else_=par_val_sq.c.value)
        type_ = case([(par_val_sq.c.type == "list_value_ref", self.list_value_sq.c.type)], else_=par_val_sq.c.type)
        return (
            self.query(
                par_val_sq.c.id.label("id"),
                par_val_sq.c.parameter_definition_id,
                par_val_sq.c.entity_class_id,
                par_val_sq.c.entity_id,
                label("value", value),
                label("type", type_),
                label("list_value_id", list_value_id),
                par_val_sq.c.commit_id.label("commit_id"),
                par_val_sq.c.alternative_id,
            )
            .filter(par_val_sq.c.entity_id == self.entity_sq.c.id)
            .outerjoin(self.list_value_sq, self.list_value_sq.c.id == list_value_id)
            .subquery("clean_parameter_value_sq")
        )

    def _make_alternative_sq(self):
        """
        Creates a subquery for alternatives.

        Returns:
            Alias: an alternative subquery
        """
        return self._subquery("alternative")

    def _make_scenario_sq(self):
        """
        Creates a subquery for scenarios.

        Returns:
            Alias: a scenario subquery
        """
        return self._subquery("scenario")

    def _make_scenario_alternative_sq(self):
        """
        Creates a subquery for scenario alternatives.

        Returns:
            Alias: a scenario alternative subquery
        """
        return self._subquery("scenario_alternative")

    def override_entity_class_sq_maker(self, method):
        """
        Overrides the function that creates the ``entity_class_sq`` property.

        Args:
            method (Callable): a function that accepts a :class:`DatabaseMapping` as its argument and
                returns entity class subquery as an :class:`Alias` object
        """
        self._make_entity_class_sq = MethodType(method, self)
        self._clear_subqueries("entity_class")

    def override_entity_sq_maker(self, method):
        """
        Overrides the function that creates the ``entity_sq`` property.

        Args:
            method (Callable): a function that accepts a :class:`DatabaseMapping` as its argument and
                returns entity subquery as an :class:`Alias` object
        """
        self._make_entity_sq = MethodType(method, self)
        self._clear_subqueries("entity")

    def override_entity_element_sq_maker(self, method):
        """
        Overrides the function that creates the ``entity_element_sq`` property.

        Args:
            method (Callable): a function that accepts a :class:`DatabaseMapping` as its argument and
                returns entity_element subquery as an :class:`Alias` object
        """
        self._make_entity_element_sq = MethodType(method, self)
        self._clear_subqueries("entity_element")

    def override_parameter_definition_sq_maker(self, method):
        """
        Overrides the function that creates the ``parameter_definition_sq`` property.

        Args:
            method (Callable): a function that accepts a :class:`DatabaseMapping` as its argument and
                returns parameter definition subquery as an :class:`Alias` object
        """
        self._make_parameter_definition_sq = MethodType(method, self)
        self._clear_subqueries("parameter_definition")

    def override_parameter_value_sq_maker(self, method):
        """
        Overrides the function that creates the ``parameter_value_sq`` property.

        Args:
            method (Callable): a function that accepts a :class:`DatabaseMapping` as its argument and
                returns parameter value subquery as an :class:`Alias` object
        """
        self._make_parameter_value_sq = MethodType(method, self)
        self._clear_subqueries("parameter_value")

    def override_alternative_sq_maker(self, method):
        """
        Overrides the function that creates the ``alternative_sq`` property.

        Args:
            method (Callable): a function that accepts a :class:`DatabaseMapping` as its argument and
                returns alternative subquery as an :class:`Alias` object
        """
        self._make_alternative_sq = MethodType(method, self)
        self._clear_subqueries("alternative")

    def override_scenario_sq_maker(self, method):
        """
        Overrides the function that creates the ``scenario_sq`` property.

        Args:
            method (Callable): a function that accepts a :class:`DatabaseMapping` as its argument and
                returns scenario subquery as an :class:`Alias` object
        """
        self._make_scenario_sq = MethodType(method, self)
        self._clear_subqueries("scenario")

    def override_scenario_alternative_sq_maker(self, method):
        """
        Overrides the function that creates the ``scenario_alternative_sq`` property.

        Args:
            method (Callable): a function that accepts a :class:`DatabaseMapping` as its argument and
                returns scenario alternative subquery as an :class:`Alias` object
        """
        self._make_scenario_alternative_sq = MethodType(method, self)
        self._clear_subqueries("scenario_alternative")

    def restore_entity_class_sq_maker(self):
        """Restores the original function that creates the ``entity_class_sq`` property."""
        self._make_entity_class_sq = MethodType(DatabaseMappingQueryMixin._make_entity_class_sq, self)
        self._clear_subqueries("entity_class")

    def restore_entity_sq_maker(self):
        """Restores the original function that creates the ``entity_sq`` property."""
        self._make_entity_sq = MethodType(DatabaseMappingQueryMixin._make_entity_sq, self)
        self._clear_subqueries("entity")

    def restore_entity_element_sq_maker(self):
        """Restores the original function that creates the ``entity_element_sq`` property."""
        self._make_entity_element_sq = MethodType(DatabaseMappingQueryMixin._make_entity_element_sq, self)
        self._clear_subqueries("entity_element")

    def restore_parameter_definition_sq_maker(self):
        """Restores the original function that creates the ``parameter_definition_sq`` property."""
        self._make_parameter_definition_sq = MethodType(DatabaseMappingQueryMixin._make_parameter_definition_sq, self)
        self._clear_subqueries("parameter_definition")

    def restore_parameter_value_sq_maker(self):
        """Restores the original function that creates the ``parameter_value_sq`` property."""
        self._make_parameter_value_sq = MethodType(DatabaseMappingQueryMixin._make_parameter_value_sq, self)
        self._clear_subqueries("parameter_value")

    def restore_alternative_sq_maker(self):
        """Restores the original function that creates the ``alternative_sq`` property."""
        self._make_alternative_sq = MethodType(DatabaseMappingQueryMixin._make_alternative_sq, self)
        self._clear_subqueries("alternative")

    def restore_scenario_sq_maker(self):
        """Restores the original function that creates the ``scenario_sq`` property."""
        self._make_scenario_sq = MethodType(DatabaseMappingQueryMixin._make_scenario_sq, self)
        self._clear_subqueries("scenario")

    def restore_scenario_alternative_sq_maker(self):
        """Restores the original function that creates the ``scenario_alternative_sq`` property."""
        self._make_scenario_alternative_sq = MethodType(DatabaseMappingQueryMixin._make_scenario_alternative_sq, self)
        self._clear_subqueries("scenario_alternative")

    def _object_class_id(self):
        return case(
            [(self.wide_entity_class_sq.c.dimension_id_list == None, self.wide_entity_class_sq.c.id)], else_=None
        )

    def _relationship_class_id(self):
        return case(
            [(self.wide_entity_class_sq.c.dimension_id_list != None, self.wide_entity_class_sq.c.id)], else_=None
        )

    def _object_id(self):
        return case([(self.wide_entity_sq.c.element_id_list == None, self.wide_entity_sq.c.id)], else_=None)

    def _relationship_id(self):
        return case([(self.wide_entity_sq.c.element_id_list != None, self.wide_entity_sq.c.id)], else_=None)

    def _object_class_name(self):
        return case(
            [(self.wide_entity_class_sq.c.dimension_id_list == None, self.wide_entity_class_sq.c.name)], else_=None
        )

    def _relationship_class_name(self):
        return case(
            [(self.wide_entity_class_sq.c.dimension_id_list != None, self.wide_entity_class_sq.c.name)], else_=None
        )

    def _object_class_id_list(self):
        return case(
            [
                (
                    self.wide_entity_class_sq.c.dimension_id_list != None,
                    self.wide_relationship_class_sq.c.object_class_id_list,
                )
            ],
            else_=None,
        )

    def _object_class_name_list(self):
        return case(
            [
                (
                    self.wide_entity_class_sq.c.dimension_id_list != None,
                    self.wide_relationship_class_sq.c.object_class_name_list,
                )
            ],
            else_=None,
        )

    def _object_name(self):
        return case([(self.wide_entity_sq.c.element_id_list == None, self.wide_entity_sq.c.name)], else_=None)

    def _object_id_list(self):
        return case(
            [(self.wide_entity_sq.c.element_id_list != None, self.wide_relationship_sq.c.object_id_list)], else_=None
        )

    def _object_name_list(self):
        return case(
            [(self.wide_entity_sq.c.element_id_list != None, self.wide_relationship_sq.c.object_name_list)], else_=None
        )
