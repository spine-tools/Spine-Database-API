#############################################################################
# Copyright (C) 2017 - 2018 VTT Technical Research Centre of Finland
#
# This file is part of Spine Database API.
#
# Spine Spine Database API is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

"""
A class to perform SELECT queries over a Spine database ORM.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

import warnings
from sqlalchemy import false, distinct, func, or_

# TODO: Consider returning lists of dict (with _asdict()) rather than queries,
# to better support platforms that cannot handle queries efficiently (such as Julia)
# TODO: check errors


class _DatabaseMappingQuery:
    """A class to perform SELECT queries over a Spine database ORM.
    """

    def __init__(self):
        """Initialize class."""
        super().__init__()

    def create_special_subqueries(self):
        """Create special helper subqueries to act as 'views', e.g., wide_relationship_class...
        """
        self.ext_relationship_class_sq = (
            self.session.query(
                self.relationship_class_sq.c.id.label("id"),
                self.relationship_class_sq.c.object_class_id.label("object_class_id"),
                self.object_class_sq.c.name.label("object_class_name"),
                self.relationship_class_sq.c.name.label("name"),
            )
            .filter(
                self.relationship_class_sq.c.object_class_id
                == self.object_class_sq.c.id
            )
            .subquery()
        )
        self.wide_relationship_class_sq = (
            self.session.query(
                self.ext_relationship_class_sq.c.id,
                func.group_concat(
                    self.ext_relationship_class_sq.c.object_class_id
                ).label("object_class_id_list"),
                func.group_concat(
                    self.ext_relationship_class_sq.c.object_class_name
                ).label("object_class_name_list"),
                self.ext_relationship_class_sq.c.name,
            )
            .group_by(self.ext_relationship_class_sq.c.id)
            .subquery()
        )

    def single_object_class(self, id=None, name=None):
        """Return a single object class given the id or name."""
        qry = self.object_class_list()
        if id:
            return qry.filter(self.object_class_sq.c.id == id)
        if name:
            return qry.filter(self.object_class_sq.c.name == name)
        return self.empty_list()

    def single_object(self, id=None, name=None):
        """Return a single object given the id or name."""
        qry = self.object_list()
        if id:
            return qry.filter(self.object_sq.c.id == id)
        if name:
            return qry.filter(self.object_sq.c.name == name)
        return self.empty_list()

    def single_wide_relationship_class(self, id=None, name=None):
        """Return a single relationship class in wide format given the id or name."""
        qry = self.session.query(self.wide_relationship_class_sq)
        if id:
            return qry.filter(self.wide_relationship_class_sq.c.id == id)
        if name:
            return qry.filter(self.wide_relationship_class_sq.c.name == name)
        return self.empty_list()

    def single_wide_relationship(
        self,
        id=None,
        name=None,
        class_id=None,
        object_id_list=None,
        object_name_list=None,
    ):
        """Return a single relationship in wide format given the id or name."""
        subqry = self.wide_relationship_list().subquery()
        qry = self.session.query(
            subqry.c.id,
            subqry.c.class_id,
            subqry.c.object_id_list,
            subqry.c.object_name_list,
            subqry.c.name,
        )
        if id:
            return qry.filter(subqry.c.id == id)
        if name:
            return qry.filter(subqry.c.name == name)
        if class_id:
            qry = qry.filter(subqry.c.class_id == class_id)
            if object_id_list:
                return qry.filter(subqry.c.object_id_list == object_id_list)
            if object_name_list:
                return qry.filter(subqry.c.object_name_list == object_name_list)
        return self.empty_list()

    def single_parameter_definition(self, id=None, name=None):
        """Return parameter corresponding to id."""
        qry = self.parameter_definition_list()
        if id:
            return qry.filter(self.parameter_definition_sq.c.id == id)
        if name:
            return qry.filter(self.parameter_definition_sq.c.name == name)
        return self.empty_list()

    def single_object_parameter_definition(self, id):
        """Return object class and the parameter corresponding to id."""
        return self.object_parameter_definition_list().filter(
            self.parameter_definition_sq.c.id == id
        )

    def single_relationship_parameter_definition(self, id):
        """Return relationship class and the parameter corresponding to id."""
        return self.relationship_parameter_definition_list().filter(
            self.parameter_definition_sq.c.id == id
        )

    def single_parameter(self, id=None, name=None):
        warnings.warn(
            "single_parameter is deprecated, use single_parameter_definition instead",
            DeprecationWarning,
        )
        return self.single_parameter_definition(id=id, name=name)

    def single_object_parameter(self, id):
        warnings.warn(
            "single_object_parameter is deprecated, use single_object_parameter_definition instead",
            DeprecationWarning,
        )
        return self.single_object_parameter_definition(id)

    def single_relationship_parameter(self, id):
        warnings.warn(
            "single_relationship_parameter is deprecated, use single_relationship_parameter_definition instead",
            DeprecationWarning,
        )
        return self.single_relationship_parameter_definition(id)

    def single_parameter_value(self, id=None):
        """Return parameter value corresponding to id."""
        if id:
            return self.parameter_value_list().filter(
                self.parameter_value_sq.c.id == id
            )
        return self.empty_list()

    def single_object_parameter_value(
        self, id=None, parameter_id=None, parameter_definition_id=None, object_id=None
    ):
        """Return object and the parameter value, either corresponding to id,
        or to parameter_id and object_id.
        """
        if parameter_definition_id is None and parameter_id is not None:
            parameter_definition_id = parameter_id
            warnings.warn(
                "the parameter_id argument is deprecated, use parameter_definition_id instead",
                DeprecationWarning,
            )
        qry = self.object_parameter_value_list()
        if id:
            return qry.filter(self.parameter_value_sq.c.id == id)
        if parameter_definition_id and object_id:
            return qry.filter(
                self.parameter_value_sq.c.parameter_definition_id
                == parameter_definition_id
            ).filter(self.parameter_value_sq.c.object_id == object_id)
        return self.empty_list()

    def single_relationship_parameter_value(self, id):
        """Return relationship and the parameter value corresponding to id."""
        return self.relationship_parameter_value_list().filter(
            self.parameter_value_sq.c.id == id
        )

    def object_class_list(self, id_list=None, ordered=True):
        """Return object classes ordered by display order."""
        qry = self.session.query(self.object_class_sq)
        if id_list is not None:
            qry = qry.filter(self.object_class_sq.c.id.in_(id_list))
        if ordered:
            qry = qry.order_by(self.object_class_sq.c.display_order)
        return qry

    def object_list(self, id_list=None, class_id=None):
        """Return objects, optionally filtered by class id."""
        qry = self.session.query(self.object_sq)
        if id_list is not None:
            qry = qry.filter(self.object_sq.c.id.in_(id_list))
        if class_id:
            qry = qry.filter(self.object_sq.c.class_id == class_id)
        return qry

    def relationship_class_list(self, id=None, ordered=True):
        """Return all relationship classes optionally filtered by id."""
        qry = self.session.query(self.relationship_class_sq)
        if id:
            qry = qry.filter(self.relationship_class_sq.c.id == id)
        if ordered:
            qry = qry.order_by(
                self.relationship_class_sq.c.id, self.relationship_class_sq.c.dimension
            )
        return qry

    def wide_relationship_class_list(self, id_list=None, object_class_id=None):
        """Return list of relationship classes in wide format involving a given object class."""
        qry = self.session.query(self.wide_relationship_class_sq)
        if id_list is not None:
            qry = qry.filter(self.wide_relationship_class_sq.c.id.in_(id_list))
        if object_class_id:
            qry = qry.filter(
                or_(
                    self.wide_relationship_class_sq.c.object_class_id_list.like(
                        f"%,{object_class_id},%"
                    ),
                    self.wide_relationship_class_sq.c.object_class_id_list.like(
                        f"{object_class_id},%"
                    ),
                    self.wide_relationship_class_sq.c.object_class_id_list.like(
                        f"%,{object_class_id}"
                    ),
                )
            )
        return qry

    def relationship_list(self, id=None):
        """Return relationships, optionally filtered by id."""
        qry = self.session.query(self.relationship_sq).order_by(
            self.relationship_sq.c.id, self.relationship_sq.c.dimension
        )
        if id:
            qry = qry.filter(self.relationship_sq.c.id == id)
        return qry

    def wide_relationship_list(self, id_list=None, class_id=None, object_id=None):
        """Return list of relationships in wide format involving a given relationship class and object."""
        qry = self.session.query(
            self.relationship_sq.c.id.label("id"),
            self.relationship_sq.c.class_id.label("class_id"),
            self.relationship_sq.c.object_id.label("object_id"),
            self.object_sq.c.name.label("object_name"),
            self.relationship_sq.c.name.label("name"),
        ).filter(self.relationship_sq.c.object_id == self.object_sq.c.id)
        if id_list is not None:
            qry = qry.filter(self.relationship_sq.c.id.in_(id_list))
        if class_id:
            qry = qry.filter(
                self.relationship_sq.c.id.in_(
                    self.session.query(self.relationship_sq.c.id)
                    .filter(self.relationship_sq.c.class_id == class_id)
                    .distinct()
                )
            )
        if object_id:
            qry = qry.filter(
                self.relationship_sq.c.id.in_(
                    self.session.query(self.relationship_sq.c.id)
                    .filter(self.relationship_sq.c.object_id == object_id)
                    .distinct()
                )
            )
        subqry = qry.subquery()
        return self.session.query(
            subqry.c.id,
            subqry.c.class_id,
            func.group_concat(subqry.c.object_id).label("object_id_list"),
            func.group_concat(subqry.c.object_name).label("object_name_list"),
            subqry.c.name,
        ).group_by(subqry.c.id)

    def parameter_definition_list(
        self, id_list=None, object_class_id=None, relationship_class_id=None
    ):
        """Return parameter definitions."""
        qry = self.session.query(
            self.parameter_definition_sq.c.id.label("id"),
            self.parameter_definition_sq.c.name.label("name"),
            self.parameter_definition_sq.c.relationship_class_id.label(
                "relationship_class_id"
            ),
            self.parameter_definition_sq.c.object_class_id.label("object_class_id"),
            self.parameter_definition_sq.c.parameter_value_list_id.label(
                "parameter_value_list_id"
            ),
            self.parameter_definition_sq.c.default_value.label("default_value"),
        )
        if id_list is not None:
            qry = qry.filter(self.parameter_definition_sq.c.id.in_(id_list))
        if object_class_id:
            qry = qry.filter(
                self.parameter_definition_sq.c.object_class_id == object_class_id
            )
        if relationship_class_id:
            qry = qry.filter(
                self.parameter_definition_sq.c.relationship_class_id
                == relationship_class_id
            )
        return qry

    def object_parameter_definition_list(
        self, object_class_id=None, parameter_id=None, parameter_definition_id=None
    ):
        """Return object classes and their parameters."""
        if parameter_definition_id is None and parameter_id is not None:
            parameter_definition_id = parameter_id
            warnings.warn(
                "the parameter_id argument is deprecated, use parameter_definition_id instead",
                DeprecationWarning,
            )
        wide_parameter_definition_tag_list = (
            self.wide_parameter_definition_tag_list().subquery()
        )
        wide_parameter_value_list_list = (
            self.wide_parameter_value_list_list().subquery()
        )
        qry = (
            self.session.query(
                self.parameter_definition_sq.c.id.label("id"),
                self.object_class_sq.c.id.label("object_class_id"),
                self.object_class_sq.c.name.label("object_class_name"),
                self.parameter_definition_sq.c.name.label("parameter_name"),
                self.parameter_definition_sq.c.parameter_value_list_id.label(
                    "value_list_id"
                ),
                wide_parameter_value_list_list.c.name.label("value_list_name"),
                wide_parameter_definition_tag_list.c.parameter_tag_id_list,
                wide_parameter_definition_tag_list.c.parameter_tag_list,
                self.parameter_definition_sq.c.default_value,
            )
            .filter(
                self.object_class_sq.c.id
                == self.parameter_definition_sq.c.object_class_id
            )
            .outerjoin(
                wide_parameter_definition_tag_list,
                wide_parameter_definition_tag_list.c.parameter_definition_id
                == self.parameter_definition_sq.c.id,
            )
            .outerjoin(
                wide_parameter_value_list_list,
                wide_parameter_value_list_list.c.id
                == self.parameter_definition_sq.c.parameter_value_list_id,
            )
        )
        if object_class_id:
            qry = qry.filter(
                self.parameter_definition_sq.c.object_class_id == object_class_id
            )
        if parameter_id:
            qry = qry.filter(self.parameter_definition_sq.c.id == parameter_id)
        return qry

    def relationship_parameter_definition_list(
        self,
        relationship_class_id=None,
        parameter_id=None,
        parameter_definition_id=None,
    ):
        """Return relationship classes and their parameters."""
        if parameter_definition_id is None and parameter_id is not None:
            parameter_definition_id = parameter_id
            warnings.warn(
                "the parameter_id argument is deprecated, use parameter_definition_id instead",
                DeprecationWarning,
            )
        wide_relationship_class_list = self.wide_relationship_class_list().subquery()
        wide_parameter_definition_tag_list = (
            self.wide_parameter_definition_tag_list().subquery()
        )
        wide_parameter_value_list_list = (
            self.wide_parameter_value_list_list().subquery()
        )
        qry = (
            self.session.query(
                self.parameter_definition_sq.c.id.label("id"),
                wide_relationship_class_list.c.id.label("relationship_class_id"),
                wide_relationship_class_list.c.name.label("relationship_class_name"),
                wide_relationship_class_list.c.object_class_id_list,
                wide_relationship_class_list.c.object_class_name_list,
                self.parameter_definition_sq.c.name.label("parameter_name"),
                self.parameter_definition_sq.c.parameter_value_list_id.label(
                    "value_list_id"
                ),
                wide_parameter_value_list_list.c.name.label("value_list_name"),
                wide_parameter_definition_tag_list.c.parameter_tag_id_list,
                wide_parameter_definition_tag_list.c.parameter_tag_list,
                self.parameter_definition_sq.c.default_value,
            )
            .filter(
                self.parameter_definition_sq.c.relationship_class_id
                == wide_relationship_class_list.c.id
            )
            .outerjoin(
                wide_parameter_definition_tag_list,
                wide_parameter_definition_tag_list.c.parameter_definition_id
                == self.parameter_definition_sq.c.id,
            )
            .outerjoin(
                wide_parameter_value_list_list,
                wide_parameter_value_list_list.c.id
                == self.parameter_definition_sq.c.parameter_value_list_id,
            )
        )
        if relationship_class_id:
            qry = qry.filter(
                self.parameter_definition_sq.c.relationship_class_id
                == relationship_class_id
            )
        if parameter_id:
            qry = qry.filter(self.parameter_definition_sq.c.id == parameter_id)
        return qry

    def parameter_list(
        self, id_list=None, object_class_id=None, relationship_class_id=None
    ):
        warnings.warn(
            "parameter_list is deprecated, use parameter_definition_list instead",
            DeprecationWarning,
        )
        return self.parameter_definition_list(
            id_list=id_list,
            object_class_id=object_class_id,
            relationship_class_id=relationship_class_id,
        )

    def object_parameter_list(self, object_class_id=None, parameter_id=None):
        warnings.warn(
            "object_parameter_list is deprecated, use object_parameter_definition_list instead",
            DeprecationWarning,
        )
        return self.object_parameter_definition_list(
            object_class_id=object_class_id, parameter_id=parameter_id
        )

    def relationship_parameter_list(
        self, relationship_class_id=None, parameter_id=None
    ):
        warnings.warn(
            "relationship_parameter_list is deprecated, use relationship_parameter_definition_list instead",
            DeprecationWarning,
        )
        return self.relationship_parameter_definition_list(
            relationship_class_id=relationship_class_id, parameter_id=parameter_id
        )

    def wide_object_parameter_definition_list(
        self, object_class_id_list=None, parameter_definition_id_list=None
    ):
        """Return object classes and their parameter definitions in wide format."""
        qry = self.session.query(
            self.object_class_sq.c.id.label("object_class_id"),
            self.object_class_sq.c.name.label("object_class_name"),
            self.parameter_definition_sq.c.id.label("parameter_definition_id"),
            self.parameter_definition_sq.c.name.label("parameter_name"),
        ).filter(
            self.object_class_sq.c.id == self.parameter_definition_sq.c.object_class_id
        )
        if object_class_id_list is not None:
            qry = qry.filter(self.object_class_sq.c.id.in_(object_class_id_list))
        if parameter_definition_id_list is not None:
            qry = qry.filter(
                self.parameter_definition_sq.c.id.in_(parameter_definition_id_list)
            )
        subqry = qry.subquery()
        return self.session.query(
            subqry.c.object_class_id,
            subqry.c.object_class_name,
            func.group_concat(subqry.c.parameter_definition_id).label(
                "parameter_definition_id_list"
            ),
            func.group_concat(subqry.c.parameter_name).label("parameter_name_list"),
        ).group_by(subqry.c.object_class_id)

    def wide_relationship_parameter_definition_list(
        self, relationship_class_id_list=None, parameter_definition_id_list=None
    ):
        """Return relationship classes and their parameter definitions in wide format."""
        qry = self.session.query(
            self.RelationshipClass.id.label("relationship_class_id"),
            self.RelationshipClass.name.label("relationship_class_name"),
            self.parameter_definition_sq.c.id.label("parameter_definition_id"),
            self.parameter_definition_sq.c.name.label("parameter_name"),
        ).filter(
            self.RelationshipClass.id
            == self.parameter_definition_sq.c.relationship_class_id
        )
        if relationship_class_id_list is not None:
            qry = qry.filter(self.RelationshipClass.id.in_(relationship_class_id_list))
        if parameter_definition_id_list is not None:
            qry = qry.filter(
                self.parameter_definition_sq.c.id.in_(parameter_definition_id_list)
            )
        subqry = qry.subquery()
        return self.session.query(
            subqry.c.relationship_class_id,
            subqry.c.relationship_class_name,
            func.group_concat(subqry.c.parameter_definition_id).label(
                "parameter_definition_id_list"
            ),
            func.group_concat(subqry.c.parameter_name).label("parameter_name_list"),
        ).group_by(subqry.c.relationship_class_id)

    def parameter_value_list(self, id_list=None, object_id=None, relationship_id=None):
        """Return parameter values."""
        qry = self.session.query(
            self.parameter_value_sq.c.id,
            self.parameter_value_sq.c.parameter_definition_id,
            self.parameter_value_sq.c.object_id,
            self.parameter_value_sq.c.relationship_id,
            self.parameter_value_sq.c.value,
        )
        if id_list is not None:
            qry = qry.filter(self.parameter_value_sq.c.id.in_(id_list))
        if object_id:
            qry = qry.filter(self.parameter_value_sq.c.object_id == object_id)
        if relationship_id:
            qry = qry.filter(
                self.parameter_value_sq.c.relationship_id == relationship_id
            )
        return qry

    # TODO: This should be updated so it also brings value_list and tag_list
    def object_parameter_value_list(self, parameter_name=None):
        """Return objects and their parameter values."""
        object_list = self.object_list().subquery()
        qry = (
            self.session.query(
                self.parameter_value_sq.c.id.label("id"),
                self.object_class_sq.c.id.label("object_class_id"),
                self.object_class_sq.c.name.label("object_class_name"),
                object_list.c.id.label("object_id"),
                object_list.c.name.label("object_name"),
                self.parameter_definition_sq.c.id.label("parameter_id"),
                self.parameter_definition_sq.c.name.label("parameter_name"),
                self.parameter_value_sq.c.value,
            )
            .filter(
                self.parameter_definition_sq.c.id
                == self.parameter_value_sq.c.parameter_definition_id
            )
            .filter(self.parameter_value_sq.c.object_id == object_list.c.id)
            .filter(
                self.parameter_definition_sq.c.object_class_id
                == self.object_class_sq.c.id
            )
        )
        if parameter_name:
            qry = qry.filter(self.parameter_definition_sq.c.name == parameter_name)
        return qry

    # TODO: Should this also bring `value_list` and `tag_list`?
    def relationship_parameter_value_list(self, parameter_name=None):
        """Return relationships and their parameter values."""
        wide_relationship_class_list = self.wide_relationship_class_list().subquery()
        wide_relationship_list = self.wide_relationship_list().subquery()
        qry = (
            self.session.query(
                self.parameter_value_sq.c.id.label("id"),
                wide_relationship_class_list.c.id.label("relationship_class_id"),
                wide_relationship_class_list.c.name.label("relationship_class_name"),
                wide_relationship_class_list.c.object_class_id_list,
                wide_relationship_class_list.c.object_class_name_list,
                wide_relationship_list.c.id.label("relationship_id"),
                wide_relationship_list.c.object_id_list,
                wide_relationship_list.c.object_name_list,
                self.parameter_definition_sq.c.id.label("parameter_id"),
                self.parameter_definition_sq.c.name.label("parameter_name"),
                self.parameter_value_sq.c.value,
            )
            .filter(
                self.parameter_definition_sq.c.id
                == self.parameter_value_sq.c.parameter_definition_id
            )
            .filter(
                self.parameter_value_sq.c.relationship_id == wide_relationship_list.c.id
            )
            .filter(
                self.parameter_definition_sq.c.relationship_class_id
                == wide_relationship_class_list.c.id
            )
        )
        if parameter_name:
            qry = qry.filter(self.parameter_definition_sq.c.name == parameter_name)
        return qry

    # TODO: Is this needed?
    def all_object_parameter_value_list(self, parameter_id=None):
        """Return all object parameter values, even those that don't have a value."""
        qry = (
            self.session.query(
                self.parameter_definition_sq.c.id.label("parameter_id"),
                self.object_sq.c.name.label("object_name"),
                self.parameter_value_sq.c.id.label("parameter_value_id"),
                self.parameter_definition_sq.c.name.label("parameter_name"),
                self.parameter_value_sq.c.value,
            )
            .filter(self.parameter_value_sq.c.object_id == self.object_sq.c.id)
            .outerjoin(self.ParameterValue)
            .filter(
                self.parameter_definition_sq.c.id
                == self.parameter_value_sq.c.parameter_definition_id
            )
        )
        if parameter_id:
            qry = qry.filter(self.parameter_definition_sq.c.id == parameter_id)
        return qry

    # NOTE: maybe these unvalued... are obsolete
    def unvalued_object_parameter_list(self, object_id):
        """Return parameters that do not have a value for given object."""
        object_ = self.single_object(id=object_id).one_or_none()
        if not object_:
            return self.empty_list()
        valued_parameter_ids = self.session.query(
            self.parameter_value_sq.c.parameter_definition_id
        ).filter(self.parameter_value_sq.c.object_id == object_id)
        return self.parameter_definition_list(object_class_id=object_.class_id).filter(
            ~self.parameter_definition_sq.c.id.in_(valued_parameter_ids)
        )

    def unvalued_object_list(self, parameter_id):
        """Return objects for which given parameter does not have a value."""
        parameter = self.single_parameter(parameter_id).one_or_none()
        if not parameter:
            return self.empty_list()
        valued_object_ids = self.session.query(
            self.parameter_value_sq.c.object_id
        ).filter(self.parameter_value_sq.c.parameter_definition_id == parameter_id)
        return (
            self.object_list()
            .filter(self.object_sq.c.class_id == parameter.object_class_id)
            .filter(~self.object_sq.c.id.in_(valued_object_ids))
        )

    def unvalued_relationship_parameter_list(self, relationship_id):
        """Return parameters that do not have a value for given relationship."""
        relationship = self.single_wide_relationship(id=relationship_id).one_or_none()
        if not relationship:
            return self.empty_list()
        valued_parameter_ids = self.session.query(
            self.parameter_value_sq.c.parameter_definition_id
        ).filter(self.parameter_value_sq.relationship_id == relationship_id)
        return self.parameter_definition_list(
            relationship_class_id=relationship.class_id
        ).filter(~self.parameter_definition_sq.c.id.in_(valued_parameter_ids))

    def unvalued_relationship_list(self, parameter_id):
        """Return relationships for which given parameter does not have a value."""
        parameter = self.single_parameter(parameter_id).one_or_none()
        if not parameter:
            return self.empty_list()
        valued_relationship_ids = self.session.query(
            self.parameter_value_sq.c.relationship_id
        ).filter(self.parameter_value_sq.c.parameter_definition_id == parameter_id)
        return (
            self.wide_relationship_list()
            .filter(self.relationship_sq.c.class_id == parameter.relationship_class_id)
            .filter(~self.relationship_sq.c.id.in_(valued_relationship_ids))
        )

    def parameter_tag_list(self, id_list=None, tag_list=None):
        """Return list of parameter tags."""
        qry = self.session.query(
            self.parameter_tag_sq.c.id.label("id"),
            self.parameter_tag_sq.c.tag.label("tag"),
            self.parameter_tag_sq.c.description.label("description"),
        )
        if id_list is not None:
            qry = qry.filter(self.parameter_tag_sq.c.id.in_(id_list))
        if tag_list is not None:
            qry = qry.filter(self.parameter_tag_sq.c.tag.in_(tag_list))
        return qry

    def parameter_definition_tag_list(self, id_list=None):
        """Return list of parameter definition tags."""
        qry = self.session.query(
            self.parameter_definition_tag_sq.c.id.label("id"),
            self.parameter_definition_tag_sq.c.parameter_definition_id.label(
                "parameter_definition_id"
            ),
            self.parameter_definition_tag_sq.c.parameter_tag_id.label(
                "parameter_tag_id"
            ),
        )
        if id_list is not None:
            qry = qry.filter(self.parameter_definition_tag_sq.c.id.in_(id_list))
        return qry

    def wide_parameter_definition_tag_list(self, parameter_definition_id=None):
        """Return list of parameter tags in wide format for a given parameter definition."""
        qry = self.session.query(
            self.parameter_definition_tag_sq.c.parameter_definition_id.label(
                "parameter_definition_id"
            ),
            self.parameter_definition_tag_sq.c.parameter_tag_id.label(
                "parameter_tag_id"
            ),
            self.parameter_tag_sq.c.tag.label("parameter_tag"),
        ).filter(
            self.parameter_definition_tag_sq.c.parameter_tag_id
            == self.parameter_tag_sq.c.id
        )
        if parameter_definition_id:
            qry = qry.filter(
                self.parameter_definition_tag_sq.c.parameter_definition_id
                == parameter_definition_id
            )
        subqry = qry.subquery()
        return self.session.query(
            subqry.c.parameter_definition_id,
            func.group_concat(subqry.c.parameter_tag_id).label("parameter_tag_id_list"),
            func.group_concat(subqry.c.parameter_tag).label("parameter_tag_list"),
        ).group_by(subqry.c.parameter_definition_id)

    def wide_parameter_tag_definition_list(self, parameter_tag_id=None):
        """Return list of parameter tags (including the NULL tag) and their definitions in wide format.
        """
        parameter_definition_tag_list = self.parameter_definition_tag_list().subquery()
        qry = self.session.query(
            self.parameter_definition_sq.c.id.label("parameter_definition_id"),
            parameter_definition_tag_list.c.parameter_tag_id.label("parameter_tag_id"),
        ).outerjoin(
            parameter_definition_tag_list,
            self.parameter_definition_sq.c.id
            == parameter_definition_tag_list.c.parameter_definition_id,
        )
        if parameter_tag_id:
            qry = qry.filter(
                parameter_definition_tag_list.c.parameter_tag_id == parameter_tag_id
            )
        subqry = qry.subquery()
        return self.session.query(
            subqry.c.parameter_tag_id,
            func.group_concat(subqry.c.parameter_definition_id).label(
                "parameter_definition_id_list"
            ),
        ).group_by(subqry.c.parameter_tag_id)

    def parameter_value_list_list(self, id_list=None):
        """Return list of parameter value_lists."""
        qry = self.session.query(
            self.parameter_value_list_sq.c.id.label("id"),
            self.parameter_value_list_sq.c.name.label("name"),
            self.parameter_value_list_sq.c.value_index.label("value_index"),
            self.parameter_value_list_sq.c.value.label("value"),
        )
        if id_list is not None:
            qry = qry.filter(self.parameter_value_list_sq.c.id.in_(id_list))
        return qry

    def wide_parameter_value_list_list(self, id_list=None):
        """Return list of parameter value_lists and their elements in wide format."""
        subqry = self.parameter_value_list_list(id_list=id_list).subquery()
        return (
            self.session.query(
                subqry.c.id,
                subqry.c.name,
                func.group_concat(subqry.c.value).label("value_list"),
            )
            .order_by(subqry.c.id, subqry.c.value_index)
            .group_by(subqry.c.id)
        )

    def object_parameter_fields(self):
        """Return object parameter fields."""
        return [x["name"] for x in self.object_parameter_list().column_descriptions]

    def relationship_parameter_fields(self):
        """Return relationship parameter fields."""
        return [
            x["name"] for x in self.relationship_parameter_list().column_descriptions
        ]

    def object_parameter_value_fields(self):
        """Return object parameter value fields."""
        return [
            x["name"] for x in self.object_parameter_value_list().column_descriptions
        ]

    def relationship_parameter_value_fields(self):
        """Return relationship parameter value fields."""
        return [
            x["name"]
            for x in self.relationship_parameter_value_list().column_descriptions
        ]

    def empty_list(self):
        return self.session.query(false()).filter(false())
