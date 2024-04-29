********
Metadata
********

Metadata can be used to provide additional information about the data in Spine data structure.
Every entity and parameter value can have metadata associated with it.

A metadata "item" has a *name* and a *value*, e.g. "authors" and "N.N, M.M et al.".
The same metadata item can be referenced by multiple entities and parameter values.
Entities and values can also refer to multiple items of metadata.

.. note::

   Referring to multiple items of metadata from a huge number of entities or parameter values
   may take up a lot of space in the database.
   Therefore, it might make more sense, for example,
   to list all contributors to the data in a single metadata value than
   having each contributor as a separate name-value pair.

Choosing suitable names and values for metadata is left up to the user.
However, some suggestions and recommendations are presented below.

title
    One sentence description for the data.

sources
    The raw sources of the data.

tools
    Names and versions of tools that were used to process the data.

contributors
    The people or organisations who contributed to the data.

created
    The date this data was created or put together, e.g. in ISO8601 format (YYYY-MM-DDTHH:MM).

description
    A more complete description of the data than the title.

keywords
    Keywords that categorize the data.

homepage
    A URL for the home on the web that is related to the data.

id
    Globally unique id, such as UUID or DOI.

licenses
    Licences that apply to the data.

temporal
    Temporal properties of the data.

spatial
    Spatial properties of the data.

unitOfMeasurement
    Unit of measurement.
