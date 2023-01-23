.. _results-metadata-description:

****************************
Results metadata description
****************************

Required properties
-------------------

``description``
    A plain text (or Markdown formatted) description what the data is about.

``author``
    Author details.

.. code-block::
    :caption: Example

    "author": {
      "title": "Joe Bloggs",
      "email": "joe@bloggs.com",
    }

``spine_toolbox_version``
    Version string of Spine Toolbox application.

``created``
    The date these results were created, in ISO8601 format (YYYY-MM-DDTHH:MM).

Optional properties
-------------------

``tools``
    An array of records with processing tool names and versions.

.. code-block::
    :caption: Example

    "tools": [{"name": "Spine Model",
              "version": "1.0.2",
              "path": "https://github.com/spine-tools/Spine-Model"},
              ...
             ]
