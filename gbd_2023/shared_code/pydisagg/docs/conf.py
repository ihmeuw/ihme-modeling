# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath("."))
import datetime

import tomllib

with open("../pyproject.toml", "rb") as f:
    about = tomllib.load(f)["project"]


# -- Project information -----------------------------------------------------

project = about["name"]
author = ", ".join([info["name"] for info in about["authors"]])
copyright = f"2019-{datetime.datetime.today().year}, {author}"


# The full version, including alpha/beta/rc tags
version = about["version"]


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named "sphinx.ext.*") or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.extlinks",
    "sphinx.ext.intersphinx",
    "sphinx.ext.mathjax",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
]
autodoc_typehints = "description"
autodoc_member_order = "bysource"
autodoc_type_aliases = {
    "ArrayLike": "ArrayLike",
    "NDArray": "NDArray",
    "DataFrame": "DataFrame",
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "furo"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_css_files = ["css/custom.css"]
html_title = f"{project} {version}"
html_theme_options = {
    "sidebar_hide_name": False,
    "light_logo": "logo/logo-light.png",
    "dark_logo": "logo/logo-dark.png",
    "light_css_variables": {
        "color-brand-primary": "#008080",
        "color-brand-content": "#008080",
        "color-problematic": "#BF5844",
        "color-background-secondary": "#F8F8F8",
        "color-admonition-title--note": "#008080",
        "color-admonition-title-background--note": "#00808033",
    },
    "dark_css_variables": {
        "color-brand-primary": "#6FD8D1",
        "color-brand-content": "#6FD8D1",
        "color-problematic": "#FA9F50",
        "color-background-secondary": "#202020",
        "color-admonition-title--note": "#6FD8D1",
        "color-admonition-title-background--note": "#6FD8D133",
    },
}
# get versions
with open("meta.toml", "rb") as f:
    versions = tomllib.load(f)["versions"]
html_context = {
    "versions": versions,
}
