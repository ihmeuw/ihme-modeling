Getting started
===============

.. toctree::
   :hidden:
   
   installation
   quickstart

.. note::

    Mathematical details can be found on the main page's README. This page section will have installation instructions and a quickstart.

Goal of the Package
-------------------

**PyDisagg** is a Python package designed to disaggregate aggregated data into finer buckets based on proportionality assumptions. It uses baseline patterns, such as population data or high-quality estimates, to guide the process and ensure consistency in the disaggregated results.

The package is particularly useful for situations where you need to split a total count into smaller groups while maintaining logical relationships between the groups and the total.

Key Concepts
------------

1. **Aggregated Data**: Start with a total count across multiple groups, such as regions or demographic categories.
2. **Baseline Patterns**: Use reliable baseline data, such as rates or proportions, to guide how the total is split into smaller groups.
3. **Transformations**: PyDisagg supports methods like logarithmic or log-odds transformations to model proportionality and ensure realistic results.

Current Capabilities
--------------------

1. **Rate Multiplicative Model**:
   - A basic model that assumes proportionality in rates across groups using a logarithmic transformation.

2. **Log-Modified Odds Model**:
   - A more advanced model that uses log-odds transformations for better statistical properties and to keep disaggregated rates within realistic bounds.

Why Use PyDisagg?
-----------------

- **Flexible Modeling**: Allows you to disaggregate data using different transformations based on your specific needs.
- **Consistent Results**: Ensures that the disaggregated counts sum up correctly to the original total.
- **Practical Applications**: Ideal for demographic studies, epidemiological analyses, and other scenarios where detailed sub-group estimates are required.

PyDisagg is a powerful tool for creating detailed estimates from aggregated data while maintaining logical and statistical consistency.
