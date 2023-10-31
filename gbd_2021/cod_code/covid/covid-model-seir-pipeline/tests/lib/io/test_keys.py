from pathlib import Path

import pytest

from covid_model_seiir_pipeline.lib.io.keys import (
    DatasetKey,
    MetadataKey,
    DatasetType,
    MetadataType,
    PREFIX_TEMPLATES,
    LEAF_TEMPLATES,
)
from covid_model_seiir_pipeline.lib.io.data_roots import DataRoot


@pytest.fixture(params=list(LEAF_TEMPLATES) + [None])
def leaf_template(request):
    return request.param


@pytest.fixture(params=list(PREFIX_TEMPLATES) + [None])
def prefix_template(request):
    return request.param


def test_DatasetType_init(leaf_template, prefix_template):
    DatasetType('saturn', leaf_template, prefix_template)


def test_DatasetType_init_fail(leaf_template):
    with pytest.raises(ValueError, match='Invalid prefix_template'):
        DatasetType('saturn', leaf_template, '{space_probe}')

    with pytest.raises(ValueError, match='Invalid leaf_template'):
        DatasetType('jupyter', '{moon}')


def test_DatasetType_repr(leaf_template, prefix_template):
    d = DatasetType('saturn', leaf_template)
    expected = ("DatasetType(name=saturn, prefix_template=None, "
                f"leaf_template={leaf_template}, root=None, disk_format=None)")
    assert repr(d) == expected

    d = DatasetType('saturn', leaf_template, prefix_template)
    expected = (f"DatasetType(name=saturn, prefix_template={prefix_template}, "
                f"leaf_template={leaf_template}, root=None, disk_format=None)")
    assert repr(d) == expected


def test_DatasetType_call_fail(leaf_template, prefix_template):
    d = DatasetType('saturn', leaf_template, prefix_template)
    with pytest.raises(AssertionError, match='DatasetType must be bound'):
        d(draw_id=1)

    d = DatasetType('saturn', leaf_template, prefix_template,
                    _root=Path('/outer/space'), _disk_format='csv')
    with pytest.raises(TypeError, match='All dataset key arguments'):
        d('cassini', draw_id=1)


def test_MetadataType_init():
    MetadataType('metadata')


def test_MetadataType_repr():
    m = MetadataType('metadata')
    expected = 'MetadataType(name=metadata, root=None, disk_format=None)'
    assert repr(m) == expected


def test_MetadataType_call_fail():
    m = MetadataType('metadata')
    with pytest.raises(AssertionError, match='MetadataType must be bound'):
        m(draw_id=1)

    m = MetadataType('metadata', Path('/outer/space'), 'yaml')
    with pytest.raises(TypeError, match='Metadata keys have no parameterization.'):
        m(draw_id=1)
    with pytest.raises(TypeError, match='Metadata keys have no parameterization.'):
        m(1)


def test_both_types_binding_and_call():

    class MyDataRoot(DataRoot):
        metadata = MetadataType('metadata')
        saturn = DatasetType('saturn', LEAF_TEMPLATES.DRAW_TEMPLATE, PREFIX_TEMPLATES.SCENARIO_TEMPLATE)

    mdr = MyDataRoot('/outer/space')

    expected = DatasetType('saturn', LEAF_TEMPLATES.DRAW_TEMPLATE, PREFIX_TEMPLATES.SCENARIO_TEMPLATE,
                           _root=mdr._root, _disk_format=mdr._data_format)
    assert mdr.saturn == expected

    expected = MetadataType('metadata', mdr._root, mdr._metadata_format)
    assert mdr.metadata == expected

    expected_key = DatasetKey(mdr._root, mdr._data_format, 'saturn', 'draw_0', 'cassini', None)
    assert mdr.saturn(draw_id=0, scenario='cassini') == expected_key
    assert mdr.saturn(scenario='cassini', draw_id=0) == expected_key
    # We silently ignore extra keys because I haven't thought of a clever thing yet.
    assert mdr.saturn(draw_id=0, scenario='cassini', terrestrial_things='banana') == expected_key

    expected_key = MetadataKey(mdr._root, mdr._metadata_format, 'metadata')
    assert mdr.metadata() == expected_key

    with pytest.raises(TypeError, match='All dataset key arguments'):
        mdr.saturn(0, 'cassini')
    with pytest.raises(KeyError, match='draw_id'):
        mdr.saturn(scenario='cassini')
    with pytest.raises(KeyError, match='scenario'):
        mdr.saturn(draw_id=0)

    with pytest.raises(TypeError, match='Metadata keys have no parameterization.'):
        mdr.metadata(1)
    with pytest.raises(TypeError, match='Metadata keys have no parameterization.'):
        mdr.metadata(draw_id=1)
