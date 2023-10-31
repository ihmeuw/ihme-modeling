from pathlib import Path

import pytest

from covid_model_seiir_pipeline.lib.io.data_roots import (
    DataRoot,
)
from covid_model_seiir_pipeline.lib.io.keys import (
    DatasetType,
    MetadataType,
    LEAF_TEMPLATES,
    PREFIX_TEMPLATES,
)
from covid_model_seiir_pipeline.lib.io.marshall import (
    DATA_STRATEGIES,
    METADATA_STRATEGIES,
)


@pytest.fixture(params=list(DATA_STRATEGIES))
def data_strategy(request):
    return request.param


@pytest.fixture(params=list(METADATA_STRATEGIES))
def metadata_strategy(request):
    return request.param


@pytest.fixture(params=['pen_and_ink', 'telegraph', 'interpretive_dance'])
def bad_strategy(request):
    return request.param


def test_DataRoot_init(tmpdir, data_strategy, metadata_strategy, bad_strategy):
    DataRoot(tmpdir, data_strategy, metadata_strategy)

    with pytest.raises(ValueError, match='Invalid data format'):
        DataRoot(tmpdir, bad_strategy, metadata_strategy)

    with pytest.raises(ValueError, match='Invalid metadata format'):
        DataRoot(tmpdir, data_strategy, bad_strategy)


def test_empty_DataRoot_info(tmpdir):

    class MyDataRoot(DataRoot):
        pass

    mdr = MyDataRoot(tmpdir)
    assert mdr.metadata_types == []
    assert mdr.dataset_types == []


def test_DataRoot_info(tmpdir):

    class MyDataRoot(DataRoot):
        sights = MetadataType('sights')
        sounds = MetadataType('sounds')
        smells = MetadataType('smells')

        antarctica = DatasetType('antarctica')
        europe = DatasetType('europe', LEAF_TEMPLATES.MEASURE_TEMPLATE)
        southeast_asia = DatasetType('southeast_asia', LEAF_TEMPLATES.MEASURE_TEMPLATE)
        patagonia = DatasetType('patagonia', LEAF_TEMPLATES.MEASURE_TEMPLATE, PREFIX_TEMPLATES.SCENARIO_TEMPLATE)

    mdr = MyDataRoot(tmpdir)
    assert set(mdr.metadata_types) == {'sights', 'sounds', 'smells'}
    assert set(mdr.dataset_types) == {'antarctica', 'europe', 'southeast_asia', 'patagonia'}

    with pytest.raises(KeyError):
        mdr.terminal_paths()

    root = Path(tmpdir)
    expected = {root, root / 'europe', root / 'southeast_asia',
                root / 'summer' / 'patagonia', root / 'winter' / 'patagonia'}
    assert set(mdr.terminal_paths(scenario=['summer', 'winter'])) == expected
