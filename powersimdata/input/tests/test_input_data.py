import pytest

from powersimdata.input.input_data import InputHelper, _check_field


def test_get_file_components():
    s_info = {"id": "123"}
    ct_path = InputHelper.get_file_path(s_info, "ct")
    grid_path = InputHelper.get_file_path(s_info, "grid")

    assert "data/input/123_ct.pkl" == ct_path
    assert "data/input/123_grid.mat" == grid_path


def test_check_field():
    _check_field("demand")
    _check_field("hydro")
    with pytest.raises(ValueError):
        _check_field("foo")
        _check_field("coal")
