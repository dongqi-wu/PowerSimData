def get_profile_version(_fs, grid_model, kind):
    """Returns available raw profile from the given filesystem

    :param fs.base.FS _fs: filesystem instance
    :param str grid_model: grid model.
    :param str kind: *'demand'*, *'hydro'*, *'solar'*, *'wind'*,
        *'demand_flexibility_up'*, *'demand_flexibility_dn'*,
        *'demand_flexibility_cost_up'*, or *'demand_flexibility_cost_dn'*.
    :return: (*list*) -- available profile version.
    """
    _fs = _fs.makedirs(f"raw/{grid_model}", recreate=True)
    matching = [f for f in _fs.listdir(".") if kind in f]

    # Don't include demand flexibility profiles as possible demand profiles
    if kind == "demand":
        matching = [p for p in matching if "demand_flexibility" not in p]
    return [f.lstrip(f"{kind}_").rstrip(".csv") for f in matching]


class ProfileHelper:
    @staticmethod
    def get_file_path(scenario_info, field_name):
        """Get the pyfilesystem path to the profile

        :param dict scenario_info: metadata for a scenario.
        :param str field_name: the kind of profile.
        :return: (*str*) -- the filepath
        """
        if "demand_flexibility" in field_name:
            version = scenario_info[field_name]
        else:
            version = scenario_info["base_" + field_name]
        file_name = field_name + "_" + version + ".csv"
        grid_model = scenario_info["grid_model"]
        return "/".join(["raw", grid_model, file_name])
