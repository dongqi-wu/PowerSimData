import fs
from tqdm.auto import tqdm

from powersimdata.utility import server_setup


def get_profile_fs():
    return fs.open_fs("azblob://besciences@profiles")


def download(bfs, path, dst_file):
    size = bfs.getinfo(path, namespaces=["details"]).size
    with bfs.openbin(path) as src_file:
        read = src_file.read
        write = dst_file.write
        with tqdm(
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            miniters=1,
            total=size,
        ) as pbar:
            for chunk in iter(lambda: read(4096) or None, None):
                write(chunk)
                pbar.update(len(chunk))


def _get_profile_version(_fs, kind):
    """Returns available raw profiles from the give filesystem
    :param fs.base.FS _fs: filesystem instance
    :param str kind: *'demand'*, *'hydro'*, *'solar'* or *'wind'*.
    :return: (*list*) -- available profile version.
    """
    matching = [f for f in _fs.listdir(".") if kind in f]
    return [f.lstrip(f"{kind}_").rstrip(".csv") for f in matching]


def get_profile_version_cloud(grid_model, kind):
    """Returns available raw profile from blob storage.

    :param str grid_model: grid model.
    :param str kind: *'demand'*, *'hydro'*, *'solar'* or *'wind'*.
    :return: (*list*) -- available profile version.
    """
    bfs = get_profile_fs().opendir(f"raw/{grid_model}")
    return _get_profile_version(bfs, kind)


def get_profile_version_local(grid_model, kind):
    """Returns available raw profile from local file.

    :param str grid_model: grid model.
    :param str kind: *'demand'*, *'hydro'*, *'solar'* or *'wind'*.
    :return: (*list*) -- available profile version.
    """
    profile_dir = fs.path.join(server_setup.LOCAL_DIR, "raw", grid_model)
    lfs = fs.open_fs(profile_dir)
    return _get_profile_version(lfs, kind)


class ProfileHelper:
    @staticmethod
    def get_file_components(scenario_info, field_name):
        """Get the file name and relative path for the given profile and
        scenario.

        :param dict scenario_info: metadata for a scenario.
        :param str field_name: the kind of profile.
        :return: (*tuple*) -- file name and list of path components.
        """
        version = scenario_info["base_" + field_name]
        file_name = field_name + "_" + version + ".csv"
        grid_model = scenario_info["grid_model"]
        return file_name, ("raw", grid_model)

    @staticmethod
    def download_file(file_name, from_dir):
        """Download the profile from blob storage at the given path.

        :param str file_name: profile csv.
        :param tuple from_dir: tuple of path components.
        """
        print(f"--> Downloading {file_name} from blob storage.")
        dirpath = "/".join(from_dir)
        lfs = fs.open_fs(server_setup.LOCAL_DIR).makedirs(dirpath, recreate=True)
        bfs = get_profile_fs().opendir(dirpath)
        with lfs.open(file_name, "wb") as f:
            download(bfs, file_name, f)
