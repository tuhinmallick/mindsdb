import os
import requests
import tempfile
import shutil
from pathlib import Path
from zipfile import ZipFile

from mindsdb.utilities.config import Config
from mindsdb.utilities.log import get_log


def download_gui(destignation, version):
    if isinstance(destignation, str):
        destignation = Path(destignation)
    logger = get_log('http')
    dist_zip_path = str(destignation.joinpath('dist.zip'))
    bucket = "https://mindsdb-web-builds.s3.amazonaws.com/"

    resources = [{'url': f'{bucket}dist-V{version}.zip', 'path': dist_zip_path}]

    def get_resources(resource):
        response = requests.get(resource['url'])
        if response.status_code != requests.status_codes.codes.ok:
            raise Exception(f"Error {response.status_code} GET {resource['url']}")
        open(resource['path'], 'wb').write(response.content)

    try:
        for r in resources:
            get_resources(r)
    except Exception as e:
        logger.error(f'Error during downloading files from s3: {e}')
        return False

    static_folder = destignation
    static_folder.mkdir(mode=0o777, exist_ok=True, parents=True)
    ZipFile(dist_zip_path).extractall(static_folder)

    if static_folder.joinpath('dist').is_dir():
        shutil.move(str(destignation.joinpath('dist').joinpath('index.html')), static_folder)
        shutil.move(str(destignation.joinpath('dist').joinpath('assets')), static_folder)
        shutil.rmtree(destignation.joinpath('dist'))

    os.remove(dist_zip_path)

    version_txt_path = destignation.joinpath('version.txt')
    with open(version_txt_path, 'wt') as f:
        f.write(version)

    return True


def update_static(gui_version_lv):
    ''' Update Scout files basing on compatible-config.json content.
        Files will be downloaded and updated if new version of GUI > current.
        Current GUI version stored in static/version.txt.
    '''
    config = Config()
    logger = get_log('http')
    static_path = Path(config['paths']['static'])

    logger.info(f'New version of GUI available ({gui_version_lv.vstring}). Downloading...')

    temp_dir = tempfile.mkdtemp(prefix='mindsdb_gui_files_')
    success = download_gui(temp_dir, gui_version_lv.vstring)
    if success is False:
        shutil.rmtree(temp_dir)
        return False

    temp_dir_for_rm = tempfile.mkdtemp(prefix='mindsdb_gui_files_')
    shutil.rmtree(temp_dir_for_rm)
    shutil.copytree(str(static_path), temp_dir_for_rm)
    shutil.rmtree(str(static_path))
    shutil.copytree(temp_dir, str(static_path))
    shutil.rmtree(temp_dir_for_rm)

    logger.info(f'GUI version updated to {gui_version_lv.vstring}')
    return True
