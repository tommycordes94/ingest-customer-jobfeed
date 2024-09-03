import pathlib
import pytest
from service.ingest import CompanyJobfeedIngester

def test_ingest_fail():
    with pytest.raises(ValueError):
        base_path = str(pathlib.Path(__file__).parent.joinpath("tmp"))
        b = CompanyJobfeedIngester(
            config={'base_path':base_path},
            company=None)
        
def test_ingest_aldisued():
    base_path = str(pathlib.Path(__file__).parent.joinpath("tmp"))
    b = CompanyJobfeedIngester(
        config={'base_path':base_path},
        company="aldisued")
    b.ingest()

def test_ingest_tuevsued():
    base_path = str(pathlib.Path(__file__).parent.joinpath("tmp"))
    b = CompanyJobfeedIngester(
        config={'base_path':base_path},
        company="tuevsued")
    b.ingest()

def test_ingest_accenture():
    base_path = str(pathlib.Path(__file__).parent.joinpath("tmp"))
    b = CompanyJobfeedIngester(
        config={'base_path':base_path},
        company="accenture")
    b.ingest()

def test_ingest_zeb():
    base_path = str(pathlib.Path(__file__).parent.joinpath("tmp"))
    b = CompanyJobfeedIngester(
        config={'base_path':base_path},
        company="zeb")
    b.ingest()

def test_extract_filename():
    b = CompanyJobfeedIngester(config=None, company="aldisued")
    print(b.extract_filename(b.url_map.get('aldisued')))