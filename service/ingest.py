import requests
import os
from urllib.parse import urlparse

from trdpipe.structify_publish.pipe import BasePipe
from trdpipe.structify_publish.const import STAGE_RAW
from datetime import date

class CompanyJobfeedIngester(BasePipe):

    datasource = "customer.<TO_BE_OVERWRITTEN>"

    url_map = {
        "tuevsued":{
            "url":"https://www.tuvsud.com/jobportal/sitemap/jobFeed_normal.XML",
            "filename":"jobFeed_normal.xml"
        },
        "aldisued":{
            "url" : "https://jobs.aldi-sued.de/feed/196001",
            "filename":"196001.xml"
        },
        "accenture":{
            "url" : "https://www.accenture.com/api/sitecore/jobfeedresult/displayAllLocations?p=Deutschland%2C%C3%96sterreich%2CSchweiz&p2=&p3=German&ec=&l=&s=postedDate&sb=1&t=1&sp=&mc=&pi=0&didit=&days=10&jk=&atom=0&tt=&tt2=&ou1=&ou2=&ou3=&ou4=&ou5=&ou6=&ba=&spe=&jl=&jn=",
            "filename":"displayAllLocations.xml"
        },
        "zeb":{
            "url" : "https://career2.successfactors.eu/career?company=zebrolfess&career_ns=job_listing_summary&resultType=XML",
            "filename":"zebrolfess-external-job-listing.xml",
            "additional_headers": {'Accept-Language': 'de'}
        }
    }

    def __init__(self,
                 config,
                 company:str,
                 subsrc=date.today().strftime("%Y%m%d"),
                 params=None):

        super().__init__(
            config=config,
            subsrc=subsrc,
            params=params)
        
        self.datasource = f"customer.{company}_jobfeed"
        self.source = self.url_map.get(company)
        if self.source is None:
            raise ValueError(f"could not determine source for company {company}")

    def ingest(self, tmpPath="/tmp"):
        headers = {'Accept-Encoding':'utf-8'}
        if 'additional_headers' in self.source.keys():
            headers.update(self.source["additional_headers"])
        response = requests.get(self.source["url"], headers=headers)
        response.encoding = response.apparent_encoding
        
        localFile = f"{tmpPath}/{self.source['filename']}"

        with open(localFile, 'w', encoding="utf-8") as f:
            f.write(response.text)

        self._pushFile(
            localFile,
            timestamp=date.today().strftime("%Y%m%d"),
            create_latest=False,
            create_timebased=False,
            stage=STAGE_RAW
        )

    def extract_filename(self, url):
        a = urlparse(url)
        return os.path.basename(a.path)