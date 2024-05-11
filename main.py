from scrapers.grabfood_scrapper import GrabDataExtractor

extractor_map = {'Grab': GrabDataExtractor}

website_to_be_scrapped = 'Grab'  #can change here as per different websites

extractor = extractor_map[website_to_be_scrapped]()
data = extractor.fetch_data()
extractor.save_data_to_gzip_ndjson(data, 'extracted_data.gz')