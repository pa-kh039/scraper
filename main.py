from scrapers.grabfood_scraper import main as grabfood_main

extractor_map = {'GrabFood': grabfood_main}  #for storing mapping with driver functions of website scrapers

websites_to_be_scrapped = ['GrabFood']  #can add and remove websites from here

for website in websites_to_be_scrapped:
    main_func = extractor_map[website]
    main_func()