import requests
import json
import gzip
import logging
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)

class GrabDataExtractor:
    def __init__(self):
        self.url = "https://portal.grab.com/foodweb/v2/search"
        self.headers = {
          'accept': 'application/json, text/plain, */*',
          'accept-language': 'en',
          'content-type': 'application/json;charset=UTF-8',
          'origin': 'https://food.grab.com',
          'priority': 'u=1, i',
          'referer': 'https://food.grab.com/',
          'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"Windows"',
          'sec-fetch-dest': 'empty',
          'sec-fetch-mode': 'cors',
          'sec-fetch-site': 'same-site',
          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
          'x-country-code': 'SG',
          'x-gfc-country': 'SG',
          'x-grab-web-app-version': '~OYFo45UHckSfgUM6xfyV',
        #   'x-hydra-jwt': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnYWEiLCJhdWQiOiJnZnciLCJuYW1lIjoiZ3JhYnRheGkiLCJpYXQiOjE3MTU0MjEwNDIsImV4cCI6MTcxNTQyMTY0MiwibmJmIjoxNzE1NDIxMDQyLCJ2ZXIiOiIxLjE5LjAuMjQiLCJicklEIjoiMGRkZjRmM2Y4OGY4NGM2NDEzM2EyMmVlOWI5NDVjNmQ5ZmF5NnIiLCJzdXMiOmZhbHNlLCJicklEdjIiOiI5MDNhNDFiMWMxMjg4ODk5ZTY0MDIyMjlkZTkwOWMzNjI2OXk2ciIsImJyVUlEIjoiZmM4ZmRmYjAtMjM4MS00MDM5LWFkNmEtYzg2OTE4OTc5ZTM4In0.oZdviAe1DhHWXaIy8Mq1zL78weaS5iH-VhEslOH0zlsIXR-0QcMeYmH4NF4NaEKi10q95h71sQcTclJ6S7gks5yaFYdsULUb6nM-hF52xxApNjBZ-7a81mNhMwTMAg-teubwK45omoiSk-LDt9vBt0fm8lfvLFz-aZy5tNzZkJTlTQ3u4FFaWz34mYWHTOgCOfWa0O3x0STg48Rn_XvN63i6XBRS18Db423sOta5sB-MivoNpqugt1xCwsoXf25G6Yr2eVfIpfMxdxERQQUfr_5VXvSdQRVopRZ0q_aaIsxmKHgzRscmM-pAWOQYjhl_BjdLE-76JAFbLzDmDStyZQ'
        }

    def fetch_data(self, offset):
        data = []
        try:
            logging.info("Fetching data for GrabFood website")
            payload = json.dumps({"latlng": "1.396364,103.747462", "keyword": "", "offset": offset, "pageSize": 32, "countryCode": "SG"})  #coordinates hardcoded for PT Singapore - Choa Chu Kang North 6, Singapore, 689577
            response = requests.post(self.url, headers=self.headers, data=payload)
            res_json = response.json()

            if not res_json['searchResult'].get('searchMerchants'):
                return data

            for restaurant in res_json['searchResult']['searchMerchants']:
                try:
                    restaurant_name = restaurant['merchantBrief']['displayInfo']['primaryText']
                    restaurant_cuisine = ", ".join(restaurant['merchantBrief']['cuisine'])
                    restaurant_rating = restaurant['merchantBrief'].get('rating')
                    restaurant_estim_delivery_time = str(restaurant['estimatedDeliveryTime']) + " mins"
                    restaurant_distance = str(restaurant['merchantBrief']['distanceInKm']) + " km"
                    restaurant_promos = restaurant['merchantBrief']['promo'].get('description')
                    restaurant_notice = restaurant.get('notice')
                    restaurant_photo = restaurant['merchantBrief'].get('photoHref')
                    restaurant_promo_available = restaurant['merchantBrief']['promo'].get('hasPromo', False)
                    restaurant_id = restaurant['id']
                    restaurant_lat_long = (restaurant['latlng']['latitude'], restaurant['latlng']['longitude'])
                    restaurant_estim_delivery_fee = restaurant.get('estimatedDeliveryFee',{}).get('priceDisplay')

                    data.append({
                        'Restaurant Name': restaurant_name,
                        'Restaurant Cuisine': restaurant_cuisine,
                        'Restaurant Rating': restaurant_rating,
                        'Estimated time of Delivery': restaurant_estim_delivery_time,
                        'Restaurant Distance from Delivery Location': restaurant_distance,
                        'Promotional Offers': restaurant_promos,
                        'Restaurant Notice': restaurant_notice,
                        'Image Link': restaurant_photo,
                        'Is promo available': restaurant_promo_available,
                        'Restaurant ID': restaurant_id,
                        'Restaurant latitude and longitude': restaurant_lat_long,
                        'Estimated Delivery Fee': restaurant_estim_delivery_fee
                    })
                except Exception as e:
                    logging.error(f"Error in fetching details of a restaurant: {str(e)}")
                    logging.info("Skipping this restaurant...")
        except Exception as e:
            logging.error(f"Error fetching data: {str(e)}")
        return data

    def save_data_to_gzip_ndjson(self, data, file_path):
        try:
            logging.info(f"Saving GrabFood extracted data to: {file_path}")
            with gzip.open(file_path, 'wt', encoding='utf-8') as f:
                for item in data:
                    json.dump(item, f)
                    f.write('\n')
        except Exception as e:
            logging.error(f"Error saving data to file: {str(e)}")

def main():
    extractor = GrabDataExtractor()
    num_pages = 10  # Number of pages to fetch concurrently
    data = []

    with ThreadPoolExecutor() as executor:
        # Fetch data concurrently   #each page of paginated response being fetched by a separate thread
        threads = [executor.submit(extractor.fetch_data, 32*offset) for offset in range(num_pages)]
        for thread in threads:
            data.extend(thread.result())
    extractor.save_data_to_gzip_ndjson(data, 'data/extracted_data_GrabFood.gz')