import requests
import json
import os

def fetch_country_data():
    """
    Fetches data for specified countries and saves it to JSON files.
    """
    countries = ['india', 'us', 'uk', 'china', 'russia']
    base_url = "https://restcountries.com/v3.1/name/"
    output_dir = "country_data"

    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for country in countries:
        try:
            # Fetch data from the API
            api_url = f"{base_url}{country}"
            response = requests.get(api_url)
            response.raise_for_status()  # Raise an error for bad responses (4xx or 5xx)

            # Save data to a JSON file
            file_path = os.path.join(output_dir, f"{country}.json")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(response.json(), f, ensure_ascii=False, indent=4)
            
            print(f"✅ Successfully fetched and saved data for {country}.")
        
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching data for {country}: {e}")

if __name__ == "__main__":
    fetch_country_data()