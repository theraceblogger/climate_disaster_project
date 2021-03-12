## This script gets data from EMDAT, stores it in file
## '/Users/chuckschultz/work/data/emdat_dump.xlsx' and logs the transaction
## Variables needed for api call:
##   classif: (list)type of disasters - concatenate using +
##   iso: (list)countries of disasters - concatenate using +
##   from: (int)start date - 1900 to 2021
##   to: (int)end date - 1900 to 2021
import requests
import datetime

## classif
natural = ["1"]
geophysical = ["1-4"]
volcanic_activity = ["1-4-3"]
mass_movement = ["1-4-2"]
earthquake_tsunamis = ["1-4-1"]
meteorological = ["1-6"]
extreme_temperature = ["1-6-8"]
cold_wave = ["1-6-8-24"]
heat_wave = ["1-6-8-25"]
severe_winter_conditions = ["1-6-8-26"]
fog = ["1-6-9"]
storm = ["1-6-7"]
hydrological = ["1-5"]
wave_action = ["1-5-6"]
landslide = ["1-5-5"]
flood = ["1-5-4"]
climatological = ["1-2"]
wildfire = ["1-2-12"]
glacial_lake_ourburst = ["1-2-11"]
drought = ["1-2-10"]
biological = ["1-1"]
epidemic = ["1-1-14"]
insect_infestation = ["1-1-15"]
animal_accident = ["1-1-13"]
technological = ["2"]
complex_disasters = ["3"]
famine = ["3"]

## iso
# Asia
southern_asia = ["AFG", "BGD", "BTN", "IND", "IRN", "LKA", "MDV", "NPL", "PAK"]
western_asia = ["ARE", "ARM", "AZE", "BHR", "CYP", "GEO", "IRQ", "ISR", "JOR",\
    "KWT", "LBN", "OMN", "PSE", "QAT", "SAU", "SYR", "TUR", "YEM", "YMD", "YMN"]
southeastern_asia = ["BRN", "IDN", "KHM", "LAO", "MMR", "MYS", "PHL", "SGP",\
    "THA", "VNM", "TLS"]
eastern_asia = ["CHN", "HKG", "JPN", "KOR", "MAC", "MNG", "PRK", "TWN"]
central_asia = ["KAZ", "KGZ", "TJK", "TKM", "UZB"]
asia = southern_asia + western_asia + southeastern_asia + eastern_asia + central_asia
# Africa
middle_africa = ["AGO", "CAF", "CMR", "COG", "GAB", "GNQ", "STP", "TCD", "COD"]
eastern_africa = ["BDI", "COM", "DJI", "ERI", "ETH", "KEN", "MDG", "MOZ", "MUS",\
    "MWI", "REU", "RWA", "SOM", "SYC", "TZA", "UGA", "ZMB", "ZWE", "MYT", "ATF", "IOT"]
western_africa = ["BEN", "BFA", "CIV", "CPV", "GHA", "GIN", "GMB", "GNB", "LBR",\
    "MLI", "MRT", "NER", "NGA", "SEN", "SHN", "SLE", "TGO"]
southern_africa = ["BWA", "LSO", "NAM", "SWZ", "ZAF"]
northern_africa = ["DZA", "EGY", "ESH", "LBY", "MAR", "SDN", "TUN", "SSD"]
africa = middle_africa + eastern_africa + western_africa + southern_africa + northern_africa
# Americas
caribbean = ["AIA", "ANT", "ATG", "BHS", "BRB", "CUB", "CYM", "DMA", "DOM", "GLP",\
    "GRD", "HTI", "JAM", "KNA", "LCA", "MSR", "MTQ", "PRI", "TCA", "TTO", "VCT", "VGB",\
        "VIR", "ABW", "BES", "BLM", "CUW", "MAF", "SXM"]
south_america = ["ARG", "BOL", "BRA", "CHL", "COL", "ECU", "FLK", "GUF", "GUY", "PER",\
    "PRY", "SUR", "URY", "VEN", "BVT", "SGS"]
central_america = ["BLZ", "CRI", "GTM", "HND", "MEX", "NIC", "PAN", "SLV"]
northern_america = ["BMU", "CAN", "GRL", "SPM", "USA"]
americas = caribbean + south_america + central_america + northern_america
# Europe
southern_europe = ["ALB", "AND", "AZO", "BIH", "ESP", "GIB", "GRC", "HRV", "ITA", "MKD",\
    "MLT", "PRT", "SMR", "SPI", "SCG", "SVN", "VAT", "YUG", "SRB", "MNE"]
western_europe = ["AUT", "BEL", "CHE", "DDR", "DEU", "DFR", "FRA", "LIE", "LUX", "MCO", "NLD"]
eastern_europe = ["BGR", "BLR", "CSK", "CZE", "HUN", "MDA", "POL", "ROU", "RUS", "SVK", "UKR"]
northern_europe = ["CHA", "DNK", "EST", "FIN", "FRO", "GBR", "IRL", "ISL", "LTU", "LVA",\
    "IMN", "NOR", "SWE", "SJM", "ALA", "GGY", "JEY"]
russian_federation = ["SUN"]
europe = southern_europe + western_europe + eastern_europe + northern_europe + russian_federation
# Oceania
polynesia = ["ASM", "COK", "NIU", "PCN", "PYF", "TKL", "TON", "TUV", "WLF", "WSM"]
australia_new_zealand = ["AUS", "NZL", "NFK", "CCK", "CXR", "HMD"]
melanesia = ["FJI", "NCL", "PNG", "SLB", "VUT"]
micronesia = ["FSM", "GUM", "KIR", "MHL", "NRU", "PLW", "MNP", "UMI"]
oceania = polynesia + australia_new_zealand + melanesia + micronesia


# Set variables
url = 'https://public.emdat.be/api/graphql'
headers = {"auth": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoxNTQxMCwidXNlcm5hbWUiOiJ0aGVyYWNlYmxvZ2dlciJ9.jl04tgSr0ESF3hwgp8AmKQXuODrVOqKcJSrRNAnvj_E"}
opName = "emdat_public"
varz =  {
	"classif": storm + heat_wave, # (list)type of disasters - concatenate using +
	"iso": asia, # (list)countries of disasters - concatenate using +
    "from": 1953, #(int)start date - 1900 to 2021
	"to": 1994 # (int)end date - 1900 to 2021
}

# Function using GraphQL to make the API call for link to data API
def run_query(query):
    request = requests.post(url, json={"query": query, "operationName": opName, "variables": varz}, headers=headers)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception("Query failed to run by returning code of {}. {}".format(request.status_code, query))

# Query to get the link for the data
query = "mutation emdat_public($classif: [String!], $iso: [String!], $from: Int, $to: Int) {\n  emdat_public(classif: $classif, iso: $iso, from: $from, to: $to) {\n    count\n    link\n    xlsx\n  }\n}\n"
result = run_query(query)

# Function to get data, store in file '/Users/chuckschultz/work/data/emdat_dump.xlsx' and
# log transaction in file '/Users/chuckschultz/work/data/emdat.log'
def get_emdat():
    try:
        link_to_hit = result["data"]["emdat_public"]["link"]
        dump = requests.get(link_to_hit, headers=headers)
        count = result["data"]["emdat_public"]["count"]
        print("Link:", link_to_hit, "\nCount:", count)
        
        with open('/Users/chuckschultz/work/data/emdat_dump.xlsx', 'wb') as file: # store data
            file.write(dump.content)
        with open('/Users/chuckschultz/work/data/emdat.log', 'a') as file: # log transaction
            file.write(str(datetime.datetime.now()), "\nLink:", link_to_hit, "\nCount:", str(count))

    except TypeError: # If there are no results
        print("Count:", None)
        with open('/Users/chuckschultz/work/data/emdat.log', 'a') as file: # log transaction
            file.write(str(datetime.datetime.now()), "\nCount: None")

get_emdat()