#!/usr/bin/env python3
# -*- coding: utf8

# geocode2osm
# Geocodes ADDRESS tag for nodes in OSM xml file marked with GEOCODE=yes using a variety of techniques
# Usage: python geocode_osm.py [input_filename.osm]
# Geocoded file will be written to input_filename + "_geocoded.osm"
# Log is written to "_log.txt"
# ADDRESS format: "Skøyen skole, Lørenveien 7, 0585 Oslo" (optional first part)


import json
import sys
import urllib.request, urllib.parse, urllib.error
import csv
import time
import re
from io import TextIOWrapper
from xml.etree import ElementTree


version = "1.1.0"

header = {"User-Agent": "osm-no/geocode2osm"}

max_nominatim = 500     # Max number of Nominatim calls during one hour
pause_nominatim = True  # Wait one hour for next Nominatim batch (else do only first batch)
log_queries = True      # Save queries to log file


# Translation table for other information than street names

fix_name = [
	("Rådhuset", "Rådhus"),
	("Kommunehuset", "Kommunehus"),
	("Herredshuset", "Herredshus"),
	("Heradshuset", "Heradshus"),
	("st.", "stasjon"),
	("togstasjon", "stasjon"),
	("jernbanestasjon", "stasjon"),
	("sk.", "skole"),
	("vgs.", "videregående skole"),
	("v.g.s.", "videregående skole"),
	("b&u", "barne og ungdom"),
	("c/o", ""),
	("C/O", ""),
	("C/o", "")
	]


# Translation table for street name corrections
# Code will also test i) without ".", ii) with preceding "s" and iii) will test combinations with synonyms

street_synonyms = [
	['gata', 'gaten', 'gate', 'gt.', 'g.'],
	['veien', 'vegen', 'vei', 'veg', 'vn.', 'v.'],
	['plassen', 'plass', 'pl.'],
	['torv', 'torg'],
	['bro', 'bru'],
	['brygga', 'bryggen', 'bryggja', 'bryggje', 'brygge', 'br.'],
	['løkken', 'løkka', 'løkke'],
	['stuen', 'stua', 'stue'],
	['hagen', 'haven', 'haga', 'hage', 'have'],
	['viken', 'vika', 'vik'],
	['aleen', 'alle'],
	['fjorden', 'fjord'],
	['bukten', 'bukta', 'bukt'],
	['jordet', 'jord'],
	['kollen', 'kolle'],
	['åsen', 'ås'],
	['sletten', 'sletta', 'slette'],
	['verket', 'verk'],
	['toppen', 'topp'],
	['gamle', 'gml.'],
	['kirke', 'kyrkje', 'krk.'],
	['skole', 'skule', 'sk.'],
	['ssons', 'ssens', 'sons', 'sens', 'sson', 'ssen', 'son', 'sen'],
	['theodor', 'th.'],
	['christian', 'chr.'],
	['kristian', 'kr.'],
	['johannes', 'johs.'],
	['edvard', 'edv.']
	]


# This table is not yet supported in the code:

extra_synonyms = [
	['kirke', 'kyrkje'],
	['skole', 'skule'],
	['videregående skole', 'videregåande skule'],
	['rådhus', 'rådhuset'],
	['kommunehus', 'kommunehuset'],
	['herredshus', 'herredshuset', 'heradshus', 'heradshuset'],
	['krk.', 'kirke'],
	['st.', 'stasjon'],
	['v.g.s.', 'videregående skole']
	]


# Table for testing genitive/word separation variations

genitive_tests = [
	('',   ' ' ),  # Example: 'Snorresveg'  -> 'Snorres veg'
	(' ',  ''  ),  # Example: 'Snorres veg' -> 'Snorresveg'
	('',   's' ),  # Example: 'Snorreveg'   -> 'Snorresveg' 
	('',   's '),  # Example: 'Snorreveg'   -> 'Snorres veg'
	(' ',  's '),  # Example: 'Snorre veg'  -> 'Snorres veg'
	(' ',  's' ),  # Example: 'Snorre veg'  -> 'Snorresveg'	
	('s ', ' ' ),  # Example: 'Snorres veg' -> 'Snorre veg'
	('s',  ''  ),  # Example: 'Snorresveg'  -> 'Snorreveg'
	('s',  ' ' )   # Example: 'Snorresveg'  -> 'Snorre veg'
]


# Output message

def message (line):

	sys.stdout.write (line)
	sys.stdout.flush()


# Log query results

def log(log_text):

	if log_queries:
		log_file.write(log_text)


# Open file/api, try up to 5 times, each time with double sleep time

def try_urlopen (url):

	tries = 0
	while tries < 5:
		try:
			return urllib.request.urlopen(url)

		except urllib.error.HTTPError as e:
			if e.code in [429, 503, 504]:  # "Too many requests", "Service unavailable" or "Gateway timed out"
				if tries  == 0:
					message ("\n") 
				message ("\r\tRetry %i in %ss... " % (tries + 1, 5 * (2**tries)))
				time.sleep(5 * (2**tries))
				tries += 1
				error = e
			else:
				message ("\n\nHTTP error %i: %s\n" % (e.code, e.reason))
				message ("%s\n" % url.get_full_url())
				sys.exit()

		except urllib.error.URLError as e:  # Mostly "Connection reset by peer"
			if tries  == 0:
				message ("\n") 
			message ("\r\tRetry %i in %ss... " % (tries + 1, 5 * (2**tries)))
			time.sleep(5 * (2**tries))
			tries += 1
	
	message ("\n\nError: %s\n" % error.reason)
	message ("%s\n\n" % url.get_full_url())
	sys.exit()


# Concatenate address line

def get_address(street, house_number, postal_code, city):

	address = street
	if house_number:
		address = address + " " + house_number  # Includes letter
	if address:
		address = address + ", "
	if postal_code:
		address = address + postal_code + " "
	if city:
		address = address + city

	return address.strip()


# Geocoding with Nominatim

def nominatim_search (query_type, query_text, query_municipality, method):

	global nominatim_count, batch_count, bbox, last_nominatim_time

	# Observe policy of 1 second delay between queries
	time_now = time.time()
	if time_now < last_nominatim_time + 1:
		time.sleep(1 - time_now + last_nominatim_time)

	if not(bbox):
		bbox = get_municipality_data(query_municipality)

	url = "https://nominatim.openstreetmap.org/search?%s=%s&countrycodes=no&viewbox=%f,%f,%f,%f&format=json&limit=10" \
							% (query_type, urllib.parse.quote(query_text),
								bbox['longitude_min'], bbox['latitude_min'], bbox['longitude_max'], bbox['latitude_max'])

	request = urllib.request.Request(url, headers=header)
	file = try_urlopen(request)
	result = json.load(file)
	file.close()

	log ("Nominatim (%s): %s=%s\n" % (method, query_type, query_text))
	log (json.dumps(result, indent=2, ensure_ascii=False))
	log ("\n")
	nominatim_count += 1
	batch_count += 1
	last_nominatim_time = time.time()

	if result:
		if (result[0]['class'] != "boundary") or (result[0]['type'] != "administrative"):  # Skip administrative boundaries (municipalities)
			result = result[0]
		elif len(result) > 1:
			result = result[1]
		else:
			return None

		# Check if coordinates are within the bounding box of the municipality

		latitude = float(result['lat'])
		longitude = float(result['lon'])

		if bbox['latitude_min'] < latitude < bbox['latitude_max'] and \
			bbox['longitude_min'] <longitude < bbox['longitude_max']:

			result_type = "Nominatim/%s -> %s/%s" % (method, result['class'], result['type'])
			if result['type'] == "house" and "address" in method:
				result_quality = "house"
			elif result['class'] == "highway" and "address" in method:
				result_quality = "street"
			elif "address" in method:
				result_quality = "place"
			else:
				result_quality = "district"
			return (latitude, longitude, result_type, result_quality)
		else:
			log ("Nominatim result not within bounding box of municipality\n")
			return None

	else:
		return None


# Geocoding with Matrikkel Vegadresse

def matrikkel_search (street, house_number, house_letter, post_code, city, municipality_ref, method):

	global matrikkel_count

	# Build query string. Use municipality instead of postcode/city if available
	query = ""
	if street:
		query += "sok=%s" % urllib.parse.quote(street.replace("(","").replace(")","").replace(":",""))
	if house_number:
		query += "&nummer=%s" % house_number
	if house_letter:
		query += "&bokstav=%s" % house_letter
	if post_code and not(municipality_ref):
		query += "&postnummer=%s" % post_code
	if city and not(municipality_ref):
		query += "&poststed=%s" % urllib.parse.quote(city)
	if municipality_ref:
		query += "&kommunenummer=%s" % municipality_ref

	url = "https://ws.geonorge.no/adresser/v1/sok?" + query + "&treffPerSide=10"

	request = urllib.request.Request(url, headers=header)
	file = try_urlopen(request)
	result = json.load(file)
	file.close()

	result = result['adresser']

	log ("Matrikkel (%s): %s\n" % (method, urllib.parse.unquote(query)))  # .encode('ASCII')).decode('utf-8')))
	log (json.dumps(result, indent=2, ensure_ascii=False))
	log ("\n")
	matrikkel_count += 1

	if result:
		result_type = "Matrikkel/%s -> %s" % (method, result[0]['objtype'])
		latitude = result[0]['representasjonspunkt']['lat']
		longitude = result[0]['representasjonspunkt']['lon']
		if "address" in method:
			result_quality = "house"
		elif "street" in method:
			result_quality = "street"
		else:
			result_quality = "place"
		return (str(latitude), str(longitude), result_type, result_quality)
	else:
		return None


# Geocoding with SSR

def ssr_search (query_text, query_municipality, method, fuzzy=False):

	global ssr_count, ssr_not_found

	query = "https://ws.geonorge.no/stedsnavn/v1/navn?sok=%s&knr=%s" \
				% (urllib.parse.quote(query_text.replace("(","").replace(")","")), query_municipality)
	if fuzzy:
		query += "&fuzzy=true"
	request = urllib.request.Request(query, headers=header)
	file = try_urlopen(request)
	result = json.load(file)
	file.close()

	log ("SSR (%s): %s, municipality #%s\n" % (method, query_text, query_municipality))
	log (json.dumps(result, indent=2, ensure_ascii=False))
	log ("\n")
	ssr_count += 1

	if result['navn']: # "stedsnavn" in result:

		# Check if name type is defined in category table
		for place in result['navn']:
			if not(place['navneobjekttype'].lower().strip() in ssr_types):
				message ("\n\t**** SSR name type '%s' not found - please post issue at 'https://github.com/osmno/geocode2osm' ****\n\n"\
							% place['navneobjekttype'])
				log ("SSR name type '%s' not found\n" % place['navneobjekttype'])
				if not(place['navneobjekttype'] in ssr_not_found):
					ssr_not_found.append(place['navneobjekttype'])

		# Return the first acceptable result
		for place in result['navn']:
			if place["navneobjekttype"] == "Adressenavn" or \
				place['navneobjekttype'].lower().strip() in ssr_types and \
					ssr_types[ place['navneobjekttype'].lower().strip() ] in ['bebyggelse', 'offentligAdministrasjon', 'kultur']:

				result_type = "SSR/%s -> %s" % (method, place['navneobjekttype'].strip())

				if place["navneobjekttype"] == "Adressenavn" and "street" in method:
					result_quality = "street"
				elif "street" in method:
					result_quality = "place"
				else:
					result_quality = "district"

				return (place['representasjonspunkt']['nord'], place['representasjonspunkt']['øst'], result_type, result_quality)
	
	return None


# Load bounding box for given municipality ref

def get_municipality_data (query_municipality):

	bbox = {
		'latitude_min': 90.0,
		'latitude_max': -90.0,
		'longitude_min': 180.0,
		'longitude_max': -180.0
		}

	if query_municipality and (query_municipality != "2100"):  # Exclude Svalbard
		query = "https://ws.geonorge.no/kommuneinfo/v1/kommuner/%s" % query_municipality
		request = urllib.request.Request(query, headers=header)
		file = try_urlopen(request)
		result = json.load(file)
		file.close()

		for node in result['avgrensningsboks']['coordinates'][0][1:]:
			bbox['latitude_max'] = max(bbox['latitude_max'], node[1])
			bbox['latitude_min'] = min(bbox['latitude_min'], node[1])
			bbox['longitude_max'] = max(bbox['longitude_max'], node[0])
			bbox['longitude_min'] = min(bbox['longitude_min'], node[0])

		log ("Bounding box for municipality #%s: (%f, %f) (%f, %f)\n" % \
			(query_municipality, bbox['latitude_min'], bbox['longitude_min'], bbox['latitude_max'], bbox['longitude_max']))
	else:
		bbox = {
			'latitude_min': -90.0,
			'latitude_max': 90.0,
			'longitude_min': -180.0,
			'longitude_max': 180.0
			}

	return bbox


# Generate list of synonyms and genitive variations

def generate_synonyms (street):

	synonym_list = []
	low_street = street.lower() + " "

	# Iterate all synonyms (twice for abbreviations)

	for synonyms in street_synonyms:
		found = False

		for synonym_word in synonyms:

			if "." in synonym_word:
				test_list = [synonym_word, synonym_word[:-1]]  # Abreviation with and without period
			else:
				test_list = [synonym_word]

			for test_word in test_list:

				# Test synonyms, including abbreviations

				found_position = low_street.rfind(test_word + " ")
				if found_position >= 0:
					found = True

					for synonym_replacement in synonyms:
						if (synonym_replacement != synonym_word) and not("." in synonym_replacement):
							new_street = low_street[0:found_position] + low_street[found_position:].replace(test_word, synonym_replacement)
							synonym_list.append(new_street)

						# Test genitive variations

						if (found_position > 1) and not("sen" in synonyms):
							for genitive_test in genitive_tests:
								if ((low_street[found_position - 1] != " ") or (" " in genitive_test[0])) and\
									((low_street[found_position - 1] != "s") and (low_street[found_position - 2:found_position] != "s ")\
										or not("s" in genitive_test[1])):

									new_street = low_street[0:found_position - 2] + \
										low_street[found_position - 2:].replace(genitive_test[0] + test_word, genitive_test[1] + synonym_replacement)

									if new_street != low_street:
										synonym_list.append(new_street)

	return synonym_list


# Main program

if __name__ == '__main__':

	# Read all data into memory

	message ("\nLoading data...")
	
	if len(sys.argv) > 1:
		filename = sys.argv[1]
	else:
		message ("Please include input osm filename as parameter\n")
		sys.exit()

	tree = ElementTree.parse(filename)

	# Load post code districts from Posten

	post_filename = 'https://www.bring.no/postnummerregister-ansi.txt'
	file = urllib.request.urlopen(post_filename)
	postal_codes = csv.DictReader(TextIOWrapper(file, "windows-1252"), fieldnames=['post_code','post_city','municipality_ref','municipality_name','post_type'], delimiter="\t")
	post_districts = {}

	for row in postal_codes:
		entry = {
			'city': row['post_city'],
			'municipality_ref': row['municipality_ref'],
			'municipality_name': row['municipality_name'],
			'type': row['post_type'],  # G, P or B
			'multiple': False
		}

		# Discovre possible multiples post code districts for the same city name
		if entry['type'] == "G":
			for post_code, post in iter(post_districts.items()):
				if (post['city'] == entry['city']) and (post['type'] == "G"):
					post['multiple'] = True
					entry['multiple'] = True

		post_districts[row['post_code']] = entry

	# Load name categories from Github

	ssr_filename = 'https://raw.githubusercontent.com/osmno/geocode2osm/master/navnetyper.json'
	file = urllib.request.urlopen(ssr_filename)
	name_codes = json.load(file)
	file.close()

	ssr_types = {}
	for main_group in name_codes['navnetypeHovedgrupper']:
		for group in main_group['navnetypeGrupper']:
			for name_type in group['navnetyper']:
				ssr_types[ name_type['visningsnavn'].strip().lower() ] = main_group['navn']

	# Init output files

	message ("\nGeocoding ADDRESS tag for objects marked with GEOCODE tag in file '%s'...\n\n" % filename)

	if filename.find(".osm") >= 0:
		log_filename = filename.replace(".osm", "_geocodelog.txt")
	else:
		log_filename = filename + "_geocodelog.txt"

	if log_queries:
		log_file = open(log_filename, "w")

	nominatim_count = 0
	batch_count = 0
	ssr_count = 0
	matrikkel_count = 0
	tried_count = 0
	geocode_count = 0
	ssr_not_found = []
	last_nominatim_time = time.time()

	hits = {
		'house': 0,
		'street': 0,
		'place': 0,
		'district': 0
	}

	root = tree.getroot()

	# Loop through all elements

	for node in root.iter('node'):

		address_tag = node.find("tag[@k='ADDRESS']")
		geocode_tag = node.find("tag[@k='GEOCODE']")

		if (geocode_tag != None) and (address_tag != None) and (geocode_tag.get("v").lower() not in ["no", "done"]):

			# Decompose address into street, house number, letter, postcode and city
			# Address format: "Skøyen skole, Lørenveien 7, 0585 Oslo" (optional first part)

			tried_count += 1
			address = address_tag.get("v")
			message ("%i %s " % (tried_count, address))	
			log ("\nADDRESS %i: %s\n" % (tried_count, address))

			address = address.replace(",,", ",")
			address_split = address.split(",")
			length = len(address_split)
			for i in range(length):
				address_split[i] = address_split[i].strip()

			if length > 1:
				street = address_split[length - 2]
				postcode = address_split[length - 1][0:4]
				city = address_split[length - 1][5:].strip()
				house_number = ""
				house_letter = ""

				reg = re.search(r'(.*) [0-9]+[ \-\/]+([0-9]+)[ ]*([A-Za-z]?)$', street)
				if not(reg):
					reg = re.search(r'(.*) ([0-9]+)[ ]*([A-Za-z]?)$', street)				
				if reg:
					street = reg.group(1).strip()
					house_number = reg.group(2).upper()
					house_letter = reg.group(3)

				if length > 2:
					street_extra = ", ".join(address_split[0:length - 2])
				else:
					street_extra = ""

				# Better match in Nominatim
				for swap in fix_name:
					street = street.replace(swap[0], swap[1] + " ").replace("  "," ").strip()
					street_extra = street_extra.replace(swap[0], swap[1] + " ").replace("  "," ").strip()

			else:
				street = ""
				street_extra = ""
				house_number = ""
				house_letter = ""
				postcode = address[0:4]
				city = address[5:].strip()

			if postcode in post_districts:
				municipality_ref = post_districts[postcode]['municipality_ref']
				municipality_name = post_districts[postcode]['municipality_name']
				postcode_name = post_districts[postcode]['city']
			else:
				municipality_ref = ""
				municipality_name = ""
				postcode_name = ""
				log ("Post code %s not found in Posten table\n" % postcode)

			# Attempt to geocode address

			log ("[%s], [%s] [%s][%s], [%s] [%s (%s)]\n" % (street_extra, street, house_number, house_letter, postcode, city, postcode_name))
			log ("Municipality #%s: %s\n" % (municipality_ref, municipality_name))

			result = None
			bbox = None

			# First try to find exact location
			if street:

				# Start testing exact addresses
				if house_number:

					# With both postcode and city
					result = matrikkel_search (street, house_number, house_letter, postcode, city, "", "address")

					# Without city
					if not(result):
						result = matrikkel_search (street, house_number, house_letter, postcode, "", "", "address+postcode")

					# Without postcode
					if not(result):
						result = matrikkel_search (street, house_number, house_letter, "", city, "", "address+city")

					# Without house letter
					if not(result) and house_letter:
						result = matrikkel_search (street, house_number, "", "", city, "", "address+city")						

					# With municipality instead of postcode and city
					if not(result) and municipality_ref:
						result = matrikkel_search (street, house_number, house_letter, "", "", municipality_ref, "address+municipality")

					# Try fixes for abbreviations, synonyms and genitive ortography
					if not(result) and municipality_ref:
						for test_street in generate_synonyms(street):
							result = matrikkel_search (test_street, house_number, house_letter, postcode, city, municipality_ref, "address+fix")
							if result:
								break

				# If no house number is given, the street attribute ofte contains a place name
				if not(result) and not(house_number) and municipality_ref:
					result = ssr_search (street, municipality_ref, "street")

				# Try Nominatim to discover amenities etc.
				if not(result) and street_extra and municipality_name:
					result = nominatim_search ("q", get_address(street_extra, "", "", municipality_name),\
								municipality_ref, "address+extra")

				if not(result) and municipality_name:
					result = nominatim_search ("q", get_address(street, house_number, "", municipality_name), municipality_ref, "address")

				# If no result from Nominatim, try SSR for abbreviations, synonyms and genitive ortography
				if not(result) and municipality_ref:
					for test_street in [street] + generate_synonyms(street):
						result = ssr_search (test_street, municipality_ref, "street+fix")
						if result:
							break

#				# Finally, try fuzzy search (results may be unpredictable ...)
#				if not(result) and municipality_ref:  # and not(house_number) 
#					result = ssr_search (street, municipality_ref, "street+fuzzy", fuzzy=True)	


			# Try to find village of post district if only one district per city
			if not(result) and city and municipality_ref:

				# Find city location if city has only one post district
				if post_districts[postcode]['multiple'] == False:
					result = ssr_search (city, municipality_ref, "city")

					if not(result) and (postcode_name != city.upper()):
						result = ssr_search (postcode_name, municipality_ref, "postname")

					if not(result) and municipality_name:
						result = nominatim_search ("q", get_address (city, "", "", municipality_name), municipality_ref, "city")

			# Try to find polygon center of post district (may give results a long way from villages)
			if not(result) and postcode:
				result = nominatim_search ("postalcode", postcode, municipality_ref, "postcode")

			# Try to find village center of city
			if not(result) and city and municipality_ref:
				result = ssr_search (city, municipality_ref, "city")

				if not(result) and (postcode_name != city.upper()):
					result = ssr_search (postcode_name, municipality_ref, "postname")

			# As a last resort, just look up name of post code district
			if not(result) and postcode_name:
				if municipality_name != city.upper():
					result = nominatim_search ("q", get_address (postcode_name, "", "", municipality_name), municipality_ref, "city")

				if not(result):
					result = nominatim_search ("city", postcode_name, municipality_ref, "city")

			# If successful, update coordinates and save geocoding details for information

			if result:

				latitude = result[0]
				longitude = result[1]
				result_type = result[2]
				result_quality = result[3]

				node.set("lat", str(latitude))
				node.set("lon", str(longitude))
				node.set("action", "modify")

				tag = node.find("tag[@k='GEOCODE_METHOD']")
				if tag != None:
					tag.set("v", result_type)
				else:
					node.append(ElementTree.Element("tag", k="GEOCODE_METHOD", v=result_type))

				tag = node.find("tag[@k='GEOCODE_RESULT']")
				if tag != None:
					tag.set("v", result_quality)
				else:
					node.append(ElementTree.Element("tag", k="GEOCODE_RESULT", v=result_quality))

				message ("--> %s (%s)\n" % (result_type, result_quality))
				log ("MATCH WITH %s (precision: %s)\n" % (result_type, result_quality))
				geocode_count += 1

				hits[result_quality] += 1

			else:
				message ("--> NO MATCH\n")
				log ("NO MATCH\n")

				tag = node.find("tag[@k='GEOCODE_RESULT']")
				if tag != None:
					tag.set("v", "no match")
				else:	
					node.append(ElementTree.Element("tag", k="GEOCODE_RESULT", v="not found"))

				tag = node.find("tag[@k='GEOCODE_METHOD']")
				if tag != None:
					node.remove(tag)

			geocode_tag.set("v", "done")  # Do not geocode next time

			# Limit Nominatim calls per hour to observe usage policy

			if batch_count >= max_nominatim:
				if pause_nominatim:
					message ("Sleep for one hour\n")
					batch_count = 0
					time.sleep(60*60)  # SLeep one hour
				else:
					message ("Exceeded %i Nominatim calls per hour\n" % max_nominatim)
					break

	# Wrap up

	if filename.find(".osm") >= 0:
		filename = filename.replace(".osm", "_geocoded.osm")
	else:
		filename = filename + "_geocoded.osm"

	tree.write(filename, encoding='utf-8', method='xml', xml_declaration=True)

	log ("\nNominatim queries:  %i\n" % nominatim_count)
	log ("Matrikkel queries:  %i\n" % matrikkel_count)
	log ("SSR queries:        %i\n" % ssr_count)

	log ("\nHouse hits:         %s\n" % hits['house'])
	log ("Street hits:        %s\n" % hits['street'])
	log ("Place hits:         %s\n" % hits['place'])
	log ("District hits:      %s\n" % hits['district'])
	log ("No hits:            %s\n" % (tried_count - geocode_count))

	message ("\nGeocoded %i of %i objects, written to file '%s'\n" % (geocode_count, tried_count, filename))
	message ("Hits: %i houses (exact addresses), %i streets, %i places (villages, towns), %i post code districts\n" % \
				(hits['house'], hits['street'], hits['place'], hits['district']))
	message ("Nominatim queries: %i (max approx. 600/hour)\n" % nominatim_count)
	message ("Detailed log in file '%s'\n\n" % log_filename)

	if ssr_not_found:
		message ("SSR name types not found: %s - please post issue at 'https://github.com/osmno/geocode2osm'\n" % str(ssr_not_found))

	if log_queries:
		log_file.close()
