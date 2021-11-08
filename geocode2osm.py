#!/usr/bin/env python3
# -*- coding: utf8

# geocode2osm
# Geocodes ADDRESS tag for elements in OSM xml or CSV file marked with GEOCODE=yes using public api's
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


version = "2.0.0"

header = {"User-Agent": "osm-no/geocode2osm"}

max_nominatim = 500     # Max number of Nominatim calls during one hour
pause_nominatim = True  # Wait one hour for next Nominatim batch (else do only first batch)
ssr_filter = True       # Filter SSR to avoid hits in sea/water, terrain etc.


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


# Table for recognizing field names in CSV input file

csv_synonym_tags = {
	'address': ["address", "adresse"],
	'street': ["street", "gate"],
	'house_number': ["house", "house number", "street number", "hus", "nummer", "gatenummer"],
	'postcode': ["zip", "zipcode", "post code", "postcode", "postal code", "postnummer", "post nr"],
	'city': ["city", "poststed"],
	'municipality': ["municipality", "municipality no", "municipality number", "kommune", "kommunenr", "kommunenummer", "kommune nummer"],
	'latitude': ["latitude", "lat", "y", "nord", "north"],
	'longitude': ["longitude", "long", "lon", "x", "øst", "east"],
	'geocode': ["geocode", "geokod"],
	'geocode_method': ["geocode method", "geokodemetode"],
	'geocode_result': ["geocode result", "geokoderesultat"],
}


# Output message

def message (line):

	sys.stdout.write (line)
	sys.stdout.flush()


# Log query results

def log(log_text, action=None):

	global log_file

	if action == "open":
		log_file = open(log_text, "w")
	elif action == "close":
		log_file.close()
	elif log_file != None:
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

	global nominatim_count, nominatim_batch_count, municipality_bbox, last_nominatim_time

	# Observe policy of 1 second delay between queries

	time_now = time.time()
	if time_now < last_nominatim_time + 1:
		time.sleep(1 - time_now + last_nominatim_time)

	if not(municipality_bbox):
		municipality_bbox = get_municipality_bbox(query_municipality)

	url = "https://nominatim.openstreetmap.org/search?%s=%s&countrycodes=no&viewbox=%f,%f,%f,%f&format=json&limit=10" \
							% (query_type, urllib.parse.quote(query_text),
								municipality_bbox['longitude_min'], municipality_bbox['latitude_min'],
								municipality_bbox['longitude_max'], municipality_bbox['latitude_max'])

	request = urllib.request.Request(url, headers=header)
	file = try_urlopen(request)
	result = json.load(file)
	file.close()

	log ("Nominatim (%s): %s=%s\n" % (method, query_type, query_text))
	log (json.dumps(result, indent=2, ensure_ascii=False))
	log ("\n")

	nominatim_count += 1
	nominatim_batch_count += 1
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

		if municipality_bbox['latitude_min'] < latitude < municipality_bbox['latitude_max'] and \
			municipality_bbox['longitude_min'] <longitude < municipality_bbox['longitude_max']:

			result_type = "Nominatim/%s -> %s/%s" % (method, result['class'], result['type'])
			if result['type'] == "house" and "address" in method:
				result_quality = "house"
			elif result['class'] == "highway" and "address" in method:
				result_quality = "street"
			elif "address" in method:
				result_quality = "place"
			else:
				result_quality = "district"

			return {
				'latitude': latitude, 
				'longitude': longitude,
				'method': result_type,
				'quality': result_quality
			}
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
	matrikkel_count += 1

	log ("Matrikkel (%s): %s\n" % (method, urllib.parse.unquote(query)))
	log (json.dumps(result, indent=2, ensure_ascii=False))
	log ("\n")

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

		return {
			'latitude': latitude,
			'longitude': longitude,
			'method': result_type,
			'quality': result_quality
		}
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
			if place['navneobjekttype'] not in ssr_types:
				message ("\n\t**** SSR name type '%s' not found - please post issue at 'https://github.com/osmno/geocode2osm' ****\n\n"\
							% place['navneobjekttype'])
				log ("SSR name type '%s' not found\n" % place['navneobjekttype'])
				if not(place['navneobjekttype'] in ssr_not_found):
					ssr_not_found.append(place['navneobjekttype'])

		# Return the first acceptable result. Also try islands if no hit.

		found_place = None
		for place in result['navn']:

			if not ssr_filter or place["navneobjekttype"] in ["Adressenavn", "Matrikkeladressenavn", "Tilleggsnavn",
				"Flyplass", "Stasjon", "Busstasjon", "Ferjekai", "Kai", "Brygge"] or \
				place['navneobjekttype'] in ssr_types and \
				ssr_types[ place['navneobjekttype'] ] in ['bebyggelse', 'offentligAdministrasjon', 'kultur']:

				found_place = place
				break

		if not found_place:
			for place in result['navn']:
				if place["navneobjekttype"] in ["Øy", "Øy i sjø", "Halvøy", "Halvøy i sjø", "Nes", "Nes i sjø"]:
					found_place = place
					break

		if found_place:
			result_type = "SSR/%s -> %s" % (method, found_place['navneobjekttype'])

			if found_place["navneobjekttype"] =="Adressenavn" and "street" in method:
				result_quality = "street"
			elif "street" in method:
				result_quality = "place"
			else:
				result_quality = "district"

			return {
				'latitude': found_place['representasjonspunkt']['nord'],
				'longitude': found_place['representasjonspunkt']['øst'],
				'method': result_type,
				'quality': result_quality
			}
	
	return None


# Load bounding box for given municipality ref

def get_municipality_bbox (query_municipality):

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


# Main function to geocode one address.

def geocode2osm (address):

	'''
	Input:
	- address: Format "Skøyen skole, Lørenveien 7, 0585 Oslo" (optional first part)
	Returns:
	- Latitude (float)
	- Longitude (float)
	- Geocode search method used ("Matrikkel", "SSR" or "Nominatim" / + type of hit for each method)
	- Geocode quality of result ("house", "street", "place" or "district")
	'''

	global municipality_bbox

	# Decompose address into address, house number, house letter, postcode and (post) city + optional extra first address line. 

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

		# Fixes for better match in Nominatim
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
	municipality_bbox = None

	# Execute api test searches, starting from exact searches, gradually towards more general searches.
	# First hit will be returned.

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

		'''
		# Finally, try fuzzy search (results may be unpredictable ...)
		if not(result) and municipality_ref:  # and not(house_number) 
			result = ssr_search (street, municipality_ref, "street+fuzzy", fuzzy=True)	
		'''

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

	return result



# Indent XML output

def indent_tree(elem, level=0):
    i = "\n" + level*"  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            indent_tree(elem, level+1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i



# Geocode OSM xml file

def geocode_osm_file(filename):

	global tried_count, geocode_count, nominatim_batch_count

	tree = ElementTree.parse(filename)
	root = tree.getroot()



	# Loop all elements in input file

	for element in root:

		address_tag = element.find("tag[@k='ADDRESS']")
		geocode_tag = element.find("tag[@k='GEOCODE']")

		if (geocode_tag != None) and (address_tag != None) and (geocode_tag.get("v").lower() not in ["no", "done"]):

			tried_count += 1
			address = address_tag.get("v")
			message ("%i %s " % (tried_count, address))	
			log ("\nADDRESS %i: %s\n" % (tried_count, address))

			result = geocode2osm(address)

			# If successful, update coordinates and save geocoding details for information

			if result:

				element.set("lat", str(round(result['latitude'], 7)))
				element.set("lon", str(round(result['longitude'], 7)))
				element.set("action", "modify")

				tag = element.find("tag[@k='GEOCODE_METHOD']")
				if tag != None:
					tag.set("v", result['method'])
				else:
					element.append(ElementTree.Element("tag", k="GEOCODE_METHOD", v=result['method']))

				tag = element.find("tag[@k='GEOCODE_RESULT']")
				if tag != None:
					tag.set("v", result['quality'])
				else:
					element.append(ElementTree.Element("tag", k="GEOCODE_RESULT", v=result['quality']))

				geocode_tag.set("v", "done")  # Do not geocode next time

				message ("--> %s (%s)\n" % (result['method'], result['quality']))
				log ("MATCH WITH %s (precision: %s)\n" % (result['method'], result['quality']))
				geocode_count += 1

				hits[ result['quality'] ] += 1

			else:
				message ("--> *** NO MATCH\n")
				log ("NO MATCH\n")

				tag = element.find("tag[@k='GEOCODE_RESULT']")
				if tag != None:
					tag.set("v", "not found")
				else:	
					element.append(ElementTree.Element("tag", k="GEOCODE_RESULT", v="not found"))

				tag = element.find("tag[@k='GEOCODE_METHOD']")
				if tag != None:
					element.remove(tag)

			# Limit Nominatim calls per hour to observe usage policy

			if nominatim_batch_count >= max_nominatim:
				if pause_nominatim:
					message ("Sleep for one hour\n")
					nominatim_batch_count = 0
					time.sleep(60*60)  # SLeep one hour
				else:
					message ("Exceeded %i Nominatim calls per hour\n" % max_nominatim)
					break

	# Output file

	out_filename = filename.replace(".osm", "") + "_geocoded.osm"
	tree.write(out_filename, encoding='utf-8', method='xml', xml_declaration=True)



# Geocode CSV file

def geocode_csv_file(filename):

	global tried_count, geocode_count, nominatim_batch_count

	# Open csv file and get headers

	file = open(filename)
	csv_reader = csv.DictReader(file, delimiter=";")

	fieldnames = csv_reader.fieldnames.copy()
	message ("\tFields: %s\n" % (", ".join(fieldnames)))

	tag_names = {}
	for column in fieldnames:
		for tag, synonyms in iter(csv_synonym_tags.items()):
			if column.lower() in synonyms:
				tag_names[tag] = column
				break

	message("\tFields used: %s\n\n" % (", ".join(list(tag_names.values()))))

	for tag in csv_synonym_tags:
		if tag not in tag_names:
			tag_names[ tag ] = ""

	if tag_names['address'] == "" and tag_names['street'] == "" and tag_names['postcode'] == "" and tag_names['city'] == "":
		message ("Unable to find address or street/postcode/city columns in file\n\n")
		sys.exit()

	# Open output file

	for field in ["geocode", "geocode_method", "geocode_result", "latitude", "longitude"]:
		if tag_names[ field ] == "":
			tag_names[ field ] = field.upper().replace("_", " ")
			fieldnames.append(tag_names[ field ])

	out_filename = filename.replace(".csv", "") + "_geocoded.csv"
	out_file = open(out_filename, "w")
	csv_writer = csv.DictWriter(out_file, fieldnames=fieldnames, delimiter=";")
	csv_writer.writeheader()

	root = ElementTree.Element("osm", version="0.6", generator="n50osm v"+version, upload="false")
	osm_id = -1000


	# Loop all rows in input file	

	break_nominatim = False

	for row in csv_reader:

		# Get row data

		row_data = {}
		for tag in tag_names:
			row_data[tag] = ""
			if tag_names[tag] in row:
				row_data[tag] = row[ tag_names[tag] ]

		row_out = row.copy()

		if (tag_names['geocode'] == "" or row[ tag_names['geocode'] ] not in ["no", "done"]) and not break_nominatim:

			tried_count += 1

			if row_data['address']:
				address = row_data['address']
			else:
				address = get_address(row_data['street'], row_data['house_number'], row_data['postcode'], row_data['city'])

			message ("%i %s " % (tried_count, address))	
			log ("\nADDRESS %i: %s\n" % (tried_count, address))

			result = geocode2osm(address)

			# If successful, update coordinates and save geocoding details for information

			if result:

				row_out[ tag_names['latitude'] ] = str(round(result['latitude'], 7))
				row_out[ tag_names['longitude'] ] = str(round(result['longitude'], 7))
				row_out[ tag_names['geocode_method'] ] = result['method']
				row_out[ tag_names['geocode_result'] ] = result['quality']
				row_out[ tag_names['geocode'] ] = "done"

				message ("--> %s (%s)\n" % (result['method'], result['quality']))
				log ("MATCH WITH %s (precision: %s)\n" % (result['method'], result['quality']))
				geocode_count += 1

				hits[ result['quality'] ] += 1

			else:
				message ("--> *** NO MATCH\n")
				log ("NO MATCH\n")

				row_out[ tag_names['geocode_result'] ] = "no match"
				row_out[ tag_names['geocode_method'] ] = ""
				row_out[ tag_names['geocode'] ] = "yes"

			# Limit Nominatim calls per hour to observe usage policy

			if nominatim_batch_count >= max_nominatim:
				if pause_nominatim:
					message ("Sleep for one hour\n")
					nominatim_batch_count = 0
					time.sleep(60*60)  # SLeep one hour
				else:
					message ("Exceeded %i Nominatim calls per hour\n" % max_nominatim)
					break_nominatim = True

		# Output CSV line
		csv_writer.writerow(row_out)

		# Produce OSM xml node

		latitude = "0.0"
		longitude = "0.0"
		if tag_names['latitude'] in row_out:
			latitude = row_out[ tag_names['latitude'] ]
		if tag_names['longitude'] in row_out:
			longitude = row_out[ tag_names['longitude'] ]
			
		osm_id -= 1
		node = ElementTree.Element("node", id=str(osm_id), action="modify", lat=latitude, lon=longitude)
		root.append(node)

		if not row_out[ tag_names['address']].strip():
			row_out['ADDRESS'] = get_address(row_out['street'], row_out['house_number'], row_out['postcode'], row_out['city'])

		for key, value in iter(row_out.items()):
			if key not in [tag_names['latitude'], tag_names['longitude']]:
				osm_key = key
				for tag in ["address", "geocode", "geocode_method", "geocode_result"]:
					if key == tag_names[ tag ]:
						osm_key = tag.upper()
				osm_key = osm_key.replace("  ", " ").replace(" ", "_").strip()
				tag = ElementTree.Element("tag", k=osm_key, v=value)
				node.append(tag)

	# Save/close files

	file.close()
	out_file.close()

	if "-noosm" not in sys.argv:
		out_osm_filename = filename.replace(".csv", "") + "_geocoded.osm"
		root.set("upload", "false")
		indent_tree(root)
		tree = ElementTree.ElementTree(root)
		tree.write(out_osm_filename, encoding='utf-8', method='xml', xml_declaration=True)
		out_filename += "/" + out_osm_filename

	return out_filename



# Init section

if "init":

	# Load post code districts from Posten

	post_filename = 'https://www.bring.no/postnummerregister-ansi.txt'
	file = urllib.request.urlopen(post_filename)
	postal_codes = csv.DictReader(TextIOWrapper(file, "windows-1252"), 
		fieldnames=['post_code','post_city','municipality_ref','municipality_name','post_type'], delimiter="\t")

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

		post_districts[ row['post_code'] ] = entry

	file.close()

	# Load name categories from Github. Used for filtering SSR search results by name type.

	ssr_filename = 'https://raw.githubusercontent.com/osmno/geocode2osm/master/navnetyper.json'
	file = urllib.request.urlopen(ssr_filename)
	name_codes = json.load(file)
	file.close()

	ssr_types = {}
	for main_group in name_codes['navnetypeHovedgrupper']:
		for group in main_group['navnetypeGrupper']:
			for name_type in group['navnetyper']:
				ssr_types[ name_type['visningsnavn'] ] = main_group['navn']

	# Init global variables

	last_nominatim_time = time.time()

	nominatim_batch_count = 0  # Calls between each one hour pause
	nominatim_count = 0
	ssr_count = 0
	matrikkel_count = 0

	municipality_bbox = None
	log_file = None
	ssr_not_found = []  # SSR name tags not found in coversion table



# Main program

if __name__ == '__main__':

	message ("\nGeocoding ADDRESS tag for objects marked with GEOCODE tag\n\n")

	# Load input files

	if len(sys.argv) > 1 and (".osm" in sys.argv[1] or ".csv" in sys.argv[1]):
		filename = sys.argv[1]
	else:
		message ("Please include input osm or csv filename as parameter\n")
		sys.exit()

	message ("Loading file '%s' ...\n" % filename)

	if "-log" in sys.argv:
		log_filename = filename.replace(".osm", "").replace(".csv", "") + "_geocodelog.txt"
		log(log_filename, action="open")

	tried_count = 0
	geocode_count = 0

	hits = {
		'house': 0,
		'street': 0,
		'place': 0,
		'district': 0
	}

	if ".osm" in filename:
		out_filename = geocode_osm_file(filename)
	elif ".csv" in filename:
		out_filename = geocode_csv_file(filename)

	# Wrap up

	log ("\nNominatim queries:  %i\n" % nominatim_count)
	log ("Matrikkel queries:  %i\n" % matrikkel_count)
	log ("SSR queries:        %i\n" % ssr_count)

	log ("\nHouse hits:         %s\n" % hits['house'])
	log ("Street hits:        %s\n" % hits['street'])
	log ("Place hits:         %s\n" % hits['place'])
	log ("District hits:      %s\n" % hits['district'])
	log ("No hits:            %s\n" % (tried_count - geocode_count))

	if log_file:
		log_file.close()

	message ("\nGeocoded %i of %i objects, written to file '%s'\n" % (geocode_count, tried_count, out_filename))
	if geocode_count < tried_count:
		message ("%i objects not found. Adjust address and run agin.\n")
	message ("Hits: %i houses (exact addresses), %i streets, %i places (villages, towns), %i post code districts\n" % \
				(hits['house'], hits['street'], hits['place'], hits['district']))
	message ("Nominatim queries: %i (max approx. 600/hour)\n" % nominatim_count)

	if log_file:
		message ("Detailed log in file '%s'\n" % log_filename)

	if ssr_not_found:
		message ("SSR name types not found: %s - please post issue at 'https://github.com/osmno/geocode2osm'\n" % str(ssr_not_found))

	message ("\n")
