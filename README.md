# geocode2osm
Geocoding for OSM in Norway.

Usage: <code>python geocode2osm.py \<input_filename\> [-log]</code>.

* Accepts OSM files and CSV files as input.
* Geocodes the *ADDRESS* tag/field where *GEOCODE=yes*.
* Outputs a file with *"_geocoded.osm"* ending, plus CSV file if given as input .
* Format of ADDRESS: <code>Skøyen skole, Lørenveien 7, 0585 Oslo</code> (the first part is optional).
* CSV files may alternatively have separate fields for street, postal code and city.
* If street address is not found, the program tries to fix common mistakes (vei/veg etc.).
* Geocoding results are divided into three categories:
  * *House* - exact match with address.
  * *Street* - matchs with street.
  * *Place* - closest village/town etc. sharing the same name.
  * *District* - area given by post code or municipality.
* Please edit ADDRESS tags and run the program again to try out corrections.
* A detailed log is saved to a *"_ceocodelog.txt"* file if the <code>-log</code> parameter is given.
* Other Python program may import geocode2osm and use the <code>geocode(address)</code> function to geocode one address. Please see comments on arguments and responses in the code of that function.

The following services are used for geocoding:
* Kartverket cadastral register.
* Kartverket SSR place names.
* OSM Nominatim (limited number of queries - the program will pause to limit call to 500 per hour).

Changelog:
* 2.1: Simplify result tags + load SSR name types from [ssr2osm](https://github.com/NKAmapper/ssr2osm/blob/main/navnetyper_tagged.json).
* 2.0: Support CSV input files and support import to other Python programs.
* 1.1: Support new SSR api + improved synonym searches for streets.
* 1.0: Python 3 version.
