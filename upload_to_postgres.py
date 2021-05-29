import argparse
import csv
import datetime
import psycopg2
from psycopg2.extras import Json
from psycopg2 import pool
import re
import threading

# Instantiates a reader for pipe separated files.
csv.register_dialect('piper', delimiter='|', quoting=csv.QUOTE_NONE)
property_styles = {
    'A': 'Colonial',
    'B': 'Garrison',
    'C': 'Cape',
    'D': 'Contemporary',
    'E': 'Ranch',
    'F': 'Raised Ranch',
    'G': 'Split Entry',
    'H': 'Victorian',
    'I': 'Tudor',
    'J': 'Gambrel /Dutch',
    'K': 'Antique',
    'L': 'Farmhouse',
    'M': 'Saltbox',
    'N': 'Cottage',
    'O': 'Bungalow',
    'Q': 'Multi-Level',
    'S': 'Log',
    'T': 'Front to Back Split',
    'U': 'Lofted Split',
    'V': 'Greek Revival',
    'W': 'Shingle',
    'X': 'Mid-Century Modern',
    'Y': 'Villa',
    'Z': 'Carriage House',
    '1': 'Craftsman',
    '2': 'Georgian',
    '3': 'Queen Anne',
    '4': 'Spanish Colonial',
    '5': 'Italianate',
    '6': 'Dutch Colonial',
    '7': 'French Colonial',
    '8': 'Gothic Revival',
    '9': 'Second Empire',
    '10': 'Colonial Revival',
    '11': 'Neoclassical',
    '12': 'Prairie',
    '13': 'Octagon',
    '14': 'Federal',
    '15': 'Chateauesque',
    'P': 'Other (See Remarks)'
}

# Insert SQL Template to insert listings into boston_ma table only when
# a listing with the listing_id doesn't already exist.
INSERT = '''
    INSERT INTO
        boston_ma
            (
                agent_id,
                baths_full,
                baths_half,
                beds,
                facts,
                hashtags,
                idx_open_houses,
                idx_virtual_tours,
                last_updated,
                listing_id,
                open_house_soon,
                pictures,
                price,
                remarks,
                sort_id,
                sqft,
                status,
                street_name,
                street_no,
                type,
                zip
            )
    SELECT
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s::json[],
        %s::json[],
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
    WHERE
        NOT EXISTS
            (
                SELECT 1 FROM boston_ma WHERE listing_id = %s
            )
'''
INSERT_COLS = ['agent_id', 'baths_full', 'baths_half', 'beds', 'facts', 'hashtags', 'idx_open_houses', 'idx_virtual_tours', 'last_updated', 'listing_id', 'open_house_soon', 'pictures', 'price', 'remarks', 'sort_id', 'sqft', 'status', 'street_name', 'street_no', 'type', 'zip', 'listing_id']


# Update SQL Template to update listings in boston_ma table
UPDATE = '''
    UPDATE
        boston_ma
    SET
        agent_id = %s,
        baths_full = %s,
        baths_half = %s,
        beds = %s,
        facts = %s,
        hashtags = %s,
        idx_open_houses = %s::json[],
        idx_virtual_tours = %s::json[],
        last_updated = %s,
        open_house_soon = %s,
        pictures = %s,
        price = %s,
        remarks = %s,
        sort_id = GREATEST(CASE WHEN videos IS NULL or videos = '{}' THEN NULL ELSE '60-' || %s END, %s),
        sqft = %s,
        status = %s,
        street_name = %s,
        street_no = %s,
        type = %s,
        zip = %s
    WHERE
        listing_id = %s
'''
UPDATE_COLS = ['agent_id', 'baths_full', 'baths_half', 'beds', 'facts', 'hashtags', 'idx_open_houses', 'idx_virtual_tours', 'last_updated', 'open_house_soon', 'pictures', 'price', 'remarks', 'listing_id', 'sort_id', 'sqft', 'status', 'street_name', 'street_no', 'type', 'zip', 'listing_id']

'''
Upserts the given listing.
As there is no "UPSERT" in postgres, run two queries:
- Update the record, if it exists
- Inset the record, if it does not exist
'''
def upsert_listing(listing):

    def _execute_many(query, cols):
        values = [[listing.get(col) for col in cols]]
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        try:
            cursor.executemany(query, values)
            connection.commit()
        finally:
            cursor.close()
            connection_pool.putconn(connection)

    def _update_db_listing():
        print (f'Updating {listing["type"]} {listing["listing_id"]}')
        _execute_many(UPDATE, UPDATE_COLS)
        
    def _insert_db_listing():
        print (f'Inserting {listing["type"]} {listing["listing_id"]}')
        _execute_many(INSERT, INSERT_COLS)

    _update_db_listing()
    _insert_db_listing()

# Remove all non-numeric and decimal point characters
def _get_int_value(val):
    try:
        return int(re.sub('[^0-9]', '', str(val)))
    except:
        return None

# Remove all non-numeric characters
def _get_float_value(val):
    try:
        return float(re.sub('[^0-9\.]', '', str(val)))
    except:
        return None

'''
Validates whether the given URL is a valid video URL.
A valid URL is one that is on a list of allowed domains.
'''
def _validate_video_url(val):
    if not val:
        return False
    if not 'facebook.com' in val \
            and not 'fb.com' in val \
            and not 'matterport.com' in val \
            and not 'youtu' in val \
            and not 'zoom.us' in val:
        return False
    return True

'''
Get a listing dict object from the given row dict object.
The row dict object contains the listing's information from mlspin.
'''
def get_listing(row, listing_type):
    listing = {}
    listing_id = row['LIST_NO']
    hashtags_src_text = re.sub('[^a-z]', '', row['REMARKS'].lower())

    listing['agent_id'] = row['LIST_AGENT']
    listing['baths_full'] = _get_int_value(row.get('NO_FULL_BATHS', row.get('TOTAL_FULL_BATHS', 0)))
    listing['baths_half'] = _get_int_value(row.get('NO_HALF_BATHS', row.get('TOTAL_HALF_BATHS', 0)))
    listing['beds'] = _get_int_value(row.get('NO_BEDROOMS', row.get('TOTAL_BRS', 0)))

    listing['facts'] = Json({
        'Acre': row.get('ACRE'),
        'Area': row.get('AREA'),
        'Basement': row.get('BASEMENT'),
        'Floors': row.get('NO_FLOORS'),
        'Garage Parking': row.get('GARAGE_PARKING'),
        'Garage Spaces': row.get('GARAGE_SPACES'),
        'Lot Size': row.get('LOT_SIZE'),
        'Neighborhood': row.get('NEIGHBORHOOD'),
        'Sq Ft': row.get('SQUARE_FEET'),
        'Status': row.get('STATUS'),
        'Style': property_styles.get(row.get('STYLE')),
        'Taxes': row.get('TAXES'),
        'Units': row.get('NO_UNITS'),
        'Year Built': row.get('YEAR_BUILT'),
    })

    listing['hashtags'] = []
    for x in all_hashtags:
        if x in hashtags_src_text:
            listing['hashtags'].append(x)
    listing['hashtags'] = list(set(listing['hashtags']))
    listing['hashtags'].sort()
    listing['hashtags'] = [listing_type, f'{listing["beds"]}bed', f'{listing["baths_full"]}bath'] + listing['hashtags']

    listing['idx_open_houses'] = [Json(l) for l in idx_open_houses.get(listing_id)] if idx_open_houses.get(listing_id) else None
    listing['idx_virtual_tours'] = [Json(l) for l in idx_virtual_tours.get(listing_id)] if idx_virtual_tours.get(listing_id) else None
    listing['last_updated'] = last_updated
    listing['listing_id'] = listing_id

    listing['pictures'] = []
    photo_count = int(row.get('PHOTO_COUNT'))
    for indx in range(0, photo_count):
        listing['pictures'].append(f'https://idx.mlspin.com/photo/photo.aspx?nopadding=1&mls={listing_id}&n={indx}')

    listing['open_house_soon'] = True if idx_open_houses.get(listing_id) and idx_open_houses.get(listing_id)[0]['openHouseSoon'] else False
    listing['price'] = float(row['LIST_PRICE'])
    listing['remarks'] = row['REMARKS']
    if listing['open_house_soon']:
        listing['sort_id'] = f'70-{listing_id}'
    elif idx_virtual_tours.get(listing_id):
        listing['sort_id'] = f'50-{listing_id}'
    else:
        listing['sort_id'] = f'20-{listing_id}'
    listing['sqft'] = _get_float_value(row.get('SQUARE_FEET'))
    listing['status'] = row['STATUS']
    listing['street_name'] = row['STREET_NAME']
    listing['street_no'] = row['STREET_NO']
    listing['type'] = listing_type
    listing['zip'] = row['ZIP_CODE'].zfill(5)

    return listing

# Process the given row of mlspin information for a listing
def process_row(row, listing_type):
    try:
        listing = get_listing(row, listing_type)
        upsert_listing(listing)
    except Exception as ex:
        print (ex)

# Get the list of all realtour supported hashtags
def get_all_hashtags():
    with open('hashtags.txt','r') as hashtags_file:
        hashtags = hashtags_file.readlines()
        hashtags = [x.strip() for x in hashtags]
        return hashtags

# Read the given mlspin data file into a dict object
def get_pipe_data(file_name):
    with open(file_name,'r') as pipe_file:
        file_data = csv.DictReader(pipe_file, dialect='piper')
        file_data = [f for f in file_data]
        return file_data

'''
Process the given mlspin file for the given listing type
'''
def process(file_name, listing_type):
    file_data = get_pipe_data(file_name)
    threads = []

    for row in file_data:
        # Run in 20 parallel threads, each processing one listing
        t = threading.Thread(target=process_row, args=(row, listing_type, ))
        threads.append(t)
        t.start()

        if len(threads) >= 20:
            for t in threads:
                t.join()
            threads = []

    for t in threads:
        t.join()

    # Set status of listings that are no longer as RT-ACT
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute("UPDATE boston_ma SET status = 'RT-ACT' WHERE type = %s and (last_updated is null or last_updated < %s) ", [listing_type, last_updated])
    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


'''
Run the upload process.
If requires a --db argument, which tells whether to use dev or live database.
'''
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='DB')
    parser.add_argument('--db')
    args = vars(parser.parse_args())

    if not args.get('db'):
        raise('No db')

    db = args['db']
    last_updated = datetime.datetime.now().isoformat()
    one_week = (datetime.datetime.now() + datetime.timedelta(7)).isoformat()

    all_hashtags = get_all_hashtags()

    # Get all the open houses scheduled in the current week.
    idx_open_houses = {}
    for open_house in get_pipe_data('idx_OH.txt'):
        if not open_house.get('VIRTUALEVENTURL') or not open_house.get('START_DATE') or not open_house.get('END_DATE') or open_house.get('EVENTTYPEDESCRIPTION', '').strip().lower() != 'virtual':
            continue
        url = open_house.get('VIRTUALEVENTURL').strip()
        if not _validate_video_url(url):
            continue
        list_no = open_house['LIST_NO'].strip()
        if not idx_open_houses.get(list_no):
            idx_open_houses[list_no] = []
        idx_open_houses[list_no].append({
            'endDateTime': open_house.get('END_DATE').strip(),
            'openHouseSoon': last_updated < open_house.get('END_DATE').strip() < one_week,
            'startDateTime': open_house.get('START_DATE').strip(),
            'type': 'virtual',
            'url': url
        })
    for listing_id in idx_open_houses:
        idx_open_houses[listing_id].sort(key=lambda o: f'9{o["startDateTime"]}' if o['openHouseSoon'] else o['startDateTime'])

    # Get all the virtual tours associated.
    idx_virtual_tours = {}
    for virtual_tour in get_pipe_data('idx_VT.txt'):
        url = virtual_tour['TOUR_URL'].strip()
        if not _validate_video_url(url):
            continue
        list_no = virtual_tour['LIST_NO'].strip()
        if not idx_virtual_tours.get(list_no):
            idx_virtual_tours[list_no] = []
        idx_virtual_tours[list_no].append({'url': url})

    if (db == 'dev'):
        connection_pool = psycopg2.pool.ThreadedConnectionPool(
            20,
            50,
            database = 'dev',
            host = '35.196.167.212',
            password = 'hellodevworld!!12345',
            user = 'dev-user'
        )
    else:
        connection_pool = psycopg2.pool.ThreadedConnectionPool(
            20,
            50,
            database = 'live',
            host = '35.196.167.212',
            password = 'helloworld!!12345',
            user = 'app-user'
        )

    process('idx_sf.txt', 'singlefamily')
    process('idx_mf.txt', 'multifamily')
    process('idx_cc.txt', 'condo')

    connection_pool.closeall()
