from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from PIL import Image
import requests
from credentials import MONGO_URL
import re
from bson.objectid import ObjectId
import os
import csv
from datetime import datetime


CD_FIELDS = [
    'cd_number',
    'cd_collector',
    'cd_port',
    'cd_name',
    'cd_date',
    'cd_nationality',
    'cd_birthplace',
    'cd_age',
    'cd_complexion',
    'cd_height',
    'cd_hair',
    'cd_build',
    'cd_eyes',
    'cd_marks',
    'cd_family',
    'cd_resident',
    'cd_arrival_date',
    'cd_place_of_residence',
    'cd_occupation',
    'cd_property',
    'cd_departure_date',
    'cd_destination',
    'cd_ship',
    'cd_references'
]

CEDT_FIELDS = [
    'cedt_book_number',
    'cedt_number',
    'cedt_collector',
    'cedt_state',
    'cedt_name',
    'cedt_period',
    'cedt_date',
    'cedt_nationality',
    'cedt_birthplace',
    'cedt_age',
    'cedt_complexion',
    'cedt_height',
    'cedt_hair',
    'cedt_build',
    'cedt_eyes',
    'cedt_marks',
    'cedt_departure_date',
    'cedt_port_departure',
    'cedt_destination',
    'cedt_ship_departure',
    'cedt_arrival_date',
    'cedt_ship_arrival',
    'cedt_port_arrival'
]

LANDING_FIELDS = [
    'landing_name',
    'landing_date',
    'landing_nationality',
    'landing_ship',
    'landing_place'
]


FORM_FIELDS = {
    'certificate_domicile': CD_FIELDS,
    'certificate_exempting': CEDT_FIELDS,
    'landing_form': LANDING_FIELDS
}


def get_subject(id):
    dbclient = MongoClient(MONGO_URL)
    db = dbclient.get_default_database()
    subj = db.subjects.find_one({'_id': ObjectId(id)})
    return subj

# Finished will either be transcribed + complete or consensus + complete

# root subject -> transcribed, if complete yay, if retired -> consensus if complete yay


def find_root(subj):
    while subj['type'] != 'root':
        subj = get_subject(subj['parent_subject_id'])
    return subj


def save_photos(orientation='front'):
    counts = {}
    dbclient = MongoClient(MONGO_URL)
    db = dbclient.get_default_database()
    photos = db.subjects.find({'type': 'marked_photo_{}'.format(orientation), 'region.width': {'$gte': 100}, 'region.height': {'$gte': 100}}).batch_size(10)
    with open(os.path.join('data', 'csv', 'photos-{}-{}.csv'.format(orientation, datetime.now().strftime('%Y%m%d'))), 'wb') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['barcode', 'page', 'page_url', 'crop_filename', 'x', 'y', 'width', 'height'])
        for photo in photos:
            im_url = photo['location']['standard']
            print im_url
            details = re.search(r'(\d+)-p(\d+)\.jpg', im_url)
            pid = '{}-{}'.format(details.group(1), details.group(2))
            try:
                counts[pid] += 1
            except KeyError:
                counts[pid] = 0
            filename = '{}-{}-{}.jpg'.format(pid, orientation, counts[pid])
            # item = db.items.find_one({'identifier': details.group(1)})
            # citation = 'NAA: {}, {}, p. {}'.format(item['series'], item['control_symbol'], details.group(2))
            if not os.path.exists(os.path.join('data', 'photos', filename)):
                im = Image.open(requests.get(im_url, stream=True).raw)
                coords = [
                    photo['region']['x'] + 10,
                    photo['region']['y'] + 10,
                    photo['region']['x'] + photo['region']['width'] - 10,
                    photo['region']['y'] + photo['region']['height'] - 10
                ]
                # print coords
                crop = im.crop(coords)
                crop.save('data/photos/{}'.format(filename))
            writer.writerow([details.group(1), details.group(2), im_url, filename, photo['region']['x'], photo['region']['y'], photo['region']['width'], photo['region']['height']])


def save_prints(print_type='handprint'):
    counts = {}
    dbclient = MongoClient(MONGO_URL)
    db = dbclient.get_default_database()
    prints = db.subjects.find({'type': 'marked_{}'.format(print_type), 'region.width': {'$gte': 50}, 'region.height': {'$gte': 50}}).batch_size(10)
    with open(os.path.join('data', 'csv', '{}s-{}.csv'.format(print_type, datetime.now().strftime('%Y%m%d'))), 'wb') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['barcode', 'page', 'page_url', 'crop_filename', 'x', 'y', 'width', 'height'])
        for hprint in prints:
            im_url = hprint['location']['standard']
            print im_url
            details = re.search(r'(\d+)-p(\d+)\.jpg', im_url)
            pid = '{}-{}'.format(details.group(1), details.group(2))
            try:
                counts[pid] += 1
            except KeyError:
                counts[pid] = 0
            filename = '{}-{}-{}.jpg'.format(pid, print_type, counts[pid])
            # item = db.items.find_one({'identifier': details.group(1)})
            # citation = 'NAA: {}, {}, p. {}'.format(item['series'], item['control_symbol'], details.group(2))
            if not os.path.exists(os.path.join('data', 'prints', filename)):
                im = Image.open(requests.get(im_url, stream=True).raw)
                coords = [
                    hprint['region']['x'] + 10,
                    hprint['region']['y'] + 10,
                    hprint['region']['x'] + hprint['region']['width'] - 10,
                    hprint['region']['y'] + hprint['region']['height'] - 10
                ]
                # print coords
                crop = im.crop(coords)
                crop.save('data/prints/{}'.format(filename))
            writer.writerow([details.group(1), details.group(2), im_url, filename, hprint['region']['x'], hprint['region']['y'], hprint['region']['width'], hprint['region']['height']])


def save_characters():
    counts = {}
    dbclient = MongoClient(MONGO_URL)
    db = dbclient.get_default_database()
    chars = db.subjects.find({'type': 'marked_chinese_characters', 'region.width': {'$gte': 20}, 'region.height': {'$gte': 20}}).batch_size(10)
    with open(os.path.join('data', 'csv', 'characters-{}.csv'.format(datetime.now().strftime('%Y%m%d'))), 'wb') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['barcode', 'page', 'page_url', 'crop_filename', 'x', 'y', 'width', 'height'])
        for char in chars:
            im_url = char['location']['standard']
            print im_url
            details = re.search(r'(\d+)-p(\d+)\.jpg', im_url)
            pid = '{}-{}'.format(details.group(1), details.group(2))
            try:
                counts[pid] += 1
            except KeyError:
                counts[pid] = 0
            filename = '{}-{}.jpg'.format(pid, counts[pid])
            # item = db.items.find_one({'identifier': details.group(1)})
            # citation = 'NAA: {}, {}, p. {}'.format(item['series'], item['control_symbol'], details.group(2))
            if not os.path.exists(os.path.join('data', 'characters', filename)):
                im = Image.open(requests.get(im_url, stream=True).raw)
                coords = [
                    char['region']['x'] + 10,
                    char['region']['y'] + 10,
                    char['region']['x'] + char['region']['width'] - 10,
                    char['region']['y'] + char['region']['height'] - 10
                ]
                # print coords
                crop = im.crop(coords)
                crop.save('data/characters/{}'.format(filename))
            writer.writerow([details.group(1), details.group(2), im_url, filename, char['region']['x'], char['region']['y'], char['region']['width'], char['region']['height']])


def list_gender(gender='female'):
    dbclient = MongoClient(MONGO_URL)
    db = dbclient.get_default_database()
    people = db.classifications.find({'task_key': 'pick_photo_gender', 'annotation.value': gender})
    for person in people:
        subject = get_subject(person['subject_id'])
        print 'http://iabrowse.herokuapp.com/items/{}/pages/{}/'.format(subject['meta_data']['set_key'], subject['meta_data']['page'])
    # print list(people)


def list_page_types():
    dbclient = MongoClient(MONGO_URL)
    db = dbclient.get_default_database()
    picks = db.classifications.find({'task_key': 'pick_page_type'}).batch_size(10)
    for pick in picks:
        saved = db.completed_tasks.find_one({'_id': pick['_id']})
        if saved is not None:
            print 'Already saved'
        else:
            db.completed_tasks.insert_one({'_id': pick['_id']})
            subject = get_subject(pick['subject_id'])
            barcode = subject['meta_data']['set_key']
            page = int(subject['meta_data']['page'])
            annotation = {'field': 'pick_page_type', 'value': pick['annotation']['value']}
            db.images.update_one({'identifier': barcode, 'page': page}, {'$addToSet': {'annotations': annotation}})
            print 'Page: {}, page {}'.format(barcode, page)
            print 'Annotation: {}\n'.format(annotation)


def list_completions():
    dbclient = MongoClient(MONGO_URL)
    db = dbclient.get_default_database()
    completed = db.subjects.find({'status': 'complete'}).batch_size(10)
    for complete in completed:
        saved = db.completions.find_one({'_id': complete['_id']})
        if saved is not None:
            print 'Already saved'
        else:
            db.completions.insert_one({'_id': complete['_id']})
            field = complete['type'].replace('transcribed_', '').replace('consensus_', '')
            try:
                value = complete['data']['values'][0]['value']
            except KeyError:
                value = complete['data']['value']
            root = find_root(complete)
            barcode = root['meta_data']['set_key']
            page = int(root['meta_data']['page'])
            # print root
            # page_number = re.search(r'p(\d+)\.jpg', root['location']['standard']).group(1)
            # page_id = '{}-{}'.format(barcode, page_number)
            # try:
            #    pages[page_id]['fields'].append({'field': field, 'value': value})
            # except KeyError:
            #   pages[page_id] = {'barcode': barcode, 'page': page_number, 'image': root['location']['standard'], 'fields': [{'field': field, 'value': value}]}
            annotation = {'field': field, 'value': value, 'region': complete['region']}
            db.images.update_one({'identifier': barcode, 'page': page}, {'$addToSet': {'annotations': annotation}})
            print 'Page: {}, page {}'.format(barcode, page)
            print 'Annotation: {}\n'.format(annotation)


def write_csv_completions(page_type):
    dbclient = MongoClient(MONGO_URL)
    db = dbclient.get_default_database()
    form_fields = FORM_FIELDS[page_type]
    pages = db.images.find({'annotations.value': page_type})
    with open(os.path.join('data', 'csv', '{}-{}.csv'.format(page_type, datetime.now().strftime('%Y%m%d'))), 'wb') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['barcode', 'page'] + form_fields)
        for page in pages:
            fields = []
            has_data = False
            page_type = [a['value'] for a in page['annotations'] if a['field'] == 'pick_page_type']
            if len(page_type) == 1:
                for field in form_fields:
                    try:
                        value = [a['value'] for a in page['annotations'] if a['field'] == field][0]
                        has_data = True
                    except IndexError:
                        value = None
                    fields.append(value)
                if has_data:
                    writer.writerow([page['identifier'], page['page']] + fields)
            else:
                print page_type
