import ujson as json
import os
import sqlite3

import requests


def try_up_to_x_times(retries, func, *args, **kwargs):
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if i+1 < retries:
                print('Retry %s: %s'%(i, e))
            else:
                raise


def fetch_with_cursor(cursor, filename_index):
    api_key = open("freebase_api_key").read()
    service_url = 'https://www.googleapis.com/freebase/v1/mqlread'
    query = [{
      "name": None,
      "id": None,
      "parents": [{}],
      "gender": None,
      "spouse_s": [{}],
      "type": "/people/person",
      "limit": 1000
    }]
    params = {
        'query': json.dumps(query),
        'key': api_key,
        'cursor': cursor,
    }

    r = try_up_to_x_times(3, requests.get, service_url, params=params)
    if r.status_code != 200:
        print('Error', r.text)
        print(r.request.url)
        return False

    else:
        filename = 'people_%s.json' % filename_index
        if os.path.exists(filename):
            print('Error: ', filename, 'exists already')
            return

        with open(filename, 'w') as f:
            f.write(r.text)
        return r.json()['cursor']


def fetch_all_people():
    index = 0
    cursor = fetch_with_cursor('', index)
    index += 1
    while cursor:
        print('Fetching', index, cursor)
        cursor = fetch_with_cursor(cursor, index)
        index += 1


def import_data():
    with open('people.json') as f:
        data = json.load(f)
    raw_people = data['result']

    people = {}
    for p in raw_people:
        name = p['name']
        people[name] = {
            'name': name,
            'parents': [],
            'children': [],
        }

    for p in raw_people:
        for parent_name in p.get('parents', []):
            parent = people.get(parent_name)
            if parent:
                person = people[p['name']]
                person['parents'].append(parent)
                parent['children'].append(person)

            else:
                print('no parents', p['name'])

    return people


def count_generations(person):
    print(person['name'], len(person['parents']), person)
    return
    if person['parents']:
        counts = [0]
        for parent in person['parents']:
            counts.append(count_generations(parent))
        return max(counts)
    else:
        return 1


def analyze(people):
    no_children = 0
    no_parents = 0

    for p in people.itervalues():
        no_children += len(p['children'])
        no_parents += len(p['parents'])


    family_count = 0
    for name, person_dict in people.iteritems():
        if person_dict['children'] and not person_dict['parents']:
            family_count += 1
            #print name, person_dict['children'][0]

    for name, person_dict in people.iteritems():
        if not person_dict['children'] and person_dict['parents']:
            print(name, count_generations(person_dict))

    print(len(people), no_children, no_parents, family_count)


#fetch_people()
#people = import_data()
#analyze(people)

'''
die meisten nachkommen
groester zusammenhaengender baum
'''

'''import freebase

session = freebase.HTTPMetawebSession("https://www.googleapis.com")
res = session.mqlread([{
  "name": None,
  "parents": [],
  "type": "/people/person"
}])

print res
'''


def read_file():
    with open('people.json') as f:
        data = json.load(f)

    people = {}
    for p in data['result']:
        people[p['id']] = p

    for p_id, p in people.items():
        for parent in p['parents']:
            if parent['id'] in people:
                people[parent['id']].setdefault('children', []).append(p)

    print(len(people))
    return people




def read_files():
    all_people = {}
    for filename in sorted(os.listdir('json')):
        print(filename)
        with open('json/' + filename) as f:
            content = json.load(f)['result']
            for person in content:
                all_people[person['id']] = person

    print(len(all_people))
    return all_people


def add_children(people):
    for person in people.values():
        for parent in person['parents']:
            if parent['id'] in people:
                people[parent['id']].setdefault('children', []).append(person['id'])
            else:
                print('missing person: [%s] %s' % (parent['id'], parent['name']))


def get_children(people, person, generation, stack):
    stack += 1
    if stack > 900:
        print(person)
        return

    for child_id in person.get('children', []):
        child = people[child_id]
        generation.append(child['name'] or child['id'])
        get_children(people, child, generation, stack)


def get_generations(people):
    with open('generations.csv', 'w') as f:
        for p_id, p in people.items():
            if not p['parents']:
                generation = []
                get_children(people, p, generation, 0)
                if len(generation) > 0:
                    f.write('%s;%s\n'%(len(generation), ','.join(generation)))


def make_generation_dot(people):
    with open('generations.dot', 'w') as f:
        f.write('digraph graphname {\n')
        for person_id, person in people.items():
            if person['parents'] or person.get('children'):
                name = person['name'].replace("'", "").replace('"', "") if person['name'] else 'XXX'
                f.write('"%s"[label="%s"];\n'%(person_id, name))

        for person_id, person in people.items():
            for child_id in person.get('children', []):
                f.write('"%s"-> "%s";\n'%(person_id, child_id))

        f.write('}')




def import_into_sqlite(people):
    conn = sqlite3.connect('db.sqlite')

    # Enable foreign keys in sqlite
    curs = conn.execute('PRAGMA foreign_keys = ON')

    # Execute the create script
    sql = open('schema.sql').read()
    conn.executescript(sql)

    cur = conn.cursor()

    for person_id, person in people.items():
        cur.execute('INSERT INTO person (name, gender, freebase_id) VALUES (?, ?, ?)',
                    (person['name'], person['gender'], person_id))
        person['db_id'] = cur.lastrowid

    for person_id, person in people.items():
        for parent in person['parents']:
            if parent['id'] not in people:
                print(parent)
            else:
                cur.execute('INSERT INTO child (parent_id, child_id) VALUES (?, ?)',
                            (people[parent['id']]['db_id'], person['db_id']))

    conn.commit()
    conn.close()

#fetch_all_people()

people = read_files()
import_into_sqlite(people)
print('files read')


#add_children(people)
#print('children added')
#import sys; sys.setrecursionlimit(20000)
#get_generations(people)
#make_generation_dot(people)
