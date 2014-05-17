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


def get_db_connection():
    conn = sqlite3.connect('db.sqlite')

    # Enable foreign keys in sqlite
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


def import_into_sqlite():
    conn = get_db_connection()

    # Execute the create script
    sql = open('schema.sql').read()
    conn.executescript(sql)

    cur = conn.cursor()
    freebase_id_to_db_id = {}
    for filename in sorted(os.listdir('json')):
        print('Inserting persons:', filename)
        with open('json/' + filename) as f:
            content = json.load(f)['result']
            for person in content:
                cur.execute('INSERT INTO person (name, gender, freebase_id) VALUES (?, ?, ?)',
                            (person['name'], person['gender'], person['id']))
                freebase_id_to_db_id[person['id']] = cur.lastrowid

    for filename in sorted(os.listdir('json')):
        print('Inserting relationships:', filename)
        with open('json/' + filename) as f:
            content = json.load(f)['result']
            for person in content:
                for parent in person['parents']:
                    if parent['id'] not in freebase_id_to_db_id:
                        print(parent)
                    else:
                        cur.execute('INSERT INTO child (parent_id, child_id) VALUES (?, ?)',
                                    (freebase_id_to_db_id[parent['id']], freebase_id_to_db_id[person['id']]))

    conn.commit()
    conn.close()


def read_files_into_memory():
    all_people = {}
    for filename in sorted(os.listdir('json')):
        print(filename)
        with open('json/' + filename) as f:
            content = json.load(f)['result']
            for person in content:
                all_people[person['id']] = person

    for person in people.values():
        for parent in person['parents']:
            if parent['id'] in people:
                people[parent['id']].setdefault('children', []).append(person['id'])
            else:
                print('missing person: [%s] %s' % (parent['id'], parent['name']))

    print(len(all_people))
    return all_people


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


#fetch_all_people()
import_into_sqlite()