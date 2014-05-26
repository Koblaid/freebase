import ujson as json
import os
import psycopg2
from pprint import pprint

import requests


# http://eatthedots.blogspot.de/2008/08/faking-read-support-for-psycopgs.html
class IteratorToFile:
    def __init__(self, iterator, template):
        self._iterator = iterator
        self._template = template

    def readline(self, size=None):
        try:
            try:
                line = next(self._iterator)
            except StopIteration:
                #print('return stopiteration')
                return ''
            else:
                return self._template % line
        except Exception as e:
            print(line)
            print(e)

    read = readline


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
    conn = psycopg2.connect(host='localhost', user='postgres', database='persons')
    conn.cursor().execute('SET search_path TO persons, public')
    return conn


def drop_schema():
    if input('Really drop schema [y|N]?') == 'y':
        conn = get_db_connection()
        conn.cursor().execute('DROP SCHEMA persons CASCADE')
        conn.commit()
        conn.close()
        print('Schema dropped')
    else:
        print('Cancelled')


def create_schema():
    conn = get_db_connection()
    conn.cursor().execute(open("schema.sql", "r").read())
    conn.commit()
    conn.close()


def _format_for_copy(value):
    if value is None:
        return '\\N'
    else:
        return value.replace('\t', r'\\t').replace('\n', r'\\n')


def _iter_persons(freebase_id_to_db_id):
    person_id = 1000
    for filename in sorted(os.listdir('json')):
        print('Inserting persons:', filename)
        content = json.load(open('json/' + filename))['result']

        for person in content:
            name = _format_for_copy(person['name'])
            gender = _format_for_copy(person['gender'])
            freebase_id = _format_for_copy(person['id'])
            yield (person_id, name, gender, freebase_id)
            freebase_id_to_db_id[person['id']] = person_id
            person_id += 1


def _iter_relationship(freebase_id_to_db_id, unimportable_parents):
    for filename in sorted(os.listdir('json')):
        print('Inserting relationships:', filename)
        content = json.load(open('json/' + filename))['result']
        for person in content:
            for parent in person['parents']:
                if parent['id'] not in freebase_id_to_db_id:
                    print(parent)
                    unimportable_parents.append(parent)
                else:
                    yield (freebase_id_to_db_id[parent['id']], freebase_id_to_db_id[person['id']])


def import_into_db():
    conn = get_db_connection()

    cur = conn.cursor()
    freebase_id_to_db_id = {}

    streamer = IteratorToFile(_iter_persons(freebase_id_to_db_id), '%i\t"%s"\t"%s"\t"%s"\n')
    cur.copy_from(streamer, 'person', columns=['id', 'name', 'gender', 'freebase_id'])

    unimportable_parents = []
    streamer = IteratorToFile(_iter_relationship(freebase_id_to_db_id, unimportable_parents), '%i\t%i\n')
    cur.copy_from(streamer, 'parent_child', columns=['parent_id', 'child_id'])

    print('unimportable parents:')
    pprint(unimportable_parents)
    conn.commit()
    conn.close()


class Person:
    def __init__(self, db_id, name):
        self.db_id = db_id
        self.name = name
        self.children = []
        self.parents = []

    def get_parents(self):
        return (rel.parent for rel in self.parents)

    def get_children(self):
        return (rel.child for rel in self.children)

    def remove_parent(self, parent):
        for rel in self.parents:
            if rel.parent is parent:
                rel.remove()
        return rel

    def remove_child(self, child):
        for rel in self.children:
            if rel.child is child:
                rel.remove()
        return rel


class Parent_Child_Relationship:
    def __init__(self, db_id, parent, child):
        self.db_id = db_id
        self.parent = parent
        self.child = child

    def remove(self):
        self.parent.children.remove(self)
        self.child.parents.remove(self)


def read_db_into_memory():
    persons = {}
    conn = get_db_connection()
    cur = conn.cursor()
    stmt = 'select id, name from person'
    cur.execute(stmt)
    for db_id, name in cur:
        person = Person(db_id, name)
        persons[db_id] = person

    cur.execute('select id, parent_id, child_id from parent_child')
    for db_id, parent_id, child_id in cur:
        parent = persons[parent_id]
        child = persons[child_id]
        relationship = Parent_Child_Relationship(db_id, parent, child)
        parent.children.append(relationship)
        child.parents.append(relationship)
    return persons


def generate_statistics(persons):
    without_parents = 0
    without_children = 0
    without_relatives = 0
    with_children = 0
    with_parents = 0
    max_children = 0
    max_parents = 0
    for p in persons.values():
        if not p.parents and not p.children:
            without_relatives += 1
            continue

        if not p.parents:
            without_parents += 1
        else:
            with_parents += 1
            max_parents = max(len(p.parents), max_parents)

        if not p.children:
            without_children += 1
        else:
            with_children += 1
            max_children = max(len(p.children), max_children)

    return dict(
        total_number_of_persons=len(persons),
        number_of_persons_without_relatives=without_relatives,
        number_of_persons_without_children=without_children,
        number_of_persons_without_parents=without_parents,
        max_number_children=max_children,
        max_number_parents=max_parents,
        number_of_persons_with_parents=with_parents,
        number_of_persons_with_children=with_children,
    )


def extract_generations(persons):
    invalid_relationships = []
    generations = {}
    for ancestor in persons.values():
        if not ancestor.parents:
            generation_counter = 0
            max_generation_depth = 1

            # Use a set to prevent that the same relationship is added twice
            generation = set()

            contained_persons = set([ancestor])

            # start with depth 2 to account for the ancestor and the latest child
            persons_to_process = [(ancestor, set([ancestor]), 2)]
            while persons_to_process:
                generation_counter += 1

                person, parents, current_generation_depth = persons_to_process.pop()
                parents.add(person)
                contained_persons.add(person)
                for relationship in person.children:
                    max_generation_depth = max(max_generation_depth, current_generation_depth)
                    child = relationship.child

                    # Check if a person is the parent of one of its ancestors
                    for grand_child in child.get_children():
                        if grand_child in parents:
                            rel = child.remove_child(grand_child)
                            invalid_relationships.append(rel)

                    contained_persons.add(child)
                    if relationship not in generation:
                        generation.add(relationship)
                        persons_to_process.append((child, parents.copy(), current_generation_depth+1))

            if len(generation) > 0:
                person_count = len(contained_persons)
                generations[ancestor.db_id] = (max_generation_depth, person_count, generation)

    return generations, invalid_relationships


def write_families_into_db(generations):
    counter = 0
    conn = get_db_connection()
    cur = conn.cursor()
    for ancestor_id, (max_depth, person_count, relationships) in generations.items():
        counter += 1
        print('%s / %s (%s%%)' % (counter, len(generations), round(counter*100/len(generations), 2)))

        stmt = 'insert into family (ancestor_id, max_generation_depth, person_count) values (%s, %s, %s) returning id'
        cur.execute(stmt, (ancestor_id, max_depth, person_count))
        family_id = cur.fetchone()[0]

        iter_parent_child = ((family_id, relationship.db_id) for relationship in relationships)

        streamer = IteratorToFile(iter_parent_child, '%i\t%i\n')
        cur.copy_from(streamer, 'family_member', columns=['family_id', 'parent_child_id'])

    conn.commit()
    conn.close()
