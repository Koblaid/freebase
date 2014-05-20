import ujson as json
import os
import sqlite3
from pprint import pprint
from collections import Counter

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

    unimportable_parents = []
    for filename in sorted(os.listdir('json')):
        print('Inserting relationships:', filename)
        with open('json/' + filename) as f:
            content = json.load(f)['result']
            for person in content:
                for parent in person['parents']:
                    if parent['id'] not in freebase_id_to_db_id:
                        print(parent)
                        unimportable_parents.append(parent)
                    else:
                        cur.execute('INSERT INTO parent_child (parent_id, child_id) VALUES (?, ?)',
                                    (freebase_id_to_db_id[parent['id']], freebase_id_to_db_id[person['id']]))

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

            contained_persons = {}

            # start with depth 2 to account for the ancestor and the latest child
            persons_to_process = [(ancestor, set([ancestor]), 2)]
            while persons_to_process:
                generation_counter += 1

                person, parents, current_generation_depth = persons_to_process.pop()
                parents.add(person)
                for relationship in person.children:
                    max_generation_depth = max(max_generation_depth, current_generation_depth)
                    child = relationship.child

                    # Check if a person is the parent of one of its ancestors
                    for grand_child in child.get_children():
                        if grand_child in parents:
                            rel = child.remove_child(grand_child)
                            invalid_relationships.append(rel)

                    contained_persons[person.db_id] = person.name
                    contained_persons[child.db_id] = child.name
                    if relationship not in generation:
                        generation.add(relationship)
                        persons_to_process.append((child, parents.copy(), current_generation_depth+1))

            if len(generation) > 0:
                generations[ancestor.db_id] = (max_generation_depth, generation)

    return generations, invalid_relationships


def write_families_into_db(generations):
    counter = 0
    conn = get_db_connection()
    cur = conn.cursor()
    for ancestor_id, (max_depth, relationships) in generations.items():
        counter += 1
        print('%s / %s (%s%%)' % (counter, len(generations), counter*100/len(generations)))

        stmt = 'insert into family (ancestor_id, max_generation_depth) values (?, ?)'
        cur.execute(stmt, (ancestor_id, max_depth))
        family_id = cur.lastrowid

        for relationship in relationships:
            stmt = 'insert into family_member (family_id, parent_child_id) values (?, ?)'
            cur.execute(stmt, (family_id, relationship.db_id))

    conn.commit()


def start_image_server():
    import flask
    app = flask.Flask(__name__)

    @app.route('/')
    def index():
        distribution_of_family_size = Counter()
        cur.execute('select count(f.id) as cnt from family f join family_member fm on f.id = fm.family_id group by f.id')
        rows = cur.fetchall()

        max_family_size = 0
        for (count, ) in rows:
            max_family_size = max(max_family_size, count)
            if count <= 10:
                distribution_of_family_size[count] += 1
            elif count <= 15:
                distribution_of_family_size['10-15'] += 1
            elif count <= 20:
                distribution_of_family_size['15-20'] += 1
            elif count <= 50:
                distribution_of_family_size['20-50'] += 1
            elif count <= 100:
                distribution_of_family_size['50-100'] += 1
            elif count <= 200:
                distribution_of_family_size['100-200'] += 1
            elif count <= 500:
                distribution_of_family_size['200-500'] += 1
            elif count <= 1000:
                distribution_of_family_size['500-1000'] += 1
            elif count <= 2000:
                distribution_of_family_size['1000-2000'] += 1
            elif count <= 5000:
                distribution_of_family_size['2000-5000'] += 1
            elif count <= 10000:
                distribution_of_family_size['5000-10000'] += 1
            elif count <= 15000:
                distribution_of_family_size['10000-15000'] += 1
            else:
                distribution_of_family_size['>15000'] += 1

        cur.execute('select max_generation_depth, count(*) from family group by max_generation_depth')
        distribution_of_generation_depth = {}
        for max_generation_depth, count in cur:
            distribution_of_generation_depth[max_generation_depth] = count

        stats = dict(
            person_count=person_count,
            gender_distribution=gender_distribution,
            distribution_of_family_size=distribution_of_family_size,
            max_family_size=max_family_size,
            distribution_of_generation_depth=distribution_of_generation_depth,
        )

        cur.execute('''
        select f.id, f.max_generation_depth, p.name, count(f.id) as person_count
        from family f
        join person p on f.ancestor_id = p.id
        join family_member fm on f.id = fm.family_id
        group by f.id
        having person_count > 2
        order by f.max_generation_depth desc, person_count desc''')

        return flask.render_template('index.html', rows=cur, statistics=stats)

    @app.route('/familytree/<family_id>')
    def familytree(family_id):
        return flask.render_template('familytree.html')

    @app.route('/stats')
    def stats():
        cur = get_db_connection().cursor()
        cur.execute('select count(*) from person')
        person_count = cur.fetchone()[0]
        return flask.render_template('stats.html', person_count=person_count)

    @app.route('/json/stats/gender')
    def json_stats_gender():
        cur = get_db_connection().cursor()
        cur.execute('select gender, count(*) from person group by gender')
        rows = cur.fetchall()
        total = sum((row[1] for row in rows))
        gender_distribution = {}
        for gender, count in rows:
            gender_distribution[gender or 'None'] = count# round(count*100/total, 2)
        return flask.jsonify(data=gender_distribution)

    @app.route('/json/familytree/<family_id>')
    def json_familytree(family_id):
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('''
        select
            p_parent.id, p_parent.name, p_parent.freebase_id, p_child.id, p_child.name, p_child.freebase_id
        from family f
        join family_member fm on f.id = fm.family_id
        join parent_child pc on pc.id = fm.parent_child_id
        join person p_parent on p_parent.id = pc.parent_id
        join person p_child on p_child.id = pc.child_id
        where f.id = ?;''', (family_id,))
        persons = {}
        edges = []
        for parent_id, parent_name, parent_freebase_id, child_id, child_name, child_freebase_id in cur:
            persons[parent_id] = (parent_name, parent_freebase_id)
            persons[child_id] = (child_name, child_freebase_id)
            edges.append(dict(u=parent_id, v=child_id))

        nodes = []
        for person_id, (name, freebase_id) in persons.items():
            label = '<div style="padding: 10px;"><a href="https://www.freebase.com%s">%s</a></div>' % (freebase_id, name.replace(' ', '<br>'))
            nodes.append(dict(id=person_id, value=dict(label=label)))
        return flask.jsonify(dict(nodes=nodes, edges=edges))


    @app.route('/mega')
    def mega():
        return flask.render_template('mega.html')

    @app.route('/megajson')
    def megajson():
        return flask.jsonify(json.load(open('testgen-with-names.json')))


    app.run(debug=True, host='0.0.0.0')


import sys
if len(sys.argv) > 1 and sys.argv[1] == 'server':
    start_image_server()
