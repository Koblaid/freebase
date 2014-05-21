import sqlite3

import flask


app = flask.Flask(__name__)


DATABASE = '../db.sqlite'


def get_db_connection():
    conn = getattr(flask.g, '_database', None)
    if conn is None:
        conn = flask.g._database = sqlite3.connect(DATABASE)
        # Enable foreign keys in sqlite
        conn.execute('PRAGMA foreign_keys = ON')
    return conn


@app.teardown_appcontext
def close_connection(exception):
    conn = getattr(flask.g, '_database', None)
    if conn is not None:
        conn.close()


@app.route('/')
def index():
    cur = get_db_connection().cursor()
    cur.execute('''
    select f.id, f.max_generation_depth, p.name, f.person_count
    from family f
    join person p on f.ancestor_id = p.id
    where f.max_generation_depth > 3
    order by f.max_generation_depth desc, f.person_count desc''')
    return flask.render_template('index.html', rows=cur)

@app.route('/familytree/<family_id>')
def familytree(family_id):
    return flask.render_template('familytree.html')


@app.route('/stats')
def stats():
    cur = get_db_connection().cursor()
    cur.execute('select count(*) from person')
    person_count = cur.fetchone()[0]

    cur.execute('select max_generation_depth, count(*) from family group by max_generation_depth')
    dist_dict = {}
    for max_generation_depth, count in cur:
        dist_dict[max_generation_depth] = count
    distribution_of_generation = []
    for i in range(max(dist_dict), 1, -1):
        distribution_of_generation.append((i, dist_dict.get(i, 0)))

    distribution_of_family_size = Counter()
    cur.execute('select person_count from family')
    # Attention: The following code is as bad as it gets ;-)
    max_family_size = 0
    for (count, ) in cur:
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
        else:
            raise Exception()
    order = [i for i in range(2, 10)]
    order += ['10-15', '15-20', '20-50', '50-100', '100-200', '200-500',
             '500-1000', '1000-2000', '2000-5000', '5000-10000']
    distribution_of_family_size_list = []
    for o in order:
        distribution_of_family_size_list.append([o, distribution_of_family_size[o]])

    return flask.render_template('stats.html',
                                 person_count=person_count,
                                 distribution_of_generation=distribution_of_generation,
                                 distribution_of_family_size=distribution_of_family_size_list,
                                 max_family_size=max_family_size)


@app.route('/json/stats/gender')
def json_stats_gender():
    cur = get_db_connection().cursor()
    cur.execute('select gender, count(*) from person group by gender')
    rows = cur.fetchall()
    total = sum((row[1] for row in rows))
    gender_distribution = {}
    for gender, count in rows:
        gender_distribution[gender or 'None'] = count
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



import sys
if sys.argv == ['app.py', 'debug']:
    app.run(debug=True)
