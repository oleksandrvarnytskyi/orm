import psycopg2
import psycopg2.extras


class DatabaseError(Exception):
    pass


class NotFoundError(Exception):
    pass


class ModifiedError(Exception):
    pass


class Entity(object):
    db = None

    # ORM part 1
    __delete_query    = 'DELETE FROM "{table}" WHERE {table}_id=%s'
    __insert_query    = 'INSERT INTO "{table}" ({columns}) VALUES ({placeholders}) RETURNING "{table}_id"'
    __list_query      = 'SELECT * FROM "{table}"'
    __select_query    = 'SELECT * FROM "{table}" WHERE {table}_id=%s'
    __update_query    = 'UPDATE "{table}" SET {columns} WHERE {table}_id=%s'

    # ORM part 2
    __parent_query    = 'SELECT * FROM "{table}" WHERE {parent}_id=%s'
    __sibling_query   = 'SELECT * FROM "{sibling}" NATURAL JOIN "{join_table}" WHERE {table}_id=%s'
    __update_children = 'UPDATE "{table}" SET {parent}_id=%s WHERE {table}_id IN ({children})'
    __created         = '{}_created'
    __updated         = '{}_updated'

    def __init__(self, id=None):
        if self.__class__.db is None:
            raise DatabaseError()

        self.__cursor   = self.__class__.db.cursor(
            cursor_factory=psycopg2.extras.DictCursor
        )
        self.__fields   = {}
        self.__id       = id
        self.__loaded   = False
        self.__modified = False
        self.__table    = self.__class__.__name__.lower()

    def __getattr__(self, name):
        # check, if instance is modified and throw an exception
        # get corresponding data from database if needed
        # check, if requested property name is in current class
        #    columns, parents, children or siblings and call corresponding
        #    getter with name as an argument
        # throw an exception, if attribute is unrecognized
        if not self.__modified:
            raise ModifiedError

        self.__load()

        if name in self._columns:
            return self._get_column(name)
        if name in self._parents:
            return self._get_parent(name)
        if name in self._children.keys():
            return self._get_children(name)
        if name in self._siblings.keys():
            return self._get_siblings(name)
        else:
            raise AttributeError


    def __setattr__(self, name, value):
        # check, if requested property name is in current class
        #    columns, parents, children or siblings and call corresponding
        #    setter with name and value as arguments or use default implementation
        if name in self._columns:
            self._set_column(name, value)
            self.__modified = True
        elif name in self._parents:
            self._set_parent(name, value)
            self.__modified = True
        else:
            super(Entity, self).__setattr__(name, value)

    def __execute_query(self, query, args):
        # execute an sql statement and handle exceptions together with transactions
        try:
            self.__cursor.execute(query, args)

            self.db.commit()

        except psycopg2.DatabaseError, e:
            self.db.rollback()
            raise e

    @staticmethod
    def get_columns_for_insert(columns):
        return ", ".join('{}'.format(col) for col in columns)

    @staticmethod
    def get_values_for_insert(columns):
        return ", ".join('%({0})s'.format(col) for col in columns)

    def __insert(self):
        # generate an insert query string from fields keys and values and execute it
        # use prepared statements save an insert id
        columns_in_insert = self.get_columns_for_insert(self.__fields.keys())
        values = self.get_values_for_insert(self.__fields.keys())
        data_for_query = dict(zip(self.__fields.keys(),
                                  self.__fields.values()))
        query = self.__insert_query.format(table=self.__table,
                                           columns=columns_in_insert,
                                           placeholders=values)
        self.__execute_query(query, data_for_query)
        self.__id = self.__cursor.fetchone()[0]

    def __load(self):
        # if current instance is not loaded yet - execute select statement and
        # store it's result as an associative array (fields), where column
        # names used as keys
        if self.__loaded:
            return

        query = self.__select_query.format(table=self.__table)
        self.__execute_query(query, [self.__id])
        rows = self.__cursor.fetchall()

        for row in rows:
            self.__fields = dict(row)

        self.__loaded = True
        self.__modified = True

    @staticmethod
    def get_columns_for_update(fields):
        return ', '.join('{} = %s'.format(key) for key in fields)

    def __update(self):
        # generate an update query string from fields keys and values and execute it
        # use prepared statements
        columns_in_set = self.get_columns_for_update(self.__fields)
        query = self.__update_query.format(table=self.__table,
                                           columns=columns_in_set)
        values = self.__fields.values()
        values.append(self.__id)
        self.__execute_query(query, values)

    def _get_children(self, name):
        # ORM part 2
        # return an array of child entity instances
        # each child instance must have an id and be filled with data
        import models

        child_instances_list = []
        child_cls_name = self._children[name]
        child_entity = getattr(models, child_cls_name)
        child = child_entity()

        query = child.__parent_query.format(table=child.__table,
                                            parent=self.__table)
        child.__execute_query(query, (self.__id,))
        rows = child.__cursor.fetchall()

        for row in rows:
            inst_id = '{}_id'.format(child.__table)
            inst = child_entity(row[inst_id])
            inst.__fields = dict(row)
            inst.__modified = True
            child_instances_list.append(inst)
        return child_instances_list

    def _get_column(self, name):
        # return value from fields array by <table>_<name> as a key
        key = '{}_{}'.format(self.__table, name)
        return self.__fields[key]

    def _get_parent(self, name):
        # ORM part 2
        # get parent id from fields with <name>_id as a key
        # return an instance of parent entity class with an appropriate id
        import models

        key = '{}_id'.format(name)
        parent_id = self.__fields[key]
        parent_cls_name = name.capitalize()
        parent_instance = getattr(models, parent_cls_name)(parent_id)
        parent_instance.__modified = True

        return parent_instance

    def _get_siblings(self, name):
        # ORM part 2
        # get parent id from fields with <name>_id as a key
        # return an array of sibling entity instances
        # each sibling instance must have an id and be filled with data
        import models

        sibling_instances_list = []
        sibling_cls_name = self._siblings[name]
        sibling_entity = getattr(models, sibling_cls_name)
        sibling = sibling_entity()
        name_lst = [sibling.__table, self.__table]
        name_lst.sort()
        connecting_table = '{}__{}'.format(name_lst[0], name_lst[1])

        query = sibling.__sibling_query.format(sibling=sibling.__table,
                                               join_table=connecting_table,
                                               table=self.__table)
        sibling.__execute_query(query, (self.__id,))
        rows = sibling.__cursor.fetchall()

        for row in rows:
            inst_id = '{}_id'.format(sibling.__table)
            inst = sibling_entity(row[inst_id])
            inst.__fields = dict(row)
            inst.__modified = True
            sibling_instances_list.append(inst)

        return sibling_instances_list

    def _set_column(self, name, value):
        # put new value into fields array with <table>_<name> as a key
        field_name = '{}_{}'.format(self.__table, name)
        self.__fields[field_name] = value

    def _set_parent(self, name, value):
        # ORM part 2
        # put new value into fields array with <name>_id as a key
        # value can be a number or an instance of Entity subclass
        key = '{}_id'.format(name)

        if isinstance(value, Entity):
            self.__fields[key] = value.__id
        if isinstance(value, int):
            self.__fields[key] = int(value)

    @classmethod
    def all(cls):
        # get ALL rows with ALL columns from corrensponding table
        # for each row create an instance of appropriate class
        # each instance must be filled with column data, a correct id and
        # MUST NOT query a database for own fields any more
        # return an array of instances
        instances_list = []
        instance = cls()

        query = cls.__list_query.format(table=instance.__table)
        instance.__execute_query(query, None)
        rows = instance.__cursor.fetchall()

        for row in rows:
            inst_id = '{}_id'.format(instance.__table)
            inst = cls(row[inst_id])
            inst.__fields = dict(row)
            inst.__modified = True
            instances_list.append(inst)

        return instances_list

    def delete(self):
        # execute delete query with appropriate id
        query = self.__delete_query.format(table=self.__table)
        self.__execute_query(query, [self.__id])

    @property
    def id(self):
        # try to guess yourself
        return self.__id

    @property
    def created(self):
        # try to guess yourself
        column_name = self.__created.format(self.__table)
        self.__load()
        return self.__fields[column_name]

    @property
    def updated(self):
        # try to guess yourself
        column_name = self.__updated.format(self.__table)
        self.__load()
        return self.__fields[column_name]

    def save(self):
        # execute either insert or update query, depending on instance id
        if self.__id:
            self.__update()
        else:
            self.__insert()
        self.__modified = True
