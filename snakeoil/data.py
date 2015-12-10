import logging
import pickle

__author__ = 'jawaad'



def sfdc_data(conn, db_name, soql_fields, limited=False):
    """
    Converts the confusing SFDC loading mechanism into a generator.

    TODO: We may also want to be migrating the record type id... how to do so?
    """
    query = "SELECT {0} from {1} ORDER BY Id ASC".format(soql_fields, db_name)
    if limited:
        query += " LIMIT 20"
    logging.info(query)

    query_results = conn.query(query)
    while True:
        logging.info("Results returned: {}" .format(len(query_results["records"])))
        yield query_results
        if not query_results.get('nextRecordsUrl'):
            break
        query_results = conn.query_more(query_results[u'nextRecordsUrl'], True)


def persist_data(filename):
    def outer(fn):
        def inner(*args, **kwargs):
            data = fn(*args, **kwargs)
            try:
                with open('{0}.p'.format(filename), 'wb') as fp:
                    pickle.dump(data, fp)
            except IOError:
                pass
            return data
        return inner
    return outer


def load_persist_data(filename, varname="data"):
    def outer(fn):
        def inner(*args, **kwargs):
            try:
                with open('{0}.p'.format(filename), 'r') as fp:
                    kwargs[varname] = pickle.load(fp)
            except IOError as e:
                kwargs[varname] = None
                pass
            except Exception as e:
                kwargs[varname] = None
                logging.exception(e)
            return fn(*args, **kwargs)
        return inner
    return outer


def sfdc_fields(db_name, ignore=None, **kwargs):
    """Figures out what fields you need to import from SFDC.
    WARNING: This prevents any further modification by other decorators above it in the stack.
    The return value is switched to kwargs["soql_fields"]"""

    if ignore is None:
        logging.info("No fields to ignore")
        ignore = []
    else:
        ignore = {x.lower() for x in ignore}

    def outer(fn):
        @persist_data("/tmp/sfdc_fields")
        @load_persist_data("/tmp/sfdc_fields", "field_list_data")
        def inner(*args, **kwargs_inner):
            if kwargs_inner.get("field_list_data") and kwargs_inner["field_list_data"].get(db_name):
                logging.info("Cache hit.")
                kwargs["soql_fields"] = kwargs["field_list_data"].get(db_name)
                fn(*args, **kwargs)
                return kwargs_inner["field_list_data"]

            elif not kwargs_inner.get("field_list_data"):
                    kwargs_inner["field_list_data"] = {}

            kwargs_inner["soql_fields"] = load_sfdc_field(db_name, kwargs["sfdc"], ignore)

            fn(*args, **kwargs_inner)

            kwargs_inner["field_list_data"][db_name] = kwargs_inner["soql_fields"]
            return kwargs_inner["field_list_data"]

        return inner
    return outer


def load_sfdc_field(db_name, conn, ignored_fields):
    """
    Returns custom fields, as well as ID and Name (usually the only fields we find useful)

    TODO: Do we need to sync other fields?

    :param db_name:
    :type db_name:
    :param conn:
    :type conn:
    :param ignored_fields:
    :type ignored_fields:
    :return:
    :rtype:
    """
    q = "Select Id from {0} limit 1".format(db_name)
    results = conn.query(q)

    test_id = results["records"][0]["Id"]

    obj_dict = getattr(conn, db_name).get(test_id)
    obj_field_set = {d[0].lower() for d in obj_dict.items() if "__c" in d[0]}
    obj_field_cnt = len(obj_field_set)
    obj_field_set = obj_field_set - ignored_fields

    if obj_field_cnt == len(obj_field_set):
        logging.info("No fields were ignored")

    if "name" in ignored_fields:
        return ",".join(["Id"] + list(obj_field_set))
    return ",".join(["Id", "Name"] + list(obj_field_set))
