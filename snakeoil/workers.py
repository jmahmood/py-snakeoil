import logging
import multiprocessing
import simple_salesforce

__author__ = 'jawaad'


def worker(conn, itr, worker_fn, update_fn=None, error_fn=None):
    for iw in itr:
        try:
            worker_fn(conn, iw)
        except simple_salesforce.SalesforceMalformedRequest as err:
            # This happens if you are inserting something that is a dupe.
            if "duplicate value found:" in err.content:
                if update_fn:
                    update_fn(conn, iw)
                logging.info(err.content)
            else:
                if error_fn:
                    error_fn(conn, iw)
                logging.exception(err)


def run_processes(conn_list, chunked_lists, fn):
    """
    SFDC processes are generally network limited.  This generates a large number of processes
    (one per connection passed) to perform your action.

    Limits are in effect mind you (50k connections an hour?)

    :param conn_list:
    :type conn_list:
    :param chunked_lists:
    :type chunked_lists:
    :param fn:
    :type fn:
    :return:
    :rtype:
    """
    procs = []
    for i, l in enumerate(chunked_lists):
        conn = conn_list[i]
        p = multiprocessing.Process(
            target=worker,
            args=(conn, l, fn))
        procs.append(p)
        p.start()
    for p in procs:
        p.join()